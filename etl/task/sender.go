package task

import (
	"context"
	"errors"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var netLayer NetLayer = &HttpNetLayer{}

func SetNetLayer(layer NetLayer) {
	netLayer = layer
}

// Sender 只发送当前已有的同步任务或获取到的已缓存的 stream，发送完即自动停止，不等待也不判断是否未来还需要有发送的数据
// Sender 不是并发安全的
type Sender struct {
	rawCtx   context.Context
	ctx      context.Context
	client   *mongo.Client
	cancel   chan context.CancelFunc
	logger   *log.Logger
	batch    int
	doneChan chan SenderCode
	addOne   chan struct{}
}

func NewSender(ctx context.Context, client *mongo.Client, batch int) *Sender {
	s := &Sender{
		client:   client,
		cancel:   make(chan context.CancelFunc, 1),
		batch:    batch,
		doneChan: make(chan SenderCode, 1),
	}

	s.rawCtx, s.logger = log.WithCtx(ctx)
	s.logger.PushPrefix("runner")

	s.ctx = s.rawCtx

	return s
}

type SenderCode int

const (
	Ok SenderCode = iota
	ByStopped
	NeedForceSync
	SendFailed
	UnknownErr
)

func (s *Sender) Done() <-chan SenderCode {
	return s.doneChan
}

var (
	senderErr             = errors.New("sender error")
	stoppedErr            = errors.New("stopped")
	lastStreamNotFoundErr = errors.New("CAN NOT find last stream id")
)

func (s *Sender) cancelSafely() (cancelled bool) {
	select {
	case cancel := <-s.cancel:
		cancel()
		return true
	default:
		return false
	}
}

func (s *Sender) done(code SenderCode) {
	select {
	case s.doneChan <- code:
	default:
	}
}

// Start 时序很重要，sync 与 send change stream 都是串行处理，每一批也都是串行处理，即使是停止，也必须等待停止后，才能新启动
// 一定是先 sync 再 send change stream
// Start 与 Stop 都不是并发安全的，但是非并发地重复调用 Start 是安全的
func (s *Sender) Start() (isBusy bool) {
	ctx, cancel := context.WithCancel(s.rawCtx)
	select {
	case s.cancel <- cancel:
	default:
		// running
		isBusy = true
		return
	}
	s.ctx = ctx

	go func() {
		err := s.sync()
		if err == nil {
			err = s.sendChangeStream()
		}

		// clear cancel chan
		s.cancelSafely()

		switch err {
		case senderErr:
			s.done(SendFailed)
		case lastStreamNotFoundErr:
			s.done(NeedForceSync)
		case stoppedErr:
			s.done(ByStopped)
		case nil:
			s.done(Ok)
		default:
			s.done(UnknownErr)
		}
	}()

	return false
}

// Stop 与 Start 都不是并发安全的，但是非并发地重复调用 Stop 是安全的
func (s *Sender) Stop() {
	if s.cancelSafely() {
		s.logger.Info("will stop")
		// wait stopped
		<-s.Done()
	}
}

func serialize(value bson.RawValue) []byte {
	ret := make([]byte, 1, 1+len(value.Value))
	ret[0] = byte(value.Type)
	return append(ret, value.Value...)
}

func deserialize(bytes []byte) bson.RawValue {
	return bson.RawValue{
		Type:  bson.Type(bytes[0]),
		Value: bytes[1:],
	}
}

func (s *Sender) sync() error {

	iter := db.SyncTask().All(s.ctx)
	defer iter.Release()

	task, ok := iter.First(s.ctx)
	for ok {
		coll := s.client.Database(task.DB).Collection(task.Collection)
		docId := deserialize(task.StartDocId)

		for {
			cursor, err := coll.Find(s.ctx, bson.D{{"_id", bson.D{{"$gt", docId}}}},
				options.Find().SetLimit(int64(s.batch)).SetSort(bson.D{{"_id", 1}}))
			if err == context.Canceled {
				s.logger.Debug(err)
				return stoppedErr
			}

			if err != nil {
				s.logger.Error(err)
				return err
			}

			all := make([]bson.Raw, 0, s.batch)
			i := 0
			for cursor.Next(s.ctx) {
				i += 1
				docId = cursor.Current.Lookup("_id")
				all = append(all, cursor.Current)
			}

			err = netLayer.Do(s.ctx, Sync, task.DB, task.Collection, all)
			if err == canceledErr {
				s.logger.Warning("change stream sender canceled")
				return stoppedErr
			}
			if err != nil {
				s.logger.Warning("change stream sender failed: " + err.Error())
				return senderErr
			}

			err = cursor.Err()

			// over
			if i < s.batch && err == nil {
				db.SyncTask().Del(s.ctx, task.Id())
				break
			}

			if err == context.Canceled {
				return stoppedErr
			}
			if err != nil {
				s.logger.Error("cursor error.", err)
				return err
			}

			// update
			task.StartDocId = serialize(docId)
			db.SyncTask().InsertOrUpdate(s.ctx, task)
		}

		task, ok = iter.Next(s.ctx)
	}

	return nil
}

func (s *Sender) sendChangeStream() error {

	sendId, ok := db.Cache().SentStreamId(s.ctx)
	values := make([]x.StreamValue, 0, s.batch)

	var iter db.ChangeStreamIterator
	if ok {
		iter = db.ChangeStream().StartWith(s.ctx, sendId)
	} else {
		iter = db.ChangeStream().All(s.ctx)
	}
	defer iter.Release()

	var lastId x.StreamId

	if ok {
		firstId, _, ok := iter.First(s.ctx)

		// 之前发送过的stream 已经不能在stream找到，说明中间有断层，必须force sync
		if !ok || string(firstId) != string(sendId) {
			return lastStreamNotFoundErr
		}

		values, lastId, ok = iter.Next(s.ctx, 1)
		if !ok {
			return nil
		}
	} else {
		var value x.StreamValue
		lastId, value, ok = iter.First(s.ctx)

		if !ok {
			s.logger.Info("sendChangeStream: has not stream to send")
			return nil
		}
		values = append(values, value)
	}

	for ok {
		err := Sender.Do(s.ctx, ChangeStream, "", "", values)
		if err == canceledErr {
			s.logger.Warning("change stream sender canceled")
			return stoppedErr
		}
		if err != nil {
			s.logger.Warning("change stream sender failed: " + err.Error())
			return senderErr
		}

		db.Cache().SaveSentStreamId(s.ctx, lastId)
		values, lastId, ok = iter.Next(s.ctx, s.batch)
	}

	return nil
}
