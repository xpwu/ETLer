package task

import (
	"context"
	"github.com/xpwu/ETLer/etl/db"
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

func (s *Sender) cancelSafely() (canceled bool) {
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
		case ErrSendFailed:
			s.done(SendFailed)
		case db.ErrNotFoundSentPoint:
			s.done(NeedForceSync)
		case context.Canceled:
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

// err context.Canceled, ErrSendFailed or unknown
func (s *Sender) sync() error {
	iter := db.SyncTask().All(s.ctx)
	defer iter.Release(s.ctx)

	for iter.Next(s.ctx) {
		task := iter.Current()
		coll := s.client.Database(task.DB).Collection(task.Collection)
		docId := task.UntilDocId

		for {
			cursor, err := coll.Find(s.ctx, bson.D{{"_id", bson.D{{"$gt", docId}}}},
				options.Find().SetLimit(int64(s.batch)).SetSort(bson.D{{"_id", 1}}))
			if err == context.Canceled {
				s.logger.Debug(err)
				return err
			}
			if err != nil {
				s.logger.Error(err)
				return err
			}

			all := make([]bson.Raw, 0, s.batch)
			for cursor.Next(s.ctx) {
				docId = cursor.Current.Lookup("_id")
				all = append(all, cursor.Current)
			}
			if cursor.Err() == context.Canceled {
				return cursor.Err()
			}

			if len(all) != 0 {
				err = netLayer.Send(s.ctx, Sync, task.DB, task.Collection, all)
			}
			if err != nil {
				s.logger.Warning("change stream sender failed: " + err.Error())
				return err
			}

			err = cursor.Err()

			// finished
			if len(all) < s.batch && err == nil {
				db.SyncTask().Del(s.ctx, task.Id())
				break
			}

			if err != nil {
				s.logger.Error("cursor error.", err)
				return err
			}

			// update
			task.UntilDocId = docId
			db.SyncTask().InsertOrUpdate(s.ctx, task)
		}
	}

	return nil
}

// err context.Canceled, ErrSendFailed, ErrNotFoundSentPoint or unknown
func (s *Sender) sendChangeStream() error {
	iter := db.ChangeStream().AllNotSent(s.ctx)
	defer iter.Release(s.ctx)

	for iter.Next(s.ctx, s.batch) {
		err := netLayer.Send(s.ctx, ChangeStream, "", "", iter.Values())

		if err != nil {
			s.logger.Warning("change stream send failed: " + err.Error())
			return err
		}

		db.ChangeStream().MarkSentUpTo(s.ctx, iter.LastStreamId())
	}

	return iter.Err()
}
