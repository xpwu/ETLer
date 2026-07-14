package task

import (
	"context"
	"errors"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/go-db-mongo/mongodb/mongocache"
	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Runner struct {
	rawCtx   context.Context
	ctx      context.Context
	client   *mongocache.Client
	cancel   chan context.CancelFunc
	logger   *log.Logger
	batch    int
	doneChan chan RunnerCode
	addOne   chan struct{}
}

func NewRunner(ctx context.Context, client *mongocache.Client, batch int) *Runner {
	r := &Runner{
		client:   client,
		cancel:   make(chan context.CancelFunc, 1),
		batch:    batch,
		doneChan: make(chan RunnerCode, 1),
	}

	r.rawCtx, r.logger = log.WithCtx(ctx)
	r.logger.PushPrefix("runner")

	r.ctx = r.rawCtx

	return r
}

type RunnerCode int

const (
	Ok RunnerCode = iota
	ByStopped
	NeedForceSync
	SendFailed
	UnknownErr
)

func (r *Runner) Done() <-chan RunnerCode {
	return r.doneChan
}

var (
	senderErr             = errors.New("sender error")
	stoppedErr            = errors.New("stopped")
	lastStreamNotFoundErr = errors.New("CAN NOT find last stream id")
)

func (r *Runner) cancelSafely() (hasCancel bool) {
	select {
	case cancel := <-r.cancel:
		cancel()
		return true
	default:
		return false
	}
}

func (r *Runner) done(code RunnerCode) {
	select {
	case r.doneChan <- code:
	default:
	}
}

// Start 时序很重要，sync 与 send change stream 都是串行处理，每一批也都是串行处理，即使是停止，也必须等待停止后，才能新启动
// 一定是先 sync 再 send change stream
// Start 与 Stop 不是并发安全的
func (r *Runner) Start() (isBusy bool) {
	ctx, cancel := context.WithCancel(r.rawCtx)
	select {
	case r.cancel <- cancel:
	default:
		// running
		isBusy = true
		return
	}
	r.ctx = ctx

	go func() {
		err := r.sync()
		if err == nil {
			err = r.sendChangeStream()
		}

		// clear cancel chan
		r.cancelSafely()

		switch err {
		case senderErr:
			r.done(SendFailed)
		case lastStreamNotFoundErr:
			r.done(NeedForceSync)
		case stoppedErr:
			r.done(ByStopped)
		case nil:
			r.done(Ok)
		default:
			r.done(UnknownErr)
		}
	}()

	return false
}

// Stop 与 Start 不是并发安全的
func (r *Runner) Stop() {
	if r.cancelSafely() {
		r.logger.Info("will stop")
		// wait stopped
		<-r.Done()
	}
}

func serialize(value bson.RawValue) []byte {
	ret := make([]byte, 1, 1+len(value.Value))
	ret[0] = byte(value.Type)
	return append(ret, value.Value...)
}

func deserialize(bytes []byte) bson.RawValue {
	return bson.RawValue{
		Type:  bsontype.Type(bytes[0]),
		Value: bytes[1:],
	}
}

func (r *Runner) sync() error {

	iter := db.SyncTask().All(r.ctx)
	defer iter.Release()

	task, ok := iter.First(r.ctx)
	for ok {
		coll := r.client.Database(task.DB).Collection(task.Collection)
		docId := deserialize(task.StartDocId)

		for {
			cursor, err := coll.Find(r.ctx, bson.D{{"_id", bson.D{{"$gt", docId}}}},
				options.Find().SetLimit(int64(r.batch)).SetSort(bson.D{{"_id", 1}}))
			if err == context.Canceled {
				r.logger.Debug(err)
				return stoppedErr
			}

			if err != nil {
				r.logger.Error(err)
				return err
			}

			all := make([]bson.Raw, 0, r.batch)
			i := 0
			for cursor.Next(r.ctx) {
				i += 1
				docId = cursor.Current.Lookup("_id")
				all = append(all, cursor.Current)
			}

			err = Sender.Do(r.ctx, Sync, task.DB, task.Collection, all)
			if err == canceledErr {
				r.logger.Warning("change stream sender canceled")
				return stoppedErr
			}
			if err != nil {
				r.logger.Warning("change stream sender failed: " + err.Error())
				return senderErr
			}

			err = cursor.Err()

			// over
			if i < r.batch && err == nil {
				db.SyncTask().Del(r.ctx, task.Id())
				break
			}

			if err == context.Canceled {
				return stoppedErr
			}
			if err != nil {
				r.logger.Error("cursor error.", err)
				return err
			}

			// update
			task.StartDocId = serialize(docId)
			db.SyncTask().InsertOrUpdate(r.ctx, task)
		}

		task, ok = iter.Next(r.ctx)
	}

	return nil
}

func (r *Runner) sendChangeStream() error {

	sendId, ok := db.Cache().SentStreamId(r.ctx)
	values := make([]db.StreamValue, 0, r.batch)

	var iter db.StreamIterator
	if ok {
		iter = db.Stream().StartWith(r.ctx, sendId)
	} else {
		iter = db.Stream().All(r.ctx)
	}
	defer iter.Release()

	var lastId db.StreamId

	if ok {
		firstId, _, ok := iter.First(r.ctx)

		// 之前发送过的stream 已经不能在stream找到，说明中间有断层，必须force sync
		if !ok || string(firstId) != string(sendId) {
			return lastStreamNotFoundErr
		}

		values, lastId, ok = iter.Next(r.ctx, 1)
		if !ok {
			return nil
		}
	} else {
		var value db.StreamValue
		lastId, value, ok = iter.First(r.ctx)

		if !ok {
			r.logger.Info("sendChangeStream: has not stream to send")
			return nil
		}
		values = append(values, value)
	}

	for ok {
		err := Sender.Do(r.ctx, ChangeStream, "", "", values)
		if err == canceledErr {
			r.logger.Warning("change stream sender canceled")
			return stoppedErr
		}
		if err != nil {
			r.logger.Warning("change stream sender failed: " + err.Error())
			return senderErr
		}

		db.Cache().SaveSentStreamId(r.ctx, lastId)
		values, lastId, ok = iter.Next(r.ctx, r.batch)
	}

	return nil
}
