package task

import (
	"context"
	"fmt"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"sync"
)

// Sender 只发送当前已有的同步任务或获取到的已缓存的 stream，发送完即自动停止，不等待也不判断是否未来还需要有发送的数据
// Start Stop 是并发安全的，可以多次重复调用
type Sender struct {
	rawCtx   context.Context
	ctx      context.Context
	client   *mongo.Client
	netLayer NetLayer

	//logger   *log.Logger
	batch    int
	doneChan chan SenderCode

	// isBusy: cancelF != nil || stoppedSig is not closed
	mutex      sync.Mutex
	cancelF    context.CancelFunc
	stoppedSig chan struct{}
}

func NewSender(ctx context.Context, client *mongo.Client, batch int) *Sender {
	return NewSender2(ctx, client, batch, &HttpNetLayer{})
}

func NewSender2(ctx context.Context, client *mongo.Client, batch int, netLayer NetLayer) *Sender {
	s := &Sender{
		client:     client,
		batch:      batch,
		doneChan:   make(chan SenderCode, 1),
		stoppedSig: make(chan struct{}),
		netLayer:   netLayer,
	}

	// 默认是 stopped
	close(s.stoppedSig)

	ctx, logger := log.WithCtx(ctx)
	s.rawCtx = ctx

	logger.PushPrefix("Sender")
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

func SenderCodeStr(code SenderCode) string {
	switch code {
	case Ok:
		return "OK"
	case ByStopped:
		return "ByStopped"
	case NeedForceSync:
		return "NeedForceSync"
	case SendFailed:
		return "SendFailed"
	case UnknownErr:
		return "UnknownErr"
	default:
		return "UnknownCode"
	}
}

func (s *Sender) Done() <-chan SenderCode {
	return s.doneChan
}

func (s *Sender) done(code SenderCode) {
	select {
	case s.doneChan <- code:
	default:
	}
}

// Start 并发安全
func (s *Sender) Start() (isBusy bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.cancelF != nil {
		return true
	}
	select {
	case <-s.stoppedSig:
	default:
		return true
	}

	s.ctx, s.cancelF = context.WithCancel(s.rawCtx)
	s.stoppedSig = make(chan struct{})

	go func() {
		// exhaust s.doneChan
		select {
		case <-s.doneChan:
		default:
		}

		err := s.sync()
		if err == nil {
			err = s.sendChangeStream()
		}

		// clear cancel chan

		s.mutex.Lock()
		canceled := s.cancelF != nil
		if canceled {
			s.cancelF()
			s.cancelF = nil
		}
		stoppedSig := s.stoppedSig
		s.mutex.Unlock()

		// 先发停止信号，再根据需要向外发出事件通知
		close(stoppedSig)

		// 是本 go func 停止的，就对外发出事件通知
		if canceled {
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
		}

	}()

	return false
}

// Stop 并发安全
func (s *Sender) Stop() {
	_, logger := log.WithCtx(s.ctx)
	s.mutex.Lock()
	if s.cancelF != nil {
		logger.Info("will stop")
		s.cancelF()
		s.cancelF = nil
	}

	stoppedSig := s.stoppedSig
	s.mutex.Unlock()

	// 必须先解锁，再等待，防止死锁
	<-stoppedSig
}

// StopNotWait 并发安全
func (s *Sender) StopNotWait() (stopped <-chan struct{}) {
	_, logger := log.WithCtx(s.ctx)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.cancelF != nil {
		logger.Info("will stop")
		s.cancelF()
		s.cancelF = nil
	}

	return s.stoppedSig
}

// err context.Canceled, ErrSendFailed or unknown
func (s *Sender) sync() error {
	iter := db.SyncTask().All(s.ctx)
	defer iter.Release(s.ctx)

	ctx, logger := log.WithCtx(s.ctx)
	logger.Info("check SyncTask")

	for iter.Next(ctx) {
		task := iter.Current()
		coll := s.client.Database(task.DB).Collection(task.Collection)
		docId := task.UntilDocId

		logger.PushPrefix(fmt.Sprintf("SyncTask:%s", task.Id()))
		logger.Info("send...")

		for {
			cursor, err := coll.Find(ctx, bson.D{{"_id", bson.D{{"$gt", docId}}}},
				options.Find().SetLimit(int64(s.batch)).SetSort(bson.D{{"_id", 1}}))
			if err == context.Canceled {
				logger.Debug(err)
				return err
			}
			if err != nil {
				logger.Error(err)
				return err
			}

			all := make([]bson.Raw, 0, s.batch)
			for cursor.Next(ctx) {
				docId = cursor.Current.Lookup("_id")
				all = append(all, cursor.Current)
			}
			if cursor.Err() == context.Canceled {
				return cursor.Err()
			}

			if len(all) != 0 {
				err = s.netLayer.Send(ctx, Sync, task.DB, task.Collection, all)
			}
			if err != nil {
				logger.Warning("sync sender failed: " + err.Error())
				return err
			}

			err = cursor.Err()

			// finished
			if len(all) < s.batch && err == nil {
				db.SyncTask().Del(ctx, task.Id())
				break
			}

			if err != nil {
				logger.Error("cursor error.", err)
				return err
			}

			// update
			task.UntilDocId = docId
			db.SyncTask().InsertOrUpdate(ctx, task)
		}

		logger.PopPrefix()
	}

	logger.Info("SyncTask end")

	return nil
}

// err context.Canceled, ErrSendFailed, ErrNotFoundSentPoint or unknown
func (s *Sender) sendChangeStream() error {
	iter := db.ChangeStream().AllNotSent(s.ctx)
	defer iter.Release(s.ctx)

	ctx, logger := log.WithCtx(s.ctx)
	logger.Info("check ChangeStreamTask")

	for iter.Next(ctx, s.batch) {
		err := s.netLayer.Send(ctx, ChangeStream, "", "", iter.Values())

		if err != nil {
			logger.Warning("change stream send failed: " + err.Error())
			return err
		}

		db.ChangeStream().MarkSentUpTo(ctx, iter.LastStreamId())
	}

	logger.Info("ChangeStream end")

	return iter.Err()
}
