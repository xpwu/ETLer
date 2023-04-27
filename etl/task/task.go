package task

import (
	"context"
	"errors"
	"fmt"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/etl/x"
	"github.com/xpwu/go-db-mongo/mongodb/mongocache"
	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync/atomic"
	"time"
)

// 时序很重要，sync 与 change stream 都是串行处理，每一批也都是串行处理，即使是停止，也必须等待停止后，才能新启动

const (
	canRun = iota
	running
	stopped

	batch   int = 1000
	maxWait     = 10 * time.Minute
	retry       = 1 * time.Minute
)

type taskRunner struct {
	ctx     context.Context
	client  *mongocache.Client
	state   int32
	timeout *time.Timer
}

var (
	forceSync    = make(chan struct{}, 1)
	syncPrepared = make(chan struct{}, 1)

	runTask = make(chan struct{}, 1)

	syncATask     = make(chan *config.WatchInfo, 1)
	syncATaskDone = make(chan error, 1)
)

func PostForceSync() {
	select {
	case forceSync <- struct{}{}:
	default:
	}
}

// PostForceSyncAndWait sync all
func PostForceSyncAndWait() {
	exhaust(syncPrepared)
	PostForceSync()
	<-syncPrepared
}

func SyncATask(w *config.WatchInfo) error {
	exhaust2(syncATaskDone)
	syncATask <- w
	err := <-syncATaskDone
	return err
}

func PostRunTask() {
	select {
	case runTask <- struct{}{}:
	default:
	}
}

func exhaust(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func exhaust2(ch <-chan error) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func Start() {
	x.AutoRestart(context.TODO(), "send-task", startAndBlock)
}

func startAndBlock(ctx context.Context) {
	ctx, logger := log.WithCtx(ctx)
	client, err := mongocache.Get(ctx, config.Etl.Deployment)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	runClientCli(ctx)

	tr := &taskRunner{
		ctx:     ctx,
		client:  client,
		state:   canRun,
		timeout: time.NewTimer(maxWait),
	}

	tr.initTaskFromConfig()

	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-forceSync:
				if atomic.CompareAndSwapInt32(&tr.state, running, stopped) {
					stop <- struct{}{}
				}
				atomic.StoreInt32(&tr.state, stopped)
				tr.forceSync()
				atomic.StoreInt32(&tr.state, canRun)

				select {
				case syncPrepared <- struct{}{}:
				default:
				}

				PostRunTask()

			case w := <-syncATask:
				if atomic.CompareAndSwapInt32(&tr.state, running, stopped) {
					stop <- struct{}{}
				}
				atomic.StoreInt32(&tr.state, stopped)
				err := tr.forceSyncATask(w)
				atomic.StoreInt32(&tr.state, canRun)

				PostRunTask()

				syncATaskDone <- err
			}
		}
	}()

	tr.run(stop)

	for {
		select {
		case <-runTask:
			if !tr.timeout.Stop() {
				<-tr.timeout.C
			}
			tr.timeout.Reset(maxWait)
			exhaust(runTask)
			tr.run(stop)
		case <-tr.timeout.C:
			tr.timeout.Reset(maxWait)
			tr.run(stop)
		}
	}

}

func (t *taskRunner) initTaskFromConfig() {
	newM := make(map[string]config.WatchInfo)
	for _, info := range config.Etl.WatchCollections {
		newM[info.Id()] = info
	}

	old := make(map[string]config.WatchInfo)
	for _, info := range db.WatchCollection().All(t.ctx) {
		old[info.Id()] = info
	}

	add := make([]db.Task, 0, len(newM))
	for id, info := range newM {
		_, has := old[id]
		if !has {
			add = append(add, MinKeyTask(info))
		}
	}
	db.SyncTask().InsertOrUpdateBatch(t.ctx, add)

	del := make([]string, 0, len(old))
	for id, info := range old {
		_, has := newM[id]
		if !has {
			del = append(del, info.Id())
		}
	}
	db.SyncTask().DelBatch(t.ctx, del)

	// 必须最后保存此项，防止前面异常出错
	db.WatchCollection().Save(t.ctx, config.Etl.WatchCollections)
}

func (t *taskRunner) forceSync() {
	streamId, ok := db.Stream().GetLastOne(t.ctx)
	t.reinitTask()

	// reinit 成功后才能更改cache，防止前面程序异常，而漏掉了sync的设置
	// 但是就可能出现：已经reinit了，但是cache失败，会造成两种情况：
	//   1、因为没有及时更新cache，sync 后，发现之前cache的stream不能成功恢复，会再次触发sync
	//   2、sync 的数据与 stream 的数据重复很多，会重复发很多没必要的stream
	//  以上两点，只是会造成执行的重叠。本着一定不能漏数据的原则，宁重不漏。
	// 后续再思考是否有优化的方式
	if ok {
		db.Cache().SaveSentStreamId(t.ctx, streamId)
	} else {
		db.Cache().DelSentStreamId(t.ctx)
	}
}

func (t *taskRunner) reinitTask() {
	add := make([]db.Task, 0, len(config.Etl.WatchCollections))
	for _, info := range config.Etl.WatchCollections {
		add = append(add, MinKeyTask(info))
	}

	db.SyncTask().DelAll(t.ctx)
	db.SyncTask().InsertOrUpdateBatch(t.ctx, add)
	// 防御性代码
	db.WatchCollection().Save(t.ctx, config.Etl.WatchCollections)
}

func (t *taskRunner) forceSyncATask(wi *config.WatchInfo) error {
	_, logger := log.WithCtx(t.ctx)

	inConfig := false
	for _, info := range config.Etl.WatchCollections {
		if info == *wi {
			inConfig = true
			break
		}
	}
	if !inConfig {
		return fmt.Errorf("%s is not in config file", wi.Id())
	}

	db.SyncTask().InsertOrUpdate(t.ctx, MinKeyTask(*wi))
	logger.Info("will sync ", wi.Id())
	return nil
}

func (t *taskRunner) run(stop <-chan struct{}) {
	if !atomic.CompareAndSwapInt32(&t.state, canRun, running) {
		return
	}

	iter := db.SyncTask().All(t.ctx)
	_, sync := iter.First(t.ctx)
	iter.Release()

	defer func() {
		atomic.CompareAndSwapInt32(&t.state, running, canRun)
		// ensure the stop channel is empty
		exhaust(stop)
	}()

	if sync {
		err := t.sync(stop)
		if err == senderErr || err == stoppedErr {
			return
		}
		if err != nil {
			panic(err)
		}
	}

	err := t.changeStream(stop)
	if err == senderErr || err == stoppedErr {
		return
	}
	if err != nil {
		panic(err)
	}
}

func MinKeyTask(info config.WatchInfo) db.Task {
	return db.Task{
		StartDocId: serialize(bson.RawValue{Type: bsontype.MinKey}),
		WatchInfo:  info,
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

var senderErr = errors.New("sender error")
var stoppedErr = errors.New("stopped")

func selectErr(isStop <-chan bool, elseErr error) error {
	select {
	case <-isStop:
		return stoppedErr
	default:
		return elseErr
	}
}

func (t *taskRunner) sync(stop <-chan struct{}) error {
	ctx, logger := log.WithCtx(t.ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	isStop := make(chan bool, 1)

	iter := db.SyncTask().All(ctx)
	defer iter.Release()

	go func() {
		select {
		case <-stop:
			logger.Info("sync is stopped by caller")
			isStop <- true
			cancel()
		case <-ctx.Done():
			logger.Error(ctx.Err())
		}
	}()

	task, ok := iter.First(ctx)
	for ok {
		coll := t.client.Database(task.DB).Collection(task.Collection)
		docId := deserialize(task.StartDocId)

		for {
			cursor, err := coll.Find(ctx, bson.D{{"_id", bson.D{{"$gt", docId}}}},
				options.Find().SetLimit(int64(batch)).SetSort(bson.D{{"_id", 1}}))
			if err != nil {
				logger.Error(err)
				return selectErr(isStop, err)
			}

			all := make([]bson.Raw, 0, batch)
			i := 0
			for cursor.Next(ctx) {
				i += 1
				docId = cursor.Current.Lookup("_id")
				all = append(all, cursor.Current)
			}

			if !Sender.Do(ctx, Sync, task.DB, task.Collection, all) {
				if !t.timeout.Stop() {
					<-t.timeout.C
				}
				t.timeout.Reset(retry)
				logger.Warning("sync sender error. retry after ", retry.Seconds(), " s")
				return senderErr
			}

			err = cursor.Err()

			// over
			if i < batch && err == nil {
				db.SyncTask().Del(ctx, task.Id())
				break
			}
			// update
			task.StartDocId = serialize(docId)
			db.SyncTask().InsertOrUpdate(ctx, task)

			if err != nil {
				logger.Error("cursor error.", err)
				return selectErr(isStop, err)
			}
		}

		task, ok = iter.Next(ctx)
	}

	return nil
}

func (t *taskRunner) changeStream(stop <-chan struct{}) error {
	ctx, logger := log.WithCtx(t.ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	isStop := make(chan bool, 1)

	go func() {
		select {
		case <-stop:
			logger.Info("change stream task is stopped by caller")
			isStop <- true
			cancel()
		case <-ctx.Done():
			logger.Error(ctx.Err())
		}
	}()

	streamId, ok := db.Cache().SentStreamId(ctx)
	values := make([]db.StreamValue, 0, batch)
	var iter db.StreamIterator
	if ok {
		iter = db.Stream().StartWith(ctx, streamId)
	} else {
		iter = db.Stream().All(ctx)
	}
	defer iter.Release()

	if ok {
		var newId db.StreamId
		newId, _, ok = iter.First(ctx)

		// 之前发送过的stream 已经不能在stream找到，说明中间有断层，必须force sync
		if string(newId) != string(streamId) {
			PostForceSync()
			return nil
		}

		values, streamId, ok = iter.Next(ctx, 1)
		if !ok {
			return nil
		}
	} else {
		var value db.StreamValue
		streamId, value, ok = iter.First(ctx)

		if !ok {
			logger.Info("sendChangeStream: has not stream to send")
			return nil
		}
		values = append(values, value)
	}

	for ok {
		if !Sender.Do(ctx, ChangeStream, "", "", values) {
			if !t.timeout.Stop() {
				<-t.timeout.C
			}
			t.timeout.Reset(retry)
			logger.Warning("changeStream sender error. retry after ", retry.Seconds(), " s")
			return senderErr
		}

		db.Cache().SaveSentStreamId(ctx, streamId)
		values, streamId, ok = iter.Next(ctx, batch)
	}

	return nil
}
