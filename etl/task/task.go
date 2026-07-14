package task

import (
	"context"
	"github.com/xpwu/ETLer/etl/changestream"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/go-cmd/x"
	"github.com/xpwu/go-db-mongo/mongodb/mongocache"
	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"time"
)

const (
	batch int = 1000
	retry     = 1 * time.Minute
)

type SyncTaskDiff struct {
	Add []config.WatchInfo
	Del []config.WatchInfo
}

var (
	updateSyncTaskChan = make(chan SyncTaskDiff)
)

func SyncTaskUpdater() chan<- SyncTaskDiff {
	return updateSyncTaskChan
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

	initTaskFromConfig(ctx)
	// todo: checkSyncTask()

	runner := NewRunner(ctx, client, batch)

	runner.Start()

	again := false
	// 必须先停止 Runner，才能更新 同步任务。防止任务更新后被意外改写
	for {
		select {
		case code := <-runner.Done():
			switch code {
			case NeedForceSync:
				addAllSyncTask(ctx)
				runner.Start()
			case SendFailed:
				time.Sleep(retry)
				runner.Start()
			case Ok:
				if again {
					again = runner.Start()
				}
			case UnknownErr:
				panic("error, wait for restarting")
			}
		case <-changestream.NeedForceSync():
			runner.Stop()
			addAllSyncTask(ctx)
			runner.Start()
		case <-changestream.OnStreamChanged():
			again = runner.Start()
		case d := <-updateSyncTaskChan:
			runner.Stop()
			updateSyncTask(ctx, d.Add, d.Del)
			runner.Start()
		}
	}
}

func addAllSyncTask(ctx context.Context) {
	collections := db.WatchCollection().All(ctx)
	add := make([]db.Task, 0, len(collections))
	for _, info := range collections {
		add = append(add, MinKeyTask(info))
	}

	db.SyncTask().DelAll(ctx)
	db.SyncTask().InsertOrUpdateBatch(ctx, add)
}

func initTaskFromConfig(ctx context.Context) {
	newM := make(map[string]config.WatchInfo)
	for _, info := range config.Etl.WatchCollections {
		newM[info.Id()] = info
	}

	old := make(map[string]config.WatchInfo)
	for _, info := range db.WatchCollection().All(ctx) {
		old[info.Id()] = info
	}

	add := make([]db.Task, 0, len(newM))
	for id, info := range newM {
		_, has := old[id]
		if !has {
			add = append(add, MinKeyTask(info))
		}
	}
	db.SyncTask().InsertOrUpdateBatch(ctx, add)

	del := make([]string, 0, len(old))
	for id, info := range old {
		_, has := newM[id]
		if !has {
			del = append(del, info.Id())
		}
	}
	db.SyncTask().DelBatch(ctx, del)

	// 必须最后保存此项，防止前面异常出错
	db.WatchCollection().Save(ctx, config.Etl.WatchCollections)
}

func updateSyncTask(ctx context.Context, add []config.WatchInfo, del []config.WatchInfo) {
	//collections := db.WatchCollection().All(ctx)

	addT := make([]db.Task, 0, len(add))
	for _, info := range add {
		addT = append(addT, MinKeyTask(info))
	}

	delIds := make([]string, 0, len(del))
	for _, d := range del {
		delIds = append(delIds, d.Id())
	}

	db.SyncTask().InsertOrUpdateBatch(ctx, addT)
	db.SyncTask().DelBatch(ctx, delIds)
}

func MinKeyTask(info config.WatchInfo) db.Task {
	return db.Task{
		StartDocId: serialize(bson.RawValue{Type: bsontype.MinKey}),
		WatchInfo:  info,
	}
}
