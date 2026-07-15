package etl

import (
	"context"
	"github.com/xpwu/ETLer/etl/changestream"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/etl/task"
	"github.com/xpwu/go-log/log"
	"time"
)

func Start() {
	ctx, logger := log.WithCtx(context.TODO())
	for {
		if initTaskFromConfig(ctx) {
			break
		}
		logger.Error("initTaskFromConfig error, will retry after 10s")
		time.Sleep(5 * time.Second)
	}

	changestream.StartWatching()
	task.Start()
}

func WatchCollectionUpdated() {
	task.WatchCollectionUpdated()
	changestream.WatchCollectionUpdated()
}

// todo  config 与 WatchCollection 之间的增量更新
func initTaskFromConfig(ctx context.Context) (succeed bool) {
	ctx, logger := log.WithCtx(ctx)
	defer func() {
		if r := recover(); r != nil {
			logger.Error(r)
			succeed = false
		}
	}()

	oldVersion := db.WatchCollection().LatestVersion(ctx)
	if oldVersion != db.ConfigVersion {
		return
	}

	newM := make(map[string]config.WatchInfo)
	for _, info := range config.Etl.WatchCollections {
		newM[info.Id()] = info
	}

	old := make(map[string]config.WatchInfo)
	all, _ := db.WatchCollection().Latest(ctx)
	for _, info := range all {
		old[info.Id()] = info
	}

	add := make([]db.Task, 0, len(newM))
	for id, info := range newM {
		_, has := old[id]
		if !has {
			add = append(add, task.MinKeyTask(info))
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
	db.WatchCollection().Save(ctx, config.Etl.WatchCollections, db.ConfigVersion)

	return true
}
