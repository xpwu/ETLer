package etl

import (
	"context"
	"github.com/xpwu/ETLer/etl/changestream"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/etl/task"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-log/log"
	"time"
)

func Start() {
	ctx, logger := log.WithCtx(context.TODO())
	for {
		if task.InitTaskFromConfig(ctx) {
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

func IsInWatchCollection(ctx context.Context, checking []x.WatchInfo) bool {
	ctx, logger := log.WithCtx(ctx)
	all := db.WatchCollection().Get(ctx, db.WatchCollection().LatestVersion(ctx))
	m := make(map[string]bool)
	for _, c := range all {
		m[c.Id()] = true
	}

	for _, wi := range checking {
		if !m[wi.Id()] {
			logger.Error(wi.DB + "." + wi.Collection + "is NOT in the Watch Collections")
			return false
		}
	}

	return true
}
