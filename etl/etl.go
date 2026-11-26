package etl

import (
	"context"
	"github.com/xpwu/ETLer/etl/changestream"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/etl/task"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-log/log"
)

func Start() {
	task.InitTaskFromConfig(context.TODO())
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
			logger.Error(wi.DB + "." + wi.Collection + " is NOT in the Watch Collections")
			return false
		}
	}

	return true
}
