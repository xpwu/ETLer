package etl

import (
	"context"
	"github.com/xpwu/ETLer/etl/changestream"
	"github.com/xpwu/ETLer/etl/task"
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
