package httpapi

import (
	"context"
	"github.com/xpwu/ETLer/etl"
	"github.com/xpwu/ETLer/etl/task"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-log/log"
)

type syncWcReq struct {
	WatchCols []x.WatchInfo `json:"watchcols"`
}

type syncWcRes struct {
	Succeed bool
}

func (s *suite) APISyncWatchCol(ctx context.Context, request *syncWcReq) *syncWcRes {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("api SyncWatchCol ")

	if !etl.IsInWatchCollection(ctx, request.WatchCols) {
		return &syncWcRes{false}
	}
	task.SyncTaskUpdater() <- task.SyncTaskDelta{Add: request.WatchCols, Del: []x.WatchInfo{}}

	return &syncWcRes{true}
}
