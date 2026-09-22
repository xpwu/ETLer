package httpapi

import (
	"context"
	"github.com/xpwu/ETLer/etl"
	"github.com/xpwu/ETLer/etl/task"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-log/log"
)

type syncCollectionReq struct {
	Collections []x.WatchInfo `json:"colls"`
}

type syncWcRes struct {
	Succeed bool
}

// APIForceSyncColl 强制全量同步 syncWcReq 指定的 collections, 指定的 collection 必须是之前配置/设置的监听集合的子集
func (s *suite) APIForceSyncColl(ctx context.Context, request *syncCollectionReq) *syncWcRes {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("api SyncWatchCol ")

	if !etl.IsInWatchCollection(ctx, request.Collections) {
		return &syncWcRes{false}
	}
	task.SyncTaskUpdater() <- task.SyncTaskDelta{Add: request.Collections, Del: []x.WatchInfo{}}

	return &syncWcRes{true}
}
