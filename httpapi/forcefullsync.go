package httpapi

import (
	"context"
	"github.com/xpwu/ETLer/etl/task"
	"github.com/xpwu/go-log/log"
	"github.com/xpwu/go-tinyserver/api"
)

type fullSyncReq struct {
}

// APIForceFullSync 强制全量同步所有监听的 collections
func (s *suite) APIForceFullSync(ctx context.Context, request *clearReq) *api.EmptyResponse {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("api ForceFullSync")

	task.PostForceFullSync()

	return &api.EmptyResponse{}
}
