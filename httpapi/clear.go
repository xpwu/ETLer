package httpapi

import (
	"context"
	"github.com/xpwu/ETLer/etl"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/go-tinyserver/api"
)

type clearReq struct {
}

// APIClearWC 清除所有监听的 collections
func (s *suite) APIClearWC(ctx context.Context, request *clearReq) *api.EmptyResponse {

	db.WatchCollection().Clear(ctx)
	etl.WatchCollectionUpdated()

	return &api.EmptyResponse{}
}
