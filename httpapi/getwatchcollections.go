package httpapi

import (
	"context"
	"fmt"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-log/log"
)

type getWcRes struct {
	Version    uint64        `json:"version"`
	WatchInfos []x.WatchInfo `json:"watchInfos"`
}

type getWcReq struct {
}

func (s *suite) APIGetWatchCols(ctx context.Context, request *getWcReq) *getWcRes {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("api GetWatchCols")

	version := db.WatchCollection().LatestVersion(ctx)
	wc := db.WatchCollection().Get(ctx, version)

	logger.Info(fmt.Sprintf("ver=%d, len(WatchInfos)=%d", version, len(wc)))

	return &getWcRes{version, wc}
}
