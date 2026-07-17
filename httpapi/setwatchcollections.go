package httpapi

import (
	"context"
	"fmt"
	"github.com/xpwu/ETLer/etl"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-log/log"
)

type setWcReq struct {
	Version    uint64        `json:"version"`
	WatchInfos []x.WatchInfo `json:"watchInfos"`
}

type setWcRes struct {
	OldVersion uint64 `json:"oldver"`
	NowVersion uint64 `json:"nowver"`
}

func (s *suite) APISetWatchCols(ctx context.Context, request *setWcReq) *setWcRes {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("api SetWatchCols")

	old, now := db.WatchCollection().Save(ctx, request.WatchInfos, request.Version)
	logger.Info(fmt.Sprintf("expect version: %d, result: old=%d, now=%d"), request.Version, old, now)

	if now != request.Version {
		etl.WatchCollectionUpdated()
	}

	return &setWcRes{}
}
