package x

import (
	"context"
	"github.com/xpwu/go-log/log"
	"time"
)

func AutoRestart(ctx context.Context, name string, startAndBlock func(context.Context)) {
	go func() {
		ctx, logger := log.WithCtx(ctx)
		logger.PushPrefix(name)

		for {
			func() {
				ctx, cancel := context.WithCancel(ctx)
				ctx, logger := log.WithCtx(ctx)

				logger.Info("start")
				defer func() {
					if r := recover(); r != nil {
						logger.Fatal(r)
					}
					cancel()
				}()

				startAndBlock(ctx)
			}()

			logger.Error("crashed! Will be restarted automatically after 5s")
			time.Sleep(5 * time.Second)
		}
	}()
}
