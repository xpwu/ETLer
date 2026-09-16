package x

import (
	"context"
	"fmt"
	"github.com/xpwu/go-log/log"
	"github.com/xpwu/go-x/exe"
	"time"
)

type option struct {
	name     string
	duration time.Duration
	ctx      context.Context
}

type Option func(*option)

func WithName(name string) Option {
	return func(o *option) {
		o.name = name
	}
}

func WithCtx(ctx context.Context) Option {
	return func(o *option) {
		o.ctx = ctx
	}
}

func WithDuration(d time.Duration) Option {
	return func(o *option) {
		o.duration = d
	}
}

func AutoRestartPanic(startAndBlock func(context.Context) error, options ...Option) {
	opt := &option{
		name:     exe.Name,
		duration: 30 * time.Second,
		ctx:      context.Background(),
	}

	for _, f := range options {
		f(opt)
	}

	go func() {
		ctx, logger := log.WithCtx(opt.ctx)
		logger.PushPrefix(opt.name)

		var err error = nil

		for {
			func() {
				ctx, cancel := context.WithCancel(ctx)
				ctx, logger := log.WithCtx(ctx)

				defer cancel()

				logger.Info("start")
				defer func() {
					if r := recover(); r != nil {
						logger.Fatal(r)
					}
				}()

				err = startAndBlock(ctx)
			}()

			if err == nil {
				logger.Error("crashed! Will be restarted automatically after 5s")
				time.Sleep(opt.duration)
			} else {
				logger.Fatal(fmt.Sprintf("stopped! Not Served! Because error: %v", err))
				break
			}
		}
	}()
}
