package task

import (
	"context"
	"flag"
	"fmt"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/go-cmd/arg"
	"github.com/xpwu/go-cmd/clientcli"
	"github.com/xpwu/go-log/log"
)

func runClientCli(ctx context.Context) {
	clientcli.Listen(ctx, "sync", "force sync a collection", func(args *arg.Arg) clientcli.Response {
		_,logger := log.WithCtx(ctx)

		d, coll := "<db>", "<coll>"
		args.String(&d, "d", "db")
		args.String(&coll, "c", "collection")
		err := args.ParseErr()
		if err != nil {
			if err == flag.ErrHelp {
				return ""
			}
			return "ERROR: " + err.Error()
		}

		logger.Debug(fmt.Sprintf("client-cli: sync %s.%s", d, coll))

		err = SyncATask(&config.WatchInfo{
			DB:         d,
			Collection: coll,
		})
		if err != nil {
			return "ERROR: " + err.Error()
		}

		return "OK!"
	})
}


