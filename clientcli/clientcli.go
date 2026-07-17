package clientcli

import (
	"context"
	"fmt"
	"github.com/xpwu/ETLer/etl"
	"github.com/xpwu/ETLer/etl/task"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-cmd/arg"
	"github.com/xpwu/go-cmd/clientcli"
	"github.com/xpwu/go-log/log"
)

func Start() {
	clientcli.RegisterCmd("sync", "force sync a collection", func(args *arg.Arg) clientcli.AckToClient {
		ctx, logger := log.WithCtx(context.TODO())

		d, coll := "<db>", "<coll>"
		args.String(&d, "d", "db")
		args.String(&coll, "c", "collection")
		args.ParseAndRunHook()

		logger.Debug(fmt.Sprintf("client-cli: sync %s.%s", d, coll))

		add := []x.WatchInfo{{
			DB:         d,
			Collection: coll},
		}

		if !etl.IsInWatchCollection(ctx, add) {
			return "ERROR: " + d + "." + coll + "is NOT in the Watch Collections"
		}

		task.SyncTaskUpdater() <- task.SyncTaskDelta{
			Add: add,
			Del: []x.WatchInfo{},
		}

		return "OK!"
	})

	clientcli.Start()
}
