package clientcli

import (
	"context"
	"flag"
	"fmt"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/etl/task"
	"github.com/xpwu/go-cmd/arg"
	"github.com/xpwu/go-cmd/clientcli"
	"github.com/xpwu/go-log/log"
)

func Start() {
	ctx := context.TODO()
	clientcli.Listen(ctx, "sync", "force sync a collection", func(args *arg.Arg) clientcli.Response {
		_, logger := log.WithCtx(ctx)

		d, coll := "<db>", "<coll>"
		args.String(&d, "d", "db")
		args.String(&coll, "c", "collection")
		err := args.ParseAndRunHookErr()
		if err != nil {
			if err == flag.ErrHelp {
				return ""
			}
			return "ERROR: " + err.Error()
		}

		logger.Debug(fmt.Sprintf("client-cli: sync %s.%s", d, coll))

		add := config.WatchInfo{
			DB:         d,
			Collection: coll,
		}

		all := db.WatchCollection().Get(ctx, db.WatchCollection().LatestVersion(ctx))
		m := make(map[string]bool)
		for _, c := range all {
			m[c.Id()] = true
		}

		if !m[add.Id()] {
			return "ERROR: " + d + "." + coll + "is NOT in the Watch Collections"
		}

		task.SyncTaskUpdater() <- task.SyncTaskDelta{
			Add: []config.WatchInfo{add},
			Del: []config.WatchInfo{},
		}

		return "OK!"
	})
}
