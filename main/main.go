package main

import (
	"fmt"
	"github.com/xpwu/ETLer/bboltdb"
	"github.com/xpwu/ETLer/etl"
	"github.com/xpwu/ETLer/httpapi"
	"github.com/xpwu/go-cmd/arg"
	"github.com/xpwu/go-cmd/cmd"
	_ "github.com/xpwu/go-cmd/cmd/printconf"
	"github.com/xpwu/go-log/log"
	"github.com/xpwu/go-tinyserver/http"
	"os"
)

func main() {
	cmd.RegisterKeepAliveCmd(cmd.DefaultCmdName, "start etler", func(args *arg.Arg) {

		arg.HookReadConfigTo(args)
		args.ParseAndRunHook()

		err := bboltdb.Init()
		if err != nil {
			log.Error(fmt.Sprintf("init bbolt db error: %s", err))
			os.Exit(2)
		}

		etl.Start()

		httpapi.AddAPI()
		http.Start()
	})

	cmd.Run()
}
