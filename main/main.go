package main

import (
	"github.com/xpwu/ETLer/clientcli"
	"github.com/xpwu/ETLer/etl"
	"github.com/xpwu/ETLer/httpapi"
	"github.com/xpwu/ETLer/leveldb"
	"github.com/xpwu/go-cmd/arg"
	"github.com/xpwu/go-cmd/cmd"
	_ "github.com/xpwu/go-cmd/cmd/printconf"
	"github.com/xpwu/go-tinyserver/http"
	"github.com/xpwu/go-x/exe"
)

func main() {
	cmd.RegisterKeepAliveCmd(cmd.DefaultCmdName, "start etler", func(args *arg.Arg) {

		arg.HookReadConfigTo(args)
		args.ParseAndRunHook()

		leveldb.Init(exe.AbsDir)
		etl.Start()
		clientcli.Start()

		httpapi.AddAPI()
		http.Start()
	})

	cmd.Run()
}
