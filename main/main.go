package main

import (
	"github.com/xpwu/ETLer/etl"
	"github.com/xpwu/go-cmd/arg"
	"github.com/xpwu/go-cmd/cmd"
)

func main()  {
	cmd.RegisterCmd(cmd.DefaultCmdName, "start elter", func(args *arg.Arg) {

		arg.ReadConfig(args)
		args.Parse()

		// todo set db

		etl.Start()

		// block
		block := make(chan struct{})
		<-block
	})

	cmd.Run()
}
