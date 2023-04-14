package main

import (
	"github.com/xpwu/ETLer/etl"
	"github.com/xpwu/ETLer/leveldb"
	"github.com/xpwu/go-cmd/arg"
	"github.com/xpwu/go-cmd/cmd"
	"github.com/xpwu/go-cmd/exe"
)

func main()  {
	cmd.RegisterCmd(cmd.DefaultCmdName, "start elter", func(args *arg.Arg) {

		arg.ReadConfig(args)
		args.Parse()

		leveldb.Init(exe.Exe.AbsDir)
		etl.Start()

		// block
		block := make(chan struct{})
		<-block
	})

	cmd.Run()
}
