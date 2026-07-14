package etl

import (
	"github.com/xpwu/ETLer/etl/changestream"
	"github.com/xpwu/ETLer/etl/task"
)

func Start() {
	changestream.StartWatching()
	task.Start()
}
