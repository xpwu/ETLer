package etl

import (
	"github.com/xpwu/ETLer/etl/changestream"
	"github.com/xpwu/ETLer/etl/task"
)

type listener struct {
}

func (l *listener) NeedForceSync() {
	task.PostForceSyncAndWait()
}

func (l *listener) OnStreamChanged() {
	task.PostRunTask()
}

func Start() {
	changestream.SetListener(&listener{})
	changestream.StartWatching()
	task.Start()
}
