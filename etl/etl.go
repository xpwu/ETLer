package etl

import (
	"github.com/xpwu/ETLer/etl/changestream"
	"github.com/xpwu/ETLer/etl/task"
)

type listener struct {

}

func (l *listener) ForceSync() {
	task.PostForceSyncAndWait()
}

func (l *listener) StreamChanged() {
	task.PostRunTask()
}

func Start() {
	changestream.SetListener(&listener{})
	changestream.Start()
	task.Start()
}
