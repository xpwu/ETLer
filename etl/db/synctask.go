package db

import (
	"context"
	"github.com/xpwu/ETLer/x"
)

type Task struct {
	x.WatchInfo

	// _id
	StartDocId []byte
}

type SyncTaskIterator interface {
	First(ctx context.Context) (task Task, ok bool)
	Next(ctx context.Context) (task Task, ok bool)
	Release()
}

// SyncTaskDBer 无需支持并发
type SyncTaskDBer interface {
	All(ctx context.Context) SyncTaskIterator

	// InsertOrUpdate task.Id 是唯一标识符，相同的id进行覆盖
	InsertOrUpdate(ctx context.Context, task Task)
	InsertOrUpdateBatch(ctx context.Context, tasks []Task)

	Del(ctx context.Context, id string)
	DelBatch(ctx context.Context, ids []string)
	DelAll(ctx context.Context)
}

var syncTask SyncTaskDBer

func SetSyncTask(s SyncTaskDBer) {
	syncTask = s
}

func SyncTask() SyncTaskDBer {
	return syncTask
}
