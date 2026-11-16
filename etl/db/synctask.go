package db

import (
	"context"
	"github.com/xpwu/ETLer/x"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type Task struct {
	x.WatchInfo

	// UntilDocId 表示该任务已经发送完的截止点，使用 _id 的值来表示该截止点，按照 Mongo 的 bson order 比较 _id 的值
	UntilDocId bson.RawValue
}

// SyncTaskIterator 迭代器返回的数据必须是独立副本，不得引用 DB 内部存储；
// 迭代期间调用方可安全执行本接口的任意写方法，实现方不得持有长事务或锁。
type SyncTaskIterator interface {
	// Next 第一次也必须调用 Next, Current() 才会有值。迭代完数据库中已有的值或者有错，返回 false
	// 如果 ctx.Cancelled, Err() 将返回此错误
	Next(ctx context.Context) (ok bool)
	Current() Task
	// Err 如果 context.Canceled, Err() 将返回此错误
	// nil if no error has occurred.
	Err() error
	Release(ctx context.Context)
}

// SyncTaskDBer 无需支持并发，但在迭代器 Next() 遍历期间，会调用 InsertOrUpdate 更新 Current() 的 Task
// 已经同步完的 Task 需要调用 Del / DelBatch 删除
// 只同步了部分文档的 Task 需要重置该 Task 的 UntilDocId 后，调用 InsertOrUpdate / InsertOrUpdateBatch 更新该 Task
type SyncTaskDBer interface {
	All(ctx context.Context) SyncTaskIterator

	// InsertOrUpdate task.Id 是唯一标识符，相同的id进行覆盖
	InsertOrUpdate(ctx context.Context, task Task)
	InsertOrUpdateBatch(ctx context.Context, tasks []Task)

	Del(ctx context.Context, id string)
	DelBatch(ctx context.Context, ids []string)

	Clear(ctx context.Context)
}

var syncTask SyncTaskDBer

func SetSyncTask(s SyncTaskDBer) {
	syncTask = s
}

func SyncTask() SyncTaskDBer {
	return syncTask
}
