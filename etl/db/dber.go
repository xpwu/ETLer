package db

import (
	"context"
	"github.com/xpwu/ETLer/x"
	"go.mongodb.org/mongo-driver/bson"
)

type StreamId = x.StreamId

type StreamValue = x.StreamValue

type StreamIterator interface {
	First(ctx context.Context) (id StreamId, value StreamValue, ok bool)
	Last(ctx context.Context) (id StreamId, ok bool)

	// Next 必须按照StreamId顺序返回
	Next(ctx context.Context, limit int) (values []StreamValue, lastId StreamId, ok bool)

	Release()
}

type StreamDBer interface {
	// Save token: StreamValue的一个唯一值，常用 resume token
	Save(ctx context.Context, token []byte, value StreamValue) (id StreamId)
	Get(ctx context.Context, id StreamId) (value StreamValue, ok bool)

	All(ctx context.Context) StreamIterator
	StartWith(ctx context.Context, id StreamId) StreamIterator

	GetLastOne(ctx context.Context) (id StreamId, ok bool)
}

const ConfigVersion uint64 = 0

// WCAccessor 需要支持并发
type WCAccessor interface {
	LatestVersion(ctx context.Context) uint64
	// Save latestVersion >= version 什么也不改变
	Save(ctx context.Context, w []x.WatchInfo, version uint64) (latestVersion uint64)
	Get(ctx context.Context, version uint64) []x.WatchInfo
	DelLessThan(ctx context.Context, version uint64)
	Clear(ctx context.Context)
}

// WCTaskifier 无需支持并发
type WCTaskifier interface {
	NeedFullSyncing(ctx context.Context) bool
	MarkFullSyncing(ctx context.Context)

	NeedDeltaSyncing(ctx context.Context) (version uint64, need bool)
	DeltaSyncing(ctx context.Context, version uint64)

	ClearSyncingAndMarkSynced(ctx context.Context, version uint64)
	LatestSynced(ctx context.Context) (version uint64)
}

type WatchCollectionDBer interface {
	WCAccessor
	WCTaskifier
}

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

// SyncTaskDBer 不保证并发安全
type SyncTaskDBer interface {
	All(ctx context.Context) SyncTaskIterator

	// InsertOrUpdate task.Id 是唯一标识符，相同的id进行覆盖
	InsertOrUpdate(ctx context.Context, task Task)
	InsertOrUpdateBatch(ctx context.Context, tasks []Task)

	Del(ctx context.Context, id string)
	DelBatch(ctx context.Context, ids []string)
	DelAll(ctx context.Context)
}

type ResumeToken = bson.Raw

type CacheDBer interface {
	ResumeToken(ctx context.Context) (token ResumeToken, ok bool)
	SaveResumeToken(ctx context.Context, token ResumeToken)

	SentStreamId(ctx context.Context) (id StreamId, ok bool)
	DelSentStreamId(ctx context.Context)
	SaveSentStreamId(ctx context.Context, id StreamId)
}
