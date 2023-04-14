package db

import (
  "context"
  "github.com/xpwu/ETLer/etl/config"
  "go.mongodb.org/mongo-driver/bson"
)

type StreamId = []byte

type StreamValue = bson.Raw

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

type WatchCollectionDBer interface {
  All(ctx context.Context) []config.WatchInfo
  Save(ctx context.Context, w []config.WatchInfo)
}

type Task struct {
  config.WatchInfo

  // _id
  StartDocId []byte
}

type SyncTaskIterator interface {
  First(ctx context.Context) (task Task, ok bool)
  Next(ctx context.Context) (task Task, ok bool)
  Release()
}

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
