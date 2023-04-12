package db

import (
  "context"
  "github.com/xpwu/ETLer/etl/config"
  "go.mongodb.org/mongo-driver/bson"
)

type StreamId = string

type StreamValue = bson.Raw

type StreamDBer interface {
  Save(ctx context.Context, id StreamId, value StreamValue)
  Get(ctx context.Context, id StreamId) (value StreamValue, ok bool)
  GetLastOne(ctx context.Context) (id StreamId, ok bool)

  First(ctx context.Context) (id StreamId, value StreamValue, ok bool)
  // Next 必须按照StreamId顺序返回
  Next(ctx context.Context, id StreamId, limit int) (values []StreamValue, lastId StreamId, ok bool)
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

type SyncTaskDBer interface {
  First(ctx context.Context) (task Task, ok bool)
  Next(ctx context.Context, afterId string) (task Task, ok bool)

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
