package db

import (
  "github.com/xpwu/ETLer/etl/config"
  "go.mongodb.org/mongo-driver/bson"
)

type StreamId = string

type StreamValue = bson.Raw

type StreamDBer interface {
  Save(id StreamId, value StreamValue)
  Get(id StreamId) (value StreamValue, err error)
  GetStartWith(id StreamId, limit int) (values []StreamValue, lastId StreamId, err error)
}

type WatchCollectionDBer interface {
  All() []config.WatchInfo
  Save(w []config.WatchInfo)
}

type Task struct {
  Id         string
  // _id
  StartDocId string
}

type SyncTaskDBer interface {
  First() (task Task, ok bool)
  Next(afterId string) (task Task, ok bool)
  Add(task Task)
  Del(id string)
}

type ResumeToken = bson.Raw

type CacheDBer interface {
  ResumeToken() (token ResumeToken, ok bool)
  SaveResumeToken(token ResumeToken)

  SentStreamId() (id StreamId, ok bool)
  SaveSentStreamId(id StreamId)
}
