package leveldb

import (
  "context"
  "encoding/json"
  "github.com/syndtr/goleveldb/leveldb"
  "github.com/syndtr/goleveldb/leveldb/iterator"
  "github.com/syndtr/goleveldb/leveldb/util"
  "github.com/xpwu/ETLer/etl/db"
  "github.com/xpwu/go-log/log"
  "path"
)

type syncTask struct {
  db *leveldb.DB
}

type syncTaskIter struct {
  iter iterator.Iterator
}

func marshalTask(ctx context.Context, task *db.Task) []byte {
  _, logger := log.WithCtx(ctx)
  d, err := json.Marshal(task)
  if err != nil {
    logger.Error(err)
    panic(err)
  }

  return d
}

func unMarshalTask(ctx context.Context, d []byte) *db.Task {
  _, logger := log.WithCtx(ctx)
  task := &db.Task{}

  err := json.Unmarshal(d, task)
  if err != nil {
    logger.Error(err)
    panic(err)
  }

  return task
}

func (s *syncTaskIter) First(ctx context.Context) (task db.Task, ok bool) {
  _, logger := log.WithCtx(ctx)
  logger.PushPrefix("syncTaskIter.First")

  ok = s.iter.First()
  if !ok {
    return db.Task{}, false
  }

  return *unMarshalTask(ctx, s.iter.Value()), true
}

func (s *syncTaskIter) Next(ctx context.Context) (task db.Task, ok bool) {
  _, logger := log.WithCtx(ctx)
  logger.PushPrefix("syncTaskIter.Next")

  ok = s.iter.Next()
  if !ok {
    return db.Task{}, false
  }

  return *unMarshalTask(ctx, s.iter.Value()), true
}

func (s *syncTaskIter) Release() {
  s.iter.Release()
}

func (s *syncTask) All(ctx context.Context) db.SyncTaskIterator {
  return &syncTaskIter{
    iter: s.db.NewIterator(&util.Range{
      Start: []byte{0},
      Limit: []byte{0xff},
    }, nil)}
}

func (s *syncTask) InsertOrUpdate(ctx context.Context, task db.Task) {
  _, logger := log.WithCtx(ctx)
  logger.PushPrefix("syncTask.InsertOrUpdate")

  err := s.db.Put([]byte(task.Id()), marshalTask(ctx, &task), nil)
  if err != nil {
    logger.Error(err)
    panic(err)
  }
}

func (s *syncTask) InsertOrUpdateBatch(ctx context.Context, tasks []db.Task) {
  _, logger := log.WithCtx(ctx)
  logger.PushPrefix("syncTask.InsertOrUpdateBatch")

  batch := &leveldb.Batch{}
  for _, task := range tasks {
    batch.Put([]byte(task.Id()), marshalTask(ctx, &task))
  }

  err := s.db.Write(batch, nil)
  if err != nil {
    logger.Error(err)
    panic(err)
  }
}

func (s *syncTask) Del(ctx context.Context, id string) {
  _, logger := log.WithCtx(ctx)
  logger.PushPrefix("syncTask.Del")

  err := s.db.Delete([]byte(id), nil)
  if err != nil {
    logger.Error(err)
    panic(err)
  }
}

func (s *syncTask) DelBatch(ctx context.Context, ids []string) {
  _, logger := log.WithCtx(ctx)
  logger.PushPrefix("syncTask.Del")

  for _, id := range ids {
    err := s.db.Delete([]byte(id), nil)
    if err != nil {
      logger.Error(err)
      panic(err)
    }
  }
}

func (s *syncTask) DelAll(ctx context.Context) {
  ids := make([]string, 0, 100)

  iter := s.All(ctx)
  task, ok := iter.Next(ctx)

  for ok {
    ids = append(ids, task.Id())
    task, ok = iter.Next(ctx)
  }
  iter.Release()

  s.DelBatch(ctx, ids)
}

func newSyncTask(root string) *syncTask {
  p := path.Join(root, "synctask")
  ldb, err := leveldb.OpenFile(p, nil)
  if err != nil {
    panic(err)
  }
  return &syncTask{db: ldb}
}
