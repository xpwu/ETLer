package task

import (
  "context"
  "github.com/xpwu/ETLer/etl/config"
  "github.com/xpwu/ETLer/etl/db"
  "github.com/xpwu/go-db-mongo/mongodb/mongocache"
  "github.com/xpwu/go-log/log"
  "go.mongodb.org/mongo-driver/bson"
  "go.mongodb.org/mongo-driver/bson/bsontype"
  "go.mongodb.org/mongo-driver/mongo/options"
  "sync/atomic"
  "time"
)

// 时序很重要，sync 与 change stream 都是串行处理，每一批也都是串行处理，即使是停止，也必须等待停止后，才能新启动

type taskRunner struct {
  ctx    context.Context
  client *mongocache.Client
  hasRun int32
}

func startAndBlock(ctx context.Context) {
  ctx, logger := log.WithCtx(ctx)
  client, err := mongocache.Get(ctx, config.Etl.Deployment)
  if err != nil {
    logger.Error(err)
    panic(err)
  }

  tr := &taskRunner{
    ctx:    ctx,
    client: client,
    hasRun: 0,
  }

  tr.initTaskFromConfig()

  // todo chan: forcesync  changestream  timeout

  stop := make(chan struct{})
  forceSync := make(chan struct{}, 1)
  changestream := make(chan struct{}, 1)
  timeout := time.NewTimer(5 * time.Minute)

  for {
    select {
    case <-forceSync:
      // todo stop
      stop <- struct{}{}
      tr.forceSync()
      go tr.run(stop)
    case <-changestream:
      // exhaust
    innerFor:
      for {
        select {
        case <-changestream:
        default:
          break innerFor
        }
      }

      go tr.run(stop)
    case <-timeout.C:
      go tr.run(stop)
    }
  }

}

func (t *taskRunner) initTaskFromConfig() {
  newM := make(map[string]config.WatchInfo)
  for _, info := range config.Etl.WatchCollections {
    newM[info.Id()] = info
  }

  old := make(map[string]config.WatchInfo)
  for _, info := range db.WatchCollection().All(t.ctx) {
    old[info.Id()] = info
  }

  add := make([]db.Task, 0, len(newM))
  for id, info := range newM {
    _, has := old[id]
    if !has {
      add = append(add, MinKeyTask(info))
    }
  }
  db.SyncTask().InsertOrUpdateBatch(t.ctx, add)

  del := make([]string, 0, len(old))
  for id, info := range old {
    _, has := newM[id]
    if !has {
      del = append(del, info.Id())
    }
  }
  db.SyncTask().DelBatch(t.ctx, del)

  // 必须最后保存此项，防止前面异常出错
  db.WatchCollection().Save(t.ctx, config.Etl.WatchCollections)
}

func (t *taskRunner) forceSync() {
  streamId, ok := db.Stream().GetLastOne(t.ctx)
  t.reinitTask()

  // reinit 成功后才能更改cache，防止前面程序异常，而漏掉了sync的设置
  // 但是就可能出现：已经reinit了，但是cache失败，会造成两种情况：
  //   1、因为没有及时更新cache，sync 后，发现之前cache的stream不能成功恢复，会再次触发sync
  //   2、sync 的数据与 stream 的数据重复很多，会重复发很多没必要的stream
  //  以上两点，只是会造成执行的重叠。本着一定不能漏数据的原则，宁重不漏。
  // 后续再思考是否有优化的方式
  if ok {
    db.Cache().SaveSentStreamId(t.ctx, streamId)
  } else {
    db.Cache().DelSentStreamId(t.ctx)
  }
}

func (t *taskRunner) reinitTask() {
  add := make([]db.Task, 0, len(config.Etl.WatchCollections))
  for _, info := range config.Etl.WatchCollections {
    add = append(add, MinKeyTask(info))
  }

  db.SyncTask().DelAll(t.ctx)
  db.SyncTask().InsertOrUpdateBatch(t.ctx, add)
  // 防御性代码
  db.WatchCollection().Save(t.ctx, config.Etl.WatchCollections)
}

func (t *taskRunner) run(stop <-chan struct{}) {
  canRun := atomic.CompareAndSwapInt32(&t.hasRun, 0, 1)
  if !canRun {
    return
  }

  _, sync := db.SyncTask().First(t.ctx)
  defer func() {
    atomic.StoreInt32(&t.hasRun, 0)

    // ensure the stop channel is empty
    select {
    case <-stop:
    default:
    }
  }()

  if sync {
    if err := t.sync(stop); err != nil {
      panic(err)
    }
  }
  if err := t.changeStream(stop); err != nil {
    panic(err)
  }
}

func MinKeyTask(info config.WatchInfo) db.Task {
  return db.Task{
    StartDocId: serialize(bson.RawValue{Type: bsontype.MinKey}),
    WatchInfo:  info,
  }
}

func serialize(value bson.RawValue) []byte {
  ret := make([]byte, 1, 1+len(value.Value))
  ret[0] = byte(value.Type)
  return append(ret, value.Value...)
}

func deserialize(bytes []byte) bson.RawValue {
  return bson.RawValue{
    Type:  bsontype.Type(bytes[0]),
    Value: bytes[1:],
  }
}

var batch = 1000

func (t *taskRunner) sync(stop <-chan struct{}) error {
  ctx, logger := log.WithCtx(t.ctx)

  task, ok := db.SyncTask().First(ctx)
  for ok {
    select {
    case <-stop:
      logger.Info("sync is stopped by caller")
      return nil
    case <-ctx.Done():
      logger.Error(ctx.Err())
      return ctx.Err()
    default:
    }

    coll := t.client.Database(task.DB).Collection(task.Collection)
    docId := deserialize(task.StartDocId)

    for {
      select {
      case <-stop:
        logger.Info("sync is stopped by caller")
        return nil
      case <-ctx.Done():
        logger.Error(ctx.Err())
        return ctx.Err()
      default:
      }

      cursor, err := coll.Find(ctx, bson.D{{"_id", bson.D{{"$gt", docId}}}},
        options.Find().SetLimit(int64(batch)).SetSort(bson.D{{"_id", 1}}))
      if err != nil {
        logger.Error(err)
        return err
      }

      all := make([]bson.Raw, 0, batch)
      i := 0
      for cursor.Next(ctx) {
        i += 1
        docId = cursor.Current.Lookup("_id")
        all = append(all, cursor.Current)
      }

      // todo send  all to upstream

      // over
      if i < batch {
        db.SyncTask().Del(ctx, task.Id())
        break
      }
      // update
      task.StartDocId = serialize(docId)
      db.SyncTask().InsertOrUpdate(ctx, task)
    }

    task, ok = db.SyncTask().Next(ctx, task.Id())
  }

  return nil
}

func (t *taskRunner) changeStream(stop <-chan struct{}) error {
  ctx, logger := log.WithCtx(t.ctx)

  streamId, ok := db.Cache().SentStreamId(ctx)
  values := make([]db.StreamValue, 0, batch)

  if ok {
    _, ok = db.Stream().Get(ctx, streamId)
    // 之前发送过的stream 已经不能在stream找到，说明中间有断层，必须force sync
    if !ok {
      // todo force sync
      return nil
    }
    values, streamId, ok = db.Stream().Next(ctx, streamId, 1)
    if !ok {
      return nil
    }
  } else {
    var value db.StreamValue
    streamId, value, ok = db.Stream().First(ctx)

    if !ok {
      logger.Info("no stream")
      return nil
    }
    values = append(values, value)
  }

  for ok {
    select {
    case <-stop:
      logger.Info("sync is stopped by caller")
      return nil
    case <-ctx.Done():
      logger.Error(ctx.Err())
      return ctx.Err()
    default:
    }

    // todo send values

    db.Cache().SaveSentStreamId(ctx, streamId)
    values, streamId, ok = db.Stream().Next(ctx, streamId, batch)
  }

  return nil
}
