package mongodb

import (
	"context"
	"fmt"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

//go:generate go run github.com/xpwu/go-mongodb/cmd/gomongodbgen
type cache struct {
	ID    string `bson:"_id"`
	Value bson.Raw
}

type CacheDBer struct {
	Client *mongo.Client
	DB     string
}

const tokenId = "tokenId"
const streamId = "sentStreamId"
const taskPre = "task-"

func taskId(task *db.Task) string {
	return taskPre + task.Id()
}

func (c *CacheDBer) coll() *mongo.Collection {
	return c.Client.Database(c.DB).Collection(cacheColl.DefaultName)
}

func (c *CacheDBer) ResumeToken(ctx context.Context) (token x.ResumeToken, ok bool) {
	res := c.coll().FindOne(ctx, cacheColl.IDF().Eq(tokenId))

	var ret cache
	err := res.Decode(&ret)

	if err == mongo.ErrNoDocuments {
		return nil, false
	}

	if err != nil {
		panic(err)
	}

	return ret.Value, true
}

func (c *CacheDBer) SaveResumeToken(ctx context.Context, token x.ResumeToken) {
	_, err := c.coll().UpdateByID(ctx, tokenId, cacheColl.ValueF().Set(token), options.UpdateOne().SetUpsert(true))
	if err != nil {
		panic(err)
	}
}

func (c *CacheDBer) SentStreamId(ctx context.Context) (id x.StreamId, ok bool) {
	res := c.coll().FindOne(ctx, cacheColl.IDF().Eq(streamId))

	var ret cache
	err := res.Decode(&ret)

	if err == mongo.ErrNoDocuments {
		return nil, false
	}

	if err != nil {
		panic(err)
	}

	return ret.Value, true
}

//func (c *CacheDBer) DelSentStreamId(ctx context.Context) {
//
//}

func (c *CacheDBer) SaveSentStreamId(ctx context.Context, id x.StreamId) {
	_, err := c.coll().UpdateByID(ctx, streamId, cacheColl.ValueF().Set(id), options.UpdateOne().SetUpsert(true))
	if err != nil {
		panic(err)
	}
}

func unmarshal(raw bson.Raw) *db.Task {
	task := &db.Task{}
	err := bson.Unmarshal(raw, &task)
	if err != nil {
		panic(err)
	}

	return task
}

func marshal(task *db.Task) bson.Raw {
	ret, err := bson.Marshal(task)
	if err != nil {
		panic(err)
	}

	return ret
}

type taskIter struct {
	cursor *mongo.Cursor
}

func (ti *taskIter) First(ctx context.Context) (task db.Task, ok bool) {
	return ti.Next(ctx)
}

func (ti *taskIter) Next(ctx context.Context) (task db.Task, ok bool) {
	ok = ti.cursor.Next(ctx)
	if !ok {
		if ti.cursor.Err() != nil {
			panic(ti.cursor.Err())
		}
		return db.Task{}, false
	}

	var ret cache
	err := ti.cursor.Decode(&ret)
	if err != nil {
		panic(err)
	}

	return *unmarshal(ret.Value), true
}

func (ti *taskIter) Release() {
	_ = ti.cursor.Close(context.Background())
}

func (c *CacheDBer) All(ctx context.Context) db.SyncTaskIterator {
	cursor, err := c.coll().Find(ctx, cacheColl.IDF().Gte(""))

	if err != nil {
		panic(err)
	}

	return &taskIter{cursor: cursor}
}

// InsertOrUpdate task.Id 是唯一标识符，相同的id进行覆盖
func (c *CacheDBer) InsertOrUpdate(ctx context.Context, task db.Task) {
	_, err := c.coll().UpdateByID(ctx, taskId(&task), cacheColl.ValueF().Set(marshal(&task)),
		options.UpdateOne().SetUpsert(true))
	if err != nil {
		panic(err)
	}
}

func (c *CacheDBer) InsertOrUpdateBatch(ctx context.Context, tasks []db.Task) {

	models := make([]mongo.WriteModel, 0, len(tasks))

	for _, u := range tasks {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(cacheColl.IDF().Eq(taskId(&u))).
			SetUpdate(cacheColl.ValueF().Set(marshal(&u))).
			SetUpsert(true),
		)
	}

	_, err := c.coll().BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		panic(err)
	}
}

func (c *CacheDBer) Del(ctx context.Context, id string) {
	_, err := c.coll().DeleteOne(ctx, cacheColl.IDF().Eq(id))
	if err != nil {
		panic(err)
	}
}

func (c *CacheDBer) DelBatch(ctx context.Context, ids []string) {
	_, err := c.coll().DeleteMany(ctx, cacheColl.IDF().In(ids))
	if err != nil {
		panic(err)
	}
}

func (c *CacheDBer) DelAll(ctx context.Context) {
	_, err := c.coll().DeleteMany(ctx, cacheColl.IDF().Regex(bson.Regex{Pattern: fmt.Sprintf("^%s.*", taskPre)}))
	if err != nil {
		panic(err)
	}
}
