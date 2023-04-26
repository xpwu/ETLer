package changestream

import (
	"context"
	"fmt"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/etl/x"
	"github.com/xpwu/go-db-mongo/mongodb/mongocache"
	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Start() {
	x.AutoRestart(context.TODO(), "change-stream", startAndBlock)
}

type Listener interface {
	ForceSync()
	StreamChanged()
}

var listener Listener

func SetListener(l Listener) {
	listener = l
}

func startAndBlock(ctx context.Context) {
	ctx, logger := log.WithCtx(ctx)

	client, err := mongocache.Get(ctx, config.Etl.Deployment)
	if err != nil {
		panic(err)
	}

	csr := newCsr(ctx, client)

	rt, ok := db.Cache().ResumeToken(ctx)
	if ok {
		logger.Info("read resume token = " + rt.String() + ".")
		needWatch := csr.resumeWatch(rt)
		if !needWatch {
			panic("resume watch error")
		}
		logger.Error("resume watch failed, will try no-resume watch")
	}

	csr.noResumeWatch()
}

type changeStreamRunner struct {
	ctx       context.Context
	client    *mongocache.Client
	watchColl map[string]bool
}

func newCsr(ctx context.Context, client *mongocache.Client) *changeStreamRunner {
	r := &changeStreamRunner{
		ctx:       ctx,
		client:    client,
		watchColl: make(map[string]bool),
	}

	// 配置的与之前存储的都加入监听，宁多不少，多了后面的处理服务器也会自动处理
	// 发送历史的change stream 本也就有之前缓存的 stream
	for _, c := range config.Etl.WatchCollections {
		r.watchColl[c.Id()] = true
	}
	for _, c := range db.WatchCollection().All(ctx) {
		r.watchColl[c.Id()] = true
	}

	return r
}

type event struct {
	Ns struct {
		Db   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"ns"`
	Id            bson.Raw `bson:"_id"`
	OperationType string   `bson:"operationType"`
	DocumentKey   bson.Raw `bson:"documentKey"`
}

func (e *event) String() string {
	return fmt.Sprintf("%s.%s %s at _id:%s, with resumetoken: %s",
		e.Ns.Db, e.Ns.Coll, e.OperationType, e.DocumentKey, e.Id)
}

func (csr *changeStreamRunner) processCs(cs *mongo.ChangeStream) error {
	_, logger := log.WithCtx(csr.ctx)

	ce := event{}
	err := cs.Decode(&ce)
	if err != nil {
		return err
	}

	cid := config.WatchInfo{
		DB:         ce.Ns.Db,
		Collection: ce.Ns.Coll,
	}.Id()

	logger.Debug("watched: ", ce.String())
	// 只是保存监听的
	if csr.watchColl[cid] {
		logger.Info("save change stream: ", ce.String())
		db.Stream().Save(csr.ctx, cs.ResumeToken(), cs.Current)
	}

	db.Cache().SaveResumeToken(csr.ctx, cs.ResumeToken())

	listener.StreamChanged()

	return nil
}

func (csr *changeStreamRunner) resumeWatch(token db.ResumeToken) (needWatch bool) {
	ctx, logger := log.WithCtx(csr.ctx)
	logger.PushPrefix("resume token.")

	cs, err := csr.client.Watch(ctx, mongo.Pipeline{},
		options.ChangeStream().SetFullDocument(options.UpdateLookup).SetResumeAfter(token))

	if err != nil {
		logger.Error(err)

		// 目前没有文档说明，错误是resume不成功，还是其他错误，所以默认返回true，表示需要普通watch
		// 只能尽可能的判断，因为进入no-resume流程时，会触发sync，但如果是网络错误，其实不应该触发
		if mongo.IsTimeout(err) {
			logger.Error("timeout err, not try no-resume watch ", err)
			return false
		}
		if mongo.IsNetworkError(err) {
			logger.Error("network err, not try no-resume watch ", err)
			return false
		}

		return true
	}

	first := true
	for cs.Next(ctx) {
		first = false

		err = csr.processCs(cs)
		if err != nil {
			break
		}
	}

	if err == nil {
		err = cs.Err()
	}
	logger.Error(err)

	// 目前没有文档说明，错误是resume不成功，还是其他错误，所以如果是第一次Next()，就返回true，表示需要普通watch
	// 不是第一次就说明肯定不是resumeToken找不到引起的错误，就返回false，
	return first
}

func (csr *changeStreamRunner) noResumeWatch() {
	ctx, logger := log.WithCtx(csr.ctx)
	logger.PushPrefix("no-resume watch.")

	cs, err := csr.client.Watch(ctx, mongo.Pipeline{},
		options.ChangeStream().SetFullDocument(options.UpdateLookup))

	if err != nil {
		logger.Error(err)
		panic(err)
	}

	listener.ForceSync()

	db.Cache().SaveResumeToken(csr.ctx, cs.ResumeToken())

	for cs.Next(ctx) {
		err = csr.processCs(cs)
		if err != nil {
			break
		}
	}

	if err == nil {
		err = cs.Err()
	}
	logger.Error(err)
	panic(err)
}
