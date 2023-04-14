package changestream

import (
	"context"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/etl/x"
	"github.com/xpwu/go-db-mongo/mongodb/mongocache"
	"github.com/xpwu/go-log/log"
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
}

func (csr *changeStreamRunner) processCs(cs *mongo.ChangeStream) error {
	ce := event{}
	err := cs.Decode(&ce)
	if err != nil {
		return err
	}

	cid := config.WatchInfo{
		DB:         ce.Ns.Db,
		Collection: ce.Ns.Coll,
	}.Id()

	// 只是保存监听的
	if csr.watchColl[cid] {
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
		// 目前没有文档说明，错误是resume不成功，还是其他错误，所以返回true，表示需要普通watch
		logger.Error(err)
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
