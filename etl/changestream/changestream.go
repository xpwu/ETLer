package changestream

import (
	"context"
	"fmt"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-log/log"
	"github.com/xpwu/go-mongodb/client"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"time"
)

func StartWatching() {
	x.AutoRestartPanic(startAndBlock, x.WithName("watchChangeStream"))
}

type SyncAckChan = chan<- struct{}

var (
	syncChan               = make(chan SyncAckChan)
	newStreamChan          = make(chan struct{}, 1)
	watchCollectionUpdated = make(chan struct{}, 1)
)

func NeedForceSync() <-chan SyncAckChan {
	return syncChan
}

func OnStreamChanged() <-chan struct{} {
	return newStreamChan
}

func WatchCollectionUpdated() {
	select {
	case watchCollectionUpdated <- struct{}{}:
	default:
	}
}

func postStreamChanged() {
	select {
	case newStreamChan <- struct{}{}:
	default:
	}
}

func startAndBlock(ctx context.Context) error {
	select {
	case <-watchCollectionUpdated:
	default:
	}

	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("ChangeStreamServer")
	logger.Info("start")
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		select {
		case <-watchCollectionUpdated:
			// stop watching stream, and wait for restarting
			logger.Debug("WatchCollection is updated, canceling")
			cancel()
			logger.Debug("WatchCollection is updated, wait for restarting")
		}
	}()

	var mongoClient *mongo.Client

	for {
		c, err := client.GetFromCache(config.Watch.Deployment.CacheId().WithSuffix("watch"))
		if err == nil {
			mongoClient = c
			break
		}
		// 出现错误，很大概率是配置文件写错，需要重写配置文件，但为了防止有未考虑到的情况，15s 后重试一下
		logger.Error(fmt.Sprintf("mongo connect failed, you should reset the config: %v", err))
		time.Sleep(15 * time.Second)
	}

	for {
		var streamErr *StreamError

		logger.Info("get ResumeToken from local db")

		resumeToken := db.ChangeStream().ResumeToken(ctx)
		if resumeToken == nil {
			logger.Info("resumeToken == nil, will init watching")
			resumeToken, streamErr = initWatching(ctx, mongoClient)
		}

		if streamErr == nil {
			logger.Info("resumeToken != nil, will watch")
			streamErr = watch(ctx, mongoClient, resumeToken)
		}

		switch {
		case streamErr.Is(ErrTokenExpired):
			logger.Error("Resume Token Expired, delete all and retry")
			db.ChangeStream().DeleteAll(ctx)
			time.Sleep(1 * time.Second)
		case streamErr.Is(ErrRetryBackoff) || streamErr.Is(ErrStopForOps):
			time.Sleep(15 * time.Second)
		default: // ErrShuttingDown, ErrRetryNow - retry now
			time.Sleep(1 * time.Second)
		}
		logger.Info("stream err, retry server")
	}
}

func watch(ctx context.Context, client *mongo.Client, resumeToken bson.Raw) *StreamError {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("watch")

	csr := newCsr(ctx, client)

	// *** option 与 pipeline 不要修改， 否则可能有未知异常 ***
	cs, err := csr.client.Watch(ctx, mongo.Pipeline{},
		options.ChangeStream().SetFullDocument(options.UpdateLookup).SetStartAfter(resumeToken))

	if err != nil {
		logger.Error(err)
		return AsStreamError(err, false)
	}

	for cs.Next(ctx) {
		streamErr := csr.processStream(cs)
		if streamErr != nil {
			logger.Error(streamErr)
			return streamErr
		}
	}

	return AsStreamError(cs.Err(), false)
}

// initWatching resumeToken 最后保存到的那个 ResumeToken
func initWatching(ctx context.Context, client *mongo.Client) (resumeToken bson.Raw, stErr *StreamError) {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("init watching")
	logger.Info("start")
	cs, err := client.Watch(ctx, mongo.Pipeline{},
		options.ChangeStream().SetFullDocument(options.UpdateLookup).SetStartAtOperationTime(&bson.Timestamp{T: 1}))
	if err != nil {
		logger.Error(fmt.Sprintf("watch error. %s", err.Error()))
		return nil, AsStreamError(err, false)
	}

	// 返回最后获取到的那个 ResumeToken
	for cs.TryNext(ctx) {
		resumeToken = cs.ResumeToken()
	}
	if resumeToken == nil {
		logger.Error("watch error: resumeToken = nil")
		return nil, &StreamError{
			Code:   ErrRetryBackoff.Code,
			Reason: "init watching: resumeToken = nil",
			Err:    cs.Err(),
		}
	}

	syncAck := make(chan struct{}, 1)
	syncChan <- syncAck
	<-syncAck
	close(syncAck)
	// 必须等待 sync ack 才能保存 resumeToken, 否则可能出现 sync 丢失的情况
	id, err := db.ChangeStream().Save(ctx, resumeToken, nil)
	if err != nil {
		logger.Error(fmt.Sprintf("save error. %s", err.Error()))
		return nil, AsStreamError(err, false)
	}
	db.ChangeStream().MarkSentUpTo(ctx, id)

	logger.Info("succeed")
	return resumeToken, AsStreamError(cs.Err(), false)
}

type changeStreamRunner struct {
	ctx       context.Context
	client    *mongo.Client
	watchColl map[string]bool
}

func newCsr(ctx context.Context, client *mongo.Client) *changeStreamRunner {
	r := &changeStreamRunner{
		ctx:       ctx,
		client:    client,
		watchColl: make(map[string]bool),
	}

	all := db.WatchCollection().Get(ctx, db.WatchCollection().LatestVersion(ctx))
	for _, c := range all {
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

func (csr *changeStreamRunner) processStream(cs *mongo.ChangeStream) *StreamError {
	_, logger := log.WithCtx(csr.ctx)
	logger.PushPrefix("processStream")

	resumeToken := cs.ResumeToken()
	if resumeToken == nil {
		logger.Error("mongo.ChangeStream.ResumeToken() == nil")
		return &StreamError{
			Code:   ErrRetryBackoff.Code,
			Reason: "processStream: resumeToken = nil",
		}
	}

	ce := &event{}
	err := cs.Decode(ce)
	if err != nil {
		logger.Error(fmt.Sprintf("mongo.ChangeStream.Decode() err. %s", err.Error()))
		return AsStreamError(err, false)
	}

	cid := x.WatchInfo{
		DB:         ce.Ns.Db,
		Collection: ce.Ns.Coll,
	}.Id()

	logger.Debug("watched: ", ce.String())

	if csr.watchColl[cid] && ce.OperationType != "invalidate" {
		logger.Debug("save resumeToken and changeStream")
		_, err = db.ChangeStream().Save(csr.ctx, resumeToken, cs.Current)
		if err != nil {
			logger.Error(fmt.Sprintf("db.ChangeStream().Save(resumeToken, changeStream) err. %s", err.Error()))
			return AsStreamError(err, false)
		}
		logger.Info("saved resumeToken and changeStream: ", ce.String())
		postStreamChanged()
	} else {
		logger.Debug(ce.String(), "is NOT in the Watching Collections, will only save resumeToken")
		_, err = db.ChangeStream().Save(csr.ctx, resumeToken, nil)
		if err != nil {
			logger.Error(fmt.Sprintf("db.ChangeStream().Save(resumeToken, nil) err. %s", err.Error()))
			return AsStreamError(err, false)
		}
		logger.Info("only saved resumeToken. ", ce.String())
	}

	return AsStreamError(cs.Err(), ce.OperationType == "invalidate")
}
