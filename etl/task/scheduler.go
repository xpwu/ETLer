package task

import (
	"context"
	"github.com/xpwu/ETLer/etl/changestream"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-log/log"
	"github.com/xpwu/go-mongodb/client"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"time"
)

const (
	batch int = 1000
	retry     = 15 * time.Second
)

type SyncTaskDelta struct {
	Add []x.WatchInfo
	Del []x.WatchInfo
}

var (
	updateSyncTaskChan     = make(chan SyncTaskDelta)
	watchCollectionUpdated = make(chan struct{}, 1)
	forceFullSync          = make(chan struct{}, 1)
)

func SyncTaskUpdater() chan<- SyncTaskDelta {
	return updateSyncTaskChan
}

func PostForceFullSync() {
	select {
	case forceFullSync <- struct{}{}:
	default:
	}
}

func WatchCollectionUpdated() {
	select {
	case watchCollectionUpdated <- struct{}{}:
	default:
	}
}

func Start() {
	x.AutoRestartPanic(startAndBlock, x.WithName("scheduler"))
}

func startAndBlock(ctx context.Context) error {
	select {
	case <-watchCollectionUpdated:
	default:
	}
	select {
	case <-updateSyncTaskChan:
	default:
	}

	ctx, logger := log.WithCtx(ctx)
	var mongoClient *mongo.Client

	for {
		var err error
		mongoClient, err = client.GetFromCache(config.Watch.Deployment.CacheId().WithSuffix("watch"))
		if err == nil {
			break
		}

		logger.Error(err)
		time.Sleep(15 * time.Second)
	}

	backFillSyncTaskify(ctx)

	sender := NewSender(ctx, mongoClient, batch)

	sender.Start()

	again := false
	// 必须先停止 Sender，才能更新 同步任务。防止任务更新后被意外改写
	for {
		select {
		case code := <-sender.Done():
			switch code {
			case NeedForceSync:
				fullSyncTaskify(ctx)
				sender.Start()
			case SendFailed:
				time.Sleep(retry)
				sender.Start()
			case Ok:
				if again {
					again = sender.Start()
				}
			case UnknownErr:
				logger.Error("unknown err, restart after 5s")
				time.Sleep(5 * time.Second)
			}
		case ack := <-changestream.NeedForceSync():
			sender.Stop()
			fullSyncTaskify(ctx)
			ack <- struct{}{}
			sender.Start()
		case <-changestream.OnStreamChanged():
			again = sender.Start()
		case delta := <-updateSyncTaskChan:
			sender.Stop()
			updateSyncTask(ctx, delta)
			sender.Start()
		case <-watchCollectionUpdated:
			sender.Stop()
			deltaSyncTaskify(ctx)
			sender.Start()
		case <-forceFullSync:
			sender.Stop()
			fullSyncTaskify(ctx)
			sender.Start()
		}
	}
}

func MinKeyTask(info x.WatchInfo) db.Task {
	return db.Task{
		UntilDocId: bson.RawValue{Type: bson.TypeMinKey},
		WatchInfo:  info,
	}
}

func diffSyncTask(new []x.WatchInfo, old []x.WatchInfo) SyncTaskDelta {

	newM := make(map[string]x.WatchInfo)
	for _, info := range new {
		newM[info.Id()] = info
	}

	oldM := make(map[string]x.WatchInfo)
	for _, info := range old {
		oldM[info.Id()] = info
	}

	add := make([]x.WatchInfo, 0, len(newM))
	for id, info := range newM {
		_, has := oldM[id]
		if !has {
			add = append(add, info)
		}
	}

	del := make([]x.WatchInfo, 0, len(old))
	for id, info := range oldM {
		_, has := newM[id]
		if !has {
			del = append(del, info)
		}
	}

	return SyncTaskDelta{
		Add: add,
		Del: del,
	}
}

func updateSyncTask(ctx context.Context, delta SyncTaskDelta) {
	add := make([]db.Task, 0, len(delta.Add))
	for _, info := range delta.Add {
		add = append(add, MinKeyTask(info))
	}

	del := make([]string, 0, len(delta.Del))
	for _, d := range delta.Del {
		del = append(del, d.Id())
	}

	db.SyncTask().InsertOrUpdateBatch(ctx, add)
	db.SyncTask().DelBatch(ctx, del)
}

func backFillSyncTaskify(ctx context.Context) {
	if db.WatchCollection().NeedFullSyncing(ctx) {
		fullSyncTaskify(ctx)
		return
	}

	if db.WatchCollection().LatestVersion(ctx) != db.WatchCollection().LatestSynced(ctx) {
		deltaSyncTaskify(ctx)
	}
}

func fullSyncTaskify(ctx context.Context) {
	db.WatchCollection().MarkFullSyncing(ctx)
	version := db.WatchCollection().LatestVersion(ctx)
	all := db.WatchCollection().Get(ctx, version)
	add := make([]db.Task, 0, len(all))
	for _, info := range all {
		add = append(add, MinKeyTask(info))
	}

	db.SyncTask().DelAll(ctx)
	db.SyncTask().InsertOrUpdateBatch(ctx, add)

	// 因为全同步，这之前的 change stream 都可以标记为已发送
	db.ChangeStream().MarkSentUpTo(ctx, db.ChangeStream().LastStreamId())

	db.WatchCollection().ClearSyncingAndMarkSynced(ctx, version)
}

func deltaSyncTaskify(ctx context.Context) {
	ctx, logger := log.WithCtx(ctx)

	latestSynced := db.WatchCollection().LatestSynced(ctx)
	latestVer := db.WatchCollection().LatestVersion(ctx)
	if latestSynced == latestVer {
		return
	}

	// 必须先把 syncing 的任务化完，再任务化 latest。如果直接任务化 latest, 那么之前未完成版本而添加的 task 可以会多余。
	// v1 = {A, B, C}  v2 = {A, D}   v3 = {A, B, C, E}
	// v2 如果没有做完而直接做 v3，那么v2可能添加的 D 将无法从任务中删除，因为 v3 - v1 = {add:[E], del:[]}
	syncing, need := db.WatchCollection().NeedDeltaSyncing(ctx)
	if need && syncing < latestSynced {
		// error
		logger.Error("syncing(", syncing, ") < latestSynced(", latestSynced, ")")
		fullSyncTaskify(ctx)
		return
	}

	latestSyncedWc := db.WatchCollection().Get(ctx, latestSynced)
	if need {
		syncingWc := db.WatchCollection().Get(ctx, syncing)
		updateSyncTask(ctx, diffSyncTask(syncingWc, latestSyncedWc))
		db.WatchCollection().ClearSyncingAndMarkSynced(ctx, syncing)
		latestSynced = syncing
		latestSyncedWc = syncingWc
	}

	if latestSynced == latestVer {
		return
	}

	db.WatchCollection().MarkDeltaSyncing(ctx, latestVer)
	latest := db.WatchCollection().Get(ctx, latestVer)
	updateSyncTask(ctx, diffSyncTask(latest, latestSyncedWc))
	db.WatchCollection().ClearSyncingAndMarkSynced(ctx, latestVer)
	db.WatchCollection().DelLessThan(ctx, latestVer)
}

func InitTaskFromConfig(ctx context.Context) (succeed bool) {
	succeed = true
	ctx, logger := log.WithCtx(ctx)
	defer func() {
		if r := recover(); r != nil {
			logger.Error(r)
			succeed = false
		}
	}()

	oldVersion := db.WatchCollection().LatestVersion(ctx)
	if oldVersion != db.ConfigVersion {
		return true
	}

	old := db.WatchCollection().Get(ctx, oldVersion)
	updateSyncTask(ctx, diffSyncTask(config.Watch.Collections, old))

	// 必须最后保存此项，防止前面异常出错
	db.WatchCollection().Save(ctx, config.Watch.Collections, db.ConfigVersion)
	db.WatchCollection().ClearSyncingAndMarkSynced(ctx, db.ConfigVersion)

	return true
}
