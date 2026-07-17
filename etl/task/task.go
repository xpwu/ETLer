package task

import (
	"context"
	"github.com/xpwu/ETLer/etl/changestream"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	cmdX "github.com/xpwu/go-cmd/x"
	"github.com/xpwu/go-db-mongo/mongodb/mongocache"
	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"time"
)

const (
	batch int = 1000
	retry     = 1 * time.Minute
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
	cmdX.AutoRestart(context.TODO(), "send-task", startAndBlock)
}

func startAndBlock(ctx context.Context) {
	select {
	case <-watchCollectionUpdated:
	default:
	}
	select {
	case <-updateSyncTaskChan:
	default:
	}

	ctx, logger := log.WithCtx(ctx)
	client, err := mongocache.Get(ctx, config.Etl.Deployment)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	backFillSyncTaskify(ctx)

	runner := NewRunner(ctx, client, batch)

	runner.Start()

	again := false
	// 必须先停止 Runner，才能更新 同步任务。防止任务更新后被意外改写
	for {
		select {
		case code := <-runner.Done():
			switch code {
			case NeedForceSync:
				fullSyncTaskify(ctx)
				runner.Start()
			case SendFailed:
				time.Sleep(retry)
				runner.Start()
			case Ok:
				if again {
					again = runner.Start()
				}
			case UnknownErr:
				panic("error, wait for restarting")
			}
		case <-changestream.NeedForceSync():
			runner.Stop()
			fullSyncTaskify(ctx)
			runner.Start()
		case <-changestream.OnStreamChanged():
			again = runner.Start()
		case delta := <-updateSyncTaskChan:
			runner.Stop()
			updateSyncTask(ctx, delta)
			runner.Start()
		case <-watchCollectionUpdated:
			runner.Stop()
			deltaSyncTaskify(ctx)
			runner.Start()
		case <-forceFullSync:
			runner.Stop()
			fullSyncTaskify(ctx)
			runner.Start()
		}
	}
}

func MinKeyTask(info x.WatchInfo) db.Task {
	return db.Task{
		StartDocId: serialize(bson.RawValue{Type: bsontype.MinKey}),
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

	db.WatchCollection().DeltaSyncing(ctx, latestVer)
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
	updateSyncTask(ctx, diffSyncTask(config.Etl.WatchCollections, old))

	// 必须最后保存此项，防止前面异常出错
	db.WatchCollection().Save(ctx, config.Etl.WatchCollections, db.ConfigVersion)
	db.WatchCollection().ClearSyncingAndMarkSynced(ctx, db.ConfigVersion)

	return true
}
