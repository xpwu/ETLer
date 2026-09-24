package db

import (
	"context"
	"github.com/xpwu/ETLer/x"
	"math"
)

const ConfigVersion uint64 = 0

// WatchCollectionDBer 所有接口都可能并发
type WatchCollectionDBer interface {
	LatestVersion(ctx context.Context) uint64
	// Save latestVersion >= version 什么也不改变
	Save(ctx context.Context, w []x.WatchInfo, version uint64) (oldVersion, nowVersion uint64)
	Get(ctx context.Context, version uint64) []x.WatchInfo
	DelLessThan(ctx context.Context, version uint64)

	// 以下四个接口主要是标记 WatchCollection 的任务化(根据 WatchCollection 的信息生成需要同步的具体任务)的执行情况
	// 如果仅是普通的 WatchCollection 版本更新，应该做增量同步(delta sync)，不可恢复或者不可继续之前的工作才全量同步(full sync)

	// MarkSyncing version = math.MaxUint64: full sync; else: delta sync
	// version 必须要大于 oldSyncingVersion 才能修改，否则不做出修改，full sync 是优先级最高的
	MarkSyncing(ctx context.Context, version uint64)
	// NeedSyncing version = math.MaxUint64: full sync; else: delta sync
	NeedSyncing(ctx context.Context) (version uint64, need bool)
	// ClearSyncingAndMarkSynced 清除所有的同步标记并标记最后同步完的 version
	ClearSyncingAndMarkSynced(ctx context.Context, version uint64)
	// LatestSynced 获取最后同步的版本
	LatestSynced(ctx context.Context) (version uint64)

	// Clear 清除所有存储的 watch collection 及 标记量
	Clear(ctx context.Context)
}

type WatchCollectionDB struct {
	WatchCollectionDBer
}

func (wc *WatchCollectionDB) MarkFullSyncing(ctx context.Context) {
	wc.MarkSyncing(ctx, math.MaxUint64)
}

func (wc *WatchCollectionDB) NeedFullSyncing(ctx context.Context) bool {
	v, need := wc.NeedSyncing(ctx)
	return need && v == math.MaxUint64
}

func (wc *WatchCollectionDB) IsFullSyncing(version uint64, need bool) bool {
	return need && version == math.MaxUint64
}

var watchCollection *WatchCollectionDB

func SetWatchCollection(dber WatchCollectionDBer) {
	watchCollection = &WatchCollectionDB{dber}
}

func WatchCollection() *WatchCollectionDB {
	return watchCollection
}
