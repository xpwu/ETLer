package db

import (
	"context"
	"github.com/xpwu/ETLer/x"
)

const ConfigVersion uint64 = 0

// WCAccessor 需要支持并发
type WCAccessor interface {
	LatestVersion(ctx context.Context) uint64
	// Save latestVersion >= version 什么也不改变
	Save(ctx context.Context, w []x.WatchInfo, version uint64) (oldVersion, nowVersion uint64)
	Get(ctx context.Context, version uint64) []x.WatchInfo
	DelLessThan(ctx context.Context, version uint64)
	DelAll(ctx context.Context)
}

// WCTaskifier 无需支持并发，主要是标记 WatchCollection 的任务化(根据 WatchCollection 的信息生成需要同步的具体任务)的执行情况
// 如果仅是普通的 WatchCollection 版本更新，应该做增量更新
type WCTaskifier interface {
	// MarkFullSyncing 标记需要做全量同步
	MarkFullSyncing(ctx context.Context)
	// NeedFullSyncing 获取是否需要做全量同步
	NeedFullSyncing(ctx context.Context) bool

	// MarkDeltaSyncing 标记需要做 version 版本的增量同步
	MarkDeltaSyncing(ctx context.Context, version uint64)
	// NeedDeltaSyncing 获取是否需要做增量同步以及需要做增量同步的版本
	NeedDeltaSyncing(ctx context.Context) (version uint64, need bool)

	// ClearSyncingAndMarkSynced 清除所有的同步标记并标记最后同步完的 version
	ClearSyncingAndMarkSynced(ctx context.Context, version uint64)
	// LatestSynced 获取最后同步的版本
	LatestSynced(ctx context.Context) (version uint64)
}

type WatchCollectionDBer interface {
	WCAccessor
	WCTaskifier
}

var watchCollection WatchCollectionDBer

func SetWatchCollection(s WatchCollectionDBer) {
	watchCollection = s
}

func WatchCollection() WatchCollectionDBer {
	return watchCollection
}
