package bboltdb

import (
	"context"
	"testing"

	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
)

// 落盘位置：<repo>/bboltdb/db_failure_test.go（整份替换）
//
// 【为什么整份替换】
//  产品侧已把「本地 DB 事务失败」定性为服务不可运行异常，三个 DBer 接口都声明
//  「所有接口出现服务不可运行的异常，直接 panic」。原来这两个用例断言的是
//  「DB 关闭后不 panic、返回零值」，与新契约完全相反，必然 FAIL。
//  用关闭底层 DB 注入 Begin 失败，正好是验证 panic 契约最稳定的手段。
//
//  注意区分：迭代器接口是另一套契约 —— ChangeStreamIter.Next 的 View 失败
//  走 it.err → Err()，不 panic（见 panic_contract_test.go 的护栏用例）。
//  这不是漏改，是有意的：sender 依赖 iter.Err() 走 UnknownErr → 5s 重试。

// requirePanic 断言 fn 会 panic，并把 recover 到的值作为诊断信息。
func requirePanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: DB 事务失败应 panic，实际静默返回", name)
		}
	}()
	fn()
}

// ---------------------------------------------------------------------------
// SyncTask：All() 与 5 个写方法在底层 DB 关闭后都必须 panic。
//
// All() 原本走「迭代器带 err」的路径，现在改为 View 失败直接 panic
// （synctask.go 的 All 实现）；这是与 ChangeStreamIter 的关键差异。
// ---------------------------------------------------------------------------
func TestSyncTaskMethodsPanicOnClosedDB(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()
	st, err := NewSyncTask(ctx, d)
	if err != nil {
		t.Fatalf("NewSyncTask failed: %v", err)
	}

	// 先写一条，确认正常路径可用
	st.InsertOrUpdate(ctx, task("db1", "c1", []byte("v1")))
	if got := len(allTasks(t, st, ctx)); got != 1 {
		t.Fatalf("写入前 All() 条数 = %d, want 1", got)
	}

	if err := d.Underlying.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	requirePanic(t, "SyncTask.All", func() { _ = st.All(ctx) })
	requirePanic(t, "SyncTask.InsertOrUpdate", func() {
		st.InsertOrUpdate(ctx, task("db1", "c2", []byte("v2")))
	})
	requirePanic(t, "SyncTask.InsertOrUpdateBatch", func() {
		st.InsertOrUpdateBatch(ctx, []db.Task{
			task("db1", "c3", []byte("v3")),
			task("db1", "c4", []byte("v4")),
		})
	})
	requirePanic(t, "SyncTask.Del", func() { st.Del(ctx, "db1.c1") })
	requirePanic(t, "SyncTask.DelBatch", func() {
		st.DelBatch(ctx, []string{"db1.c1", "db1.c2"})
	})
	requirePanic(t, "SyncTask.Clear", func() { st.Clear(ctx) })
}

// ---------------------------------------------------------------------------
// WatchCollection：5 个写方法 + 4 个只读 View 在底层 DB 关闭后都必须 panic。
// 这是上一轮「LatestVersion/Get 读失败返回 0/nil 导致静默丢事件」的回归护栏。
// ---------------------------------------------------------------------------
func TestWatchCollectionMethodsPanicOnClosedDB(t *testing.T) {
	d := newTestDB(t)
	ctx := context.Background()
	wc, err := NewWatchCollection(ctx, d)
	if err != nil {
		t.Fatalf("NewWatchCollection failed: %v", err)
	}

	infos := []x.WatchInfo{{DB: "db1", Collection: "c1"}}
	old, now := wc.Save(ctx, infos, 1)
	if old != 0 || now != 1 {
		t.Fatalf("写入前 Save = (%d,%d), want (0,1)", old, now)
	}

	if err := d.Underlying.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	requirePanic(t, "WatchCollection.LatestVersion", func() { _ = wc.LatestVersion(ctx) })
	requirePanic(t, "WatchCollection.Save", func() { _, _ = wc.Save(ctx, infos, 2) })
	requirePanic(t, "WatchCollection.Get", func() { _ = wc.Get(ctx, 1) })
	requirePanic(t, "WatchCollection.DelLessThan", func() { wc.DelLessThan(ctx, 2) })
	requirePanic(t, "WatchCollection.MarkSyncing", func() { wc.MarkSyncing(ctx, 2) })
	requirePanic(t, "WatchCollection.NeedSyncing", func() { _, _ = wc.NeedSyncing(ctx) })
	requirePanic(t, "WatchCollection.ClearSyncingAndMarkSynced", func() {
		wc.ClearSyncingAndMarkSynced(ctx, 2)
	})
	requirePanic(t, "WatchCollection.LatestSynced", func() { _ = wc.LatestSynced(ctx) })
	requirePanic(t, "WatchCollection.Clear", func() { wc.Clear(ctx) })
}

// ---------------------------------------------------------------------------
// B11（护栏）：decodeTask 返回的 UntilDocId.Value 必须是独立副本。
// 与 panic 契约无关，原样保留；若退回 rawValue = data[offset:...] 会 FAIL。
// ---------------------------------------------------------------------------
func TestDecodeTaskReturnsIndependentCopy(t *testing.T) {
	src := task("db1", "c1", []byte("ORIGINAL"))
	buf := encodeTask(src)

	got, err := decodeTask(buf)
	if err != nil {
		t.Fatalf("decodeTask failed: %v", err)
	}

	valueStart := len(buf) - len(src.UntilDocId.Value)
	for i := valueStart; i < len(buf); i++ {
		buf[i] = 'X'
	}

	if string(got.UntilDocId.Value) != "ORIGINAL" {
		t.Fatalf("UntilDocId.Value 指向了输入 buffer: %q", got.UntilDocId.Value)
	}
	if got.WatchInfo.DB != "db1" || got.WatchInfo.Collection != "c1" {
		t.Fatalf("WatchInfo = %+v, want db1.c1", got.WatchInfo)
	}
}
