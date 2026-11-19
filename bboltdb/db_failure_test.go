package bboltdb

import (
	"context"
	"testing"

	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
)

// ---------------------------------------------------------------------------
// B10 / B12（护栏）：synctask 的 5 处 `err := s.db.Update(...)` 与 All() 的 View，
// 在底层 DB 已关闭（Begin 失败）时必须「不 panic、不阻塞、返回可用零值」。
//
// 对应 synctask.go:190(All)
// 这些写方法都是 void，失败只能内部吞掉；本用例确保至少不会把 panic 扩散到调用方，
// 且 All() 通过迭代器把错误暴露出来（而不是静默返回空列表）。
// ---------------------------------------------------------------------------
func TestSyncTaskWritesOnClosedDBDoNotPanic(t *testing.T) {
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

	// 全部写方法：不得 panic、不得阻塞
	st.InsertOrUpdate(ctx, task("db1", "c2", []byte("v2")))
	st.InsertOrUpdateBatch(ctx, []db.Task{
		task("db1", "c3", []byte("v3")),
		task("db1", "c4", []byte("v4")),
	})

	st.Del(ctx, "db1.c1")
	st.DelBatch(ctx, []string{"db1.c1", "db1.c2"})
	st.Clear(ctx)

	// All() 必须返回带 err 的迭代器，而不是假装空库
	it := st.All(ctx)
	if it.Next(ctx) {
		t.Fatal("底层 DB 已关闭，All() 不应返回任何 task")
	}
	if it.Err() == nil {
		t.Fatal("底层 DB 已关闭，All() 的迭代器应带 error")
	}
	it.Release(ctx)
}

// ---------------------------------------------------------------------------
// B10（护栏）：watchcollection 的 5 处 Update / 4 处 View 在 DB 关闭后同样不 panic，
// 且返回零值而不是随机内存值。
//
// 对应 watchcollection.go:147 / 165 / 204 / 220 / 247 / 275 / 292 / 304 / 320。
// ---------------------------------------------------------------------------
func TestWatchCollectionWritesOnClosedDBDoNotPanic(t *testing.T) {
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

	o, n := wc.Save(ctx, infos, 2)
	if o != 0 || n != 0 {
		t.Fatalf("DB 关闭后 Save = (%d,%d), want (0,0)", o, n)
	}
	if v := wc.LatestVersion(ctx); v != 0 {
		t.Fatalf("DB 关闭后 LatestVersion = %d, want 0", v)
	}
	if got := wc.Get(ctx, 1); len(got) != 0 {
		t.Fatalf("DB 关闭后 Get = %v, want 空", got)
	}
	if v := wc.LatestSynced(ctx); v != 0 {
		t.Fatalf("DB 关闭后 LatestSynced = %d, want 0", v)
	}
	if v, need := wc.NeedSyncing(ctx); v != 0 || need {
		t.Fatalf("DB 关闭后 NeedSyncing = (%d,%v), want (0,false)", v, need)
	}

	// void 方法不得 panic
	wc.DelLessThan(ctx, 2)
	wc.MarkSyncing(ctx, 2)
	wc.ClearSyncingAndMarkSynced(ctx, 2)
	wc.Clear(ctx)
}

// ---------------------------------------------------------------------------
// B11（护栏）：decodeTask 返回的 UntilDocId.Value 必须是独立副本。
// synctask.go:103-104 已改成 make+copy；若退回 rawValue = data[offset:...]，
// 源 buffer（bbolt mmap）被改写后 task 的值会跟着变 → 本用例 FAIL。
// ---------------------------------------------------------------------------
func TestDecodeTaskReturnsIndependentCopy(t *testing.T) {
	src := task("db1", "c1", []byte("ORIGINAL"))
	buf := encodeTask(src)

	got, err := decodeTask(buf)
	if err != nil {
		t.Fatalf("decodeTask failed: %v", err)
	}

	// 篡改末尾的 value 字节区（encodeTask 布局：4+db + 4+coll + 1(type) + 4(len) + value）
	valueStart := len(buf) - len(src.UntilDocId.Value)
	for i := valueStart; i < len(buf); i++ {
		buf[i] = 'X'
	}

	if string(got.UntilDocId.Value) != "ORIGINAL" {
		t.Fatalf("UntilDocId.Value 指向了输入 buffer: %q", got.UntilDocId.Value)
	}
	// 顺带确认 DB / Collection 的 string 转换同样是拷贝
	if got.WatchInfo.DB != "db1" || got.WatchInfo.Collection != "c1" {
		t.Fatalf("WatchInfo = %+v, want db1.c1", got.WatchInfo)
	}
}
