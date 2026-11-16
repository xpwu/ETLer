package bboltdb

import (
	"context"
	"fmt"
	"go.etcd.io/bbolt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func newTestSyncTask(t *testing.T) *SyncTask {
	t.Helper()
	st, err := NewSyncTask(context.Background(), newTestDB(t))
	if err != nil {
		t.Fatalf("NewSyncTask failed: %v", err)
	}
	return st
}

func allTasks(t *testing.T, st *SyncTask, ctx context.Context) []db.Task {
	t.Helper()

	var tasks []db.Task
	it := st.All(ctx)
	defer it.Release(ctx)
	for it.Next(ctx) {
		tasks = append(tasks, it.Current())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterate err: %v", err)
	}
	return tasks
}

func task(dbName, coll string, value []byte) db.Task {
	return db.Task{
		WatchInfo:  x.WatchInfo{DB: dbName, Collection: coll},
		UntilDocId: bson.RawValue{Type: bson.TypeString, Value: value},
	}
}

func TestTaskEncodeDecodeRoundTrip(t *testing.T) {
	cases := []db.Task{
		{
			WatchInfo:  x.WatchInfo{DB: "db", Collection: "c"},
			UntilDocId: bson.RawValue{Type: bson.TypeMinKey},
		},
		{
			WatchInfo:  x.WatchInfo{DB: "db", Collection: "c"},
			UntilDocId: bson.RawValue{Type: bson.TypeString, Value: []byte("abc")},
		},
		{
			WatchInfo:  x.WatchInfo{DB: "", Collection: ""},
			UntilDocId: bson.RawValue{Type: bson.TypeMinKey},
		},
	}
	for _, want := range cases {
		got, err := decodeTask(encodeTask(want))
		if err != nil {
			t.Fatalf("decodeTask failed: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	}
}

func TestDecodeTaskRejectsCorrupted(t *testing.T) {
	cases := [][]byte{
		nil,
		{0, 0, 0, 1},             // dbLen 越界
		{0, 0, 0, 0, 0, 0, 0, 1}, // collection 长度前缀越界
		{0, 0, 0, 0, 0, 0, 0, 0}, // 缺 type byte
	}
	for _, c := range cases {
		if _, err := decodeTask(c); err == nil {
			t.Errorf("decodeTask(%v) should return error", c)
		}
	}
}

func TestSyncTaskEmpty(t *testing.T) {
	st := newTestSyncTask(t)
	if got := allTasks(t, st, context.Background()); len(got) != 0 {
		t.Fatalf("empty: got %d tasks", len(got))
	}
}

func TestSyncTaskInsertOrUpdateAndAll(t *testing.T) {
	st := newTestSyncTask(t)
	ctx := context.Background()

	t1 := task("db", "c1", nil)
	t2 := task("db", "c2", []byte("id-2"))
	st.InsertOrUpdate(ctx, t1)
	st.InsertOrUpdate(ctx, t2)

	got := allTasks(t, st, ctx)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}

	// 相同 Id 覆盖
	t1.UntilDocId = bson.RawValue{Type: bson.TypeString, Value: []byte("id-1")}
	st.InsertOrUpdate(ctx, t1)

	got = allTasks(t, st, ctx)
	if len(got) != 2 {
		t.Fatalf("覆盖后 len = %d, want 2", len(got))
	}
	for _, tk := range got {
		if tk.Id() != t1.Id() {
			continue
		}
		if string(tk.UntilDocId.Value) != "id-1" {
			t.Fatalf("未覆盖: UntilDocId = %q", tk.UntilDocId.Value)
		}
	}
}

func TestSyncTaskDelAndDelBatch(t *testing.T) {
	st := newTestSyncTask(t)
	ctx := context.Background()

	t1 := task("db", "c1", nil)
	t2 := task("db", "c2", nil)
	t3 := task("db", "c3", nil)
	st.InsertOrUpdateBatch(ctx, []db.Task{t1, t2, t3})

	if got := allTasks(t, st, ctx); len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}

	st.Del(ctx, t1.Id())
	if got := allTasks(t, st, ctx); len(got) != 2 {
		t.Fatalf("Del 后 len = %d, want 2", len(got))
	}

	st.DelBatch(ctx, []string{t2.Id(), t3.Id()})
	if got := allTasks(t, st, ctx); len(got) != 0 {
		t.Fatalf("DelBatch 后 len = %d, want 0", len(got))
	}
}

func TestSyncTaskClear(t *testing.T) {
	st := newTestSyncTask(t)
	ctx := context.Background()

	st.InsertOrUpdateBatch(ctx, []db.Task{
		task("db", "c1", nil),
		task("db", "c2", []byte("id")),
	})
	st.Clear(ctx)

	if got := allTasks(t, st, ctx); len(got) != 0 {
		t.Fatalf("Clear 后 len = %d, want 0", len(got))
	}

	// Clear 后仍可使用
	st.InsertOrUpdate(ctx, task("db", "c3", nil))
	if got := allTasks(t, st, ctx); len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
}

func TestSyncTaskIterAllowsWriteDuringIteration(t *testing.T) {
	dir := t.TempDir()
	underlying, err := bbolt.Open(filepath.Join(dir, "small.db"), 0600,
		&bbolt.Options{NoSync: true, NoFreelistSync: true}) // 故意不设 InitialMmapSize
	if err != nil {
		t.Fatalf("open bbolt failed: %v", err)
	}
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { _ = underlying.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			// 死锁场景放弃 Close，进程退出时由 OS 回收文件与映射
		}
	})

	st, err := NewSyncTask(context.Background(), &DB{Dir: dir, Underlying: underlying})
	if err != nil {
		t.Fatalf("NewSyncTask failed: %v", err)
	}

	ctx := context.Background()
	st.InsertOrUpdate(ctx, task("db1", "c1", nil))

	done := make(chan struct{})
	go func() {
		defer close(done)
		it := st.All(ctx)
		defer it.Release(ctx)
		for it.Next(ctx) {
			tk := it.Current()
			tk.UntilDocId.Value = []byte("progress")
			st.InsertOrUpdate(ctx, tk) // 协议允许：更新进度
			st.Del(ctx, tk.Id())       // 协议允许：删除已完成
			for i := 0; i < 500; i++ { // 制造新页，逼迫 grow mmap
				st.InsertOrUpdate(ctx, task("db1", fmt.Sprintf("c%d", i), []byte("x")))
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("迭代期间写操作死锁（读事务与 mmap 扩容互等）")
	}
}
