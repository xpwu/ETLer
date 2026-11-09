package etl

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
)

// ================= WatchCollectionDBer 替身 =================
// IsInWatchCollection 通过 db.WatchCollection() 读取监听集合。
// db.SetWatchCollection 是包级全局单例，所以这些用例不能 t.Parallel()，
// 且每个用例开头都要重新 installWatchCollectionStub(t)。

type wcCall struct {
	name    string
	version uint64
}

type stubWatchCollectionDB struct {
	mu            sync.Mutex
	calls         []wcCall
	versions      map[uint64][]x.WatchInfo
	latestVersion uint64
	latestSynced  uint64
	needSyncing   bool
	syncingVer    uint64
}

func newStubWatchCollectionDB() *stubWatchCollectionDB {
	return &stubWatchCollectionDB{versions: make(map[uint64][]x.WatchInfo)}
}

// record 只在持锁的方法内调用
func (m *stubWatchCollectionDB) record(name string, version uint64) {
	m.calls = append(m.calls, wcCall{name: name, version: version})
}

func (m *stubWatchCollectionDB) Calls() []wcCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]wcCall(nil), m.calls...)
}

func (m *stubWatchCollectionDB) LatestVersion(ctx context.Context) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("LatestVersion", 0)
	return m.latestVersion
}

func (m *stubWatchCollectionDB) Save(ctx context.Context, w []x.WatchInfo, version uint64) (uint64, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("Save", version)
	old := m.latestVersion
	m.versions[version] = append([]x.WatchInfo(nil), w...)
	if version > m.latestVersion {
		m.latestVersion = version
	}
	return old, m.latestVersion
}

func (m *stubWatchCollectionDB) Get(ctx context.Context, version uint64) []x.WatchInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("Get", version)
	return append([]x.WatchInfo(nil), m.versions[version]...)
}

func (m *stubWatchCollectionDB) DelLessThan(ctx context.Context, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("DelLessThan", version)
}

func (m *stubWatchCollectionDB) MarkSyncing(ctx context.Context, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("MarkSyncing", version)
	m.needSyncing = true
	m.syncingVer = version
}

func (m *stubWatchCollectionDB) NeedSyncing(ctx context.Context) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("NeedSyncing", 0)
	return m.syncingVer, m.needSyncing
}

func (m *stubWatchCollectionDB) ClearSyncingAndMarkSynced(ctx context.Context, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("ClearSyncingAndMarkSynced", version)
	m.needSyncing = false
	m.latestSynced = version
}

func (m *stubWatchCollectionDB) LatestSynced(ctx context.Context) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("LatestSynced", 0)
	return m.latestSynced
}

func (m *stubWatchCollectionDB) Clear(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("Clear", 0)
	m.versions = make(map[uint64][]x.WatchInfo)
	m.latestVersion = 0
	m.latestSynced = 0
	m.needSyncing = false
	m.syncingVer = 0
}

func installWatchCollectionStub(t *testing.T) *stubWatchCollectionDB {
	t.Helper()

	stub := newStubWatchCollectionDB()
	db.SetWatchCollection(stub)
	// 恢复成空替身，避免把 nil 接口留给同进程内的其他用例
	t.Cleanup(func() { db.SetWatchCollection(newStubWatchCollectionDB()) })
	return stub
}

func wi(dbName, coll string) x.WatchInfo {
	return x.WatchInfo{DB: dbName, Collection: coll}
}

// ================= IsInWatchCollection =================

func TestIsInWatchCollectionAllPresent(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 5
	stub.versions[5] = []x.WatchInfo{
		wi("db1", "A"), wi("db1", "B"), wi("db2", "C"),
	}

	cases := []struct {
		name     string
		checking []x.WatchInfo
	}{
		{"单个", []x.WatchInfo{wi("db1", "A")}},
		{"全部", []x.WatchInfo{wi("db1", "A"), wi("db1", "B"), wi("db2", "C")}},
		{"乱序", []x.WatchInfo{wi("db2", "C"), wi("db1", "A")}},
		{"checking 含重复项", []x.WatchInfo{wi("db1", "A"), wi("db1", "A")}},
		{"checking 为 nil", nil},
		{"checking 为空切片", []x.WatchInfo{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !IsInWatchCollection(context.Background(), c.checking) {
				t.Fatal("应返回 true")
			}
		})
	}
}

func TestIsInWatchCollectionMissing(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 5
	stub.versions[5] = []x.WatchInfo{wi("db1", "A"), wi("db1", "B")}

	cases := []struct {
		name     string
		checking []x.WatchInfo
	}{
		{"完全不在", []x.WatchInfo{wi("db9", "Z")}},
		{"第一个就不在", []x.WatchInfo{wi("db9", "Z"), wi("db1", "A")}},
		{"第二个才不在", []x.WatchInfo{wi("db1", "A"), wi("db9", "Z")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if IsInWatchCollection(context.Background(), c.checking) {
				t.Fatal("存在不在监听集合中的项，应返回 false")
			}
		})
	}
}

// 空 checking 恒为 true，即使监听集合本身也是空的
func TestIsInWatchCollectionEmptyCheckingIsAlwaysTrue(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = db.ConfigVersion
	stub.versions[db.ConfigVersion] = nil

	if !IsInWatchCollection(context.Background(), nil) {
		t.Fatal("checking 为 nil 应返回 true")
	}
	if !IsInWatchCollection(context.Background(), []x.WatchInfo{}) {
		t.Fatal("checking 为空切片应返回 true")
	}
}

func TestIsInWatchCollectionEmptyWatchList(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 5
	stub.versions[5] = nil

	if IsInWatchCollection(context.Background(), []x.WatchInfo{wi("db1", "A")}) {
		t.Fatal("监听集合为空时，任何 checking 都不应命中")
	}
}

// 比较用的 key 是 WatchInfo.Id()，即 "DB.Collection"。
// 只比 DB 或只比 Collection 都会误判。
func TestIsInWatchCollectionKeyIsDBDotCollection(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 3
	stub.versions[3] = []x.WatchInfo{wi("db1", "c1")}

	cases := []struct {
		name     string
		checking x.WatchInfo
		want     bool
	}{
		{"完全一致", wi("db1", "c1"), true},
		{"同 collection 不同 db", wi("db2", "c1"), false},
		{"同 db 不同 collection", wi("db1", "c2"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := IsInWatchCollection(context.Background(), []x.WatchInfo{c.checking})
			if got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestIsInWatchCollectionExactMatch(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 3
	stub.versions[3] = []x.WatchInfo{wi("db1", "A")}

	cases := []struct {
		name     string
		checking x.WatchInfo
		want     bool
	}{
		{"完全一致", wi("db1", "A"), true},
		{"collection 大小写敏感", wi("db1", "a"), false},
		{"collection 前缀不算命中", wi("db1", "AB"), false},
		{"collection 为空不算命中", wi("db1", ""), false},
		{"db 大小写敏感", wi("DB1", "A"), false},
		{"db 前缀不算命中", wi("db", "A"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := IsInWatchCollection(context.Background(), []x.WatchInfo{c.checking})
			if got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

// 空 DB + 空 Collection 的 Id() 是 "."，也是一个合法 key
func TestIsInWatchCollectionEmptyFields(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = db.ConfigVersion
	stub.versions[db.ConfigVersion] = []x.WatchInfo{wi("", "")}

	if !IsInWatchCollection(context.Background(), []x.WatchInfo{wi("", "")}) {
		t.Fatal(`Id() == "." 应能命中`)
	}
	if IsInWatchCollection(context.Background(), []x.WatchInfo{wi("", "c")}) {
		t.Fatal(`Id() == ".c" 不应命中`)
	}
}

func TestIsInWatchCollectionDuplicatesInWatchList(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 3
	stub.versions[3] = []x.WatchInfo{
		wi("db1", "A"), wi("db1", "A"), wi("db1", "B"),
	}

	if !IsInWatchCollection(context.Background(), []x.WatchInfo{wi("db1", "A"), wi("db1", "B")}) {
		t.Fatal("监听集合中的重复项不应影响判断")
	}
}

// 必须读 LatestVersion 对应的那一份，不能读旧版本，也不能读固定版本
func TestIsInWatchCollectionUsesLatestVersion(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.versions[2] = []x.WatchInfo{wi("db1", "OLD")}
	stub.versions[3] = []x.WatchInfo{wi("db1", "A")}
	stub.latestVersion = 3
	ctx := context.Background()

	if !IsInWatchCollection(ctx, []x.WatchInfo{wi("db1", "A")}) {
		t.Fatal("v3 中的 A 应命中")
	}
	if IsInWatchCollection(ctx, []x.WatchInfo{wi("db1", "OLD")}) {
		t.Fatal("不应读到旧版本 v2 的数据")
	}

	stub.latestVersion = 2

	if IsInWatchCollection(ctx, []x.WatchInfo{wi("db1", "A")}) {
		t.Fatal("切到 v2 后 A 不应命中")
	}
	if !IsInWatchCollection(ctx, []x.WatchInfo{wi("db1", "OLD")}) {
		t.Fatal("切到 v2 后 OLD 应命中")
	}
}

// Get 的参数来自 LatestVersion 的返回值，且求值顺序不能颠倒
func TestIsInWatchCollectionCallOrder(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 7
	stub.versions[7] = []x.WatchInfo{wi("db1", "A")}

	IsInWatchCollection(context.Background(), []x.WatchInfo{wi("db1", "A")})

	calls := stub.Calls()
	if len(calls) != 2 {
		t.Fatalf("调用次数 = %d, want 2: %+v", len(calls), calls)
	}
	if calls[0].name != "LatestVersion" {
		t.Errorf("calls[0] = %s, want LatestVersion", calls[0].name)
	}
	if calls[1].name != "Get" {
		t.Errorf("calls[1] = %s, want Get", calls[1].name)
	}
	if calls[1].version != 7 {
		t.Errorf("Get 的 version = %d, want 7", calls[1].version)
	}
}

// 当前实现不检查 ctx，只按数据判断；此处固化现状，
// 若将来加入 ctx 短路，本用例会失败并提醒同步更新调用方
func TestIsInWatchCollectionIgnoresCanceledContext(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 1
	stub.versions[1] = []x.WatchInfo{wi("db1", "A")}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if !IsInWatchCollection(ctx, []x.WatchInfo{wi("db1", "A")}) {
		t.Fatal("已取消的 ctx 不应改变数据判断结果")
	}
	if IsInWatchCollection(ctx, []x.WatchInfo{wi("db9", "Z")}) {
		t.Fatal("已取消的 ctx 不应让不存在的项变成命中")
	}
}

// ================= WatchCollectionUpdated =================

// 该函数分别向 task / changestream 两侧的 buffered chan(1) 发信号。
// 那两个 channel 都是包内未导出的，本包无法直接观察投递结果，
// 这里验证「重复调用不阻塞」这一语义（select + default）。
func TestWatchCollectionUpdatedNonBlocking(t *testing.T) {
	done := make(chan struct{})
	go func() {
		WatchCollectionUpdated()
		WatchCollectionUpdated()
		WatchCollectionUpdated()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WatchCollectionUpdated 阻塞了（信号通道缓冲为 1，重复调用应走 default）")
	}
}

func TestWatchCollectionUpdatedConcurrent(t *testing.T) {
	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			WatchCollectionUpdated()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("并发调用 WatchCollectionUpdated 出现阻塞")
	}
}

// 说明：
//   - Start() 会进入 InitTaskFromConfig 的 5s 重试循环，并启动
//     changestream.StartWatching() / task.Start() 两个常驻 goroutine，
//     没有注入点且会污染测试进程，单测不覆盖。
//   - WatchCollectionUpdated() 两侧信号的实际投递结果由
//     etl/task 与 etl/changestream 各自包内的通道用例覆盖。
