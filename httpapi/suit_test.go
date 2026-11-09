package httpapi

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/etl/task"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-tinyserver/api"
)

// 编译期保证 suite 满足框架的 Suite 接口（嵌入的三个组件提供了 SetUp/TearDown/URIMapper）
var _ api.Suite = (*suite)(nil)

// ================= 共享替身 =================
// 本包所有 handler 都直接调 db.WatchCollection() / db.GetBackupWorker()，
// 这两组都是包级全局单例，所以用例不能 t.Parallel()，
// 且每个用例开头都要重新 installXxxStub(t)。

type saveCall struct {
	version uint64
	infos   []x.WatchInfo
}

type stubWatchCollectionDB struct {
	mu sync.Mutex

	latestVersion uint64
	versions      map[uint64][]x.WatchInfo

	oldVersion uint64
	nowVersion uint64
	saveCalls  []saveCall

	getCalls   []uint64
	clearCalls int
	gotCtxs    []context.Context
}

func newStubWatchCollectionDB() *stubWatchCollectionDB {
	return &stubWatchCollectionDB{versions: make(map[uint64][]x.WatchInfo)}
}

// recordCtx 只在持锁的方法内调用
func (m *stubWatchCollectionDB) recordCtx(ctx context.Context) {
	m.gotCtxs = append(m.gotCtxs, ctx)
}

func (m *stubWatchCollectionDB) LatestVersion(ctx context.Context) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordCtx(ctx)
	return m.latestVersion
}

func (m *stubWatchCollectionDB) Save(ctx context.Context, w []x.WatchInfo, version uint64) (uint64, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordCtx(ctx)
	m.saveCalls = append(m.saveCalls, saveCall{
		version: version,
		infos:   append([]x.WatchInfo(nil), w...),
	})
	return m.oldVersion, m.nowVersion
}

func (m *stubWatchCollectionDB) Get(ctx context.Context, version uint64) []x.WatchInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordCtx(ctx)
	m.getCalls = append(m.getCalls, version)
	return append([]x.WatchInfo(nil), m.versions[version]...)
}

func (m *stubWatchCollectionDB) DelLessThan(ctx context.Context, version uint64) {}

func (m *stubWatchCollectionDB) MarkSyncing(ctx context.Context, version uint64) {}

func (m *stubWatchCollectionDB) NeedSyncing(ctx context.Context) (uint64, bool) { return 0, false }

func (m *stubWatchCollectionDB) ClearSyncingAndMarkSynced(ctx context.Context, version uint64) {}

func (m *stubWatchCollectionDB) LatestSynced(ctx context.Context) uint64 { return 0 }

func (m *stubWatchCollectionDB) Clear(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordCtx(ctx)
	m.clearCalls++
}

func (m *stubWatchCollectionDB) SaveCalls() []saveCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]saveCall(nil), m.saveCalls...)
}

func (m *stubWatchCollectionDB) GetCalls() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]uint64(nil), m.getCalls...)
}

func (m *stubWatchCollectionDB) ClearCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clearCalls
}

func (m *stubWatchCollectionDB) GotCtxs() []context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]context.Context(nil), m.gotCtxs...)
}

func installWatchCollectionStub(t *testing.T) *stubWatchCollectionDB {
	t.Helper()

	stub := newStubWatchCollectionDB()
	db.SetWatchCollection(stub)
	t.Cleanup(func() { db.SetWatchCollection(newStubWatchCollectionDB()) })
	return stub
}

type stubBackupWorker struct {
	mu      sync.Mutex
	path    string
	err     error
	calls   int
	gotCtxs []context.Context
}

func (b *stubBackupWorker) Backup(ctx context.Context) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	b.gotCtxs = append(b.gotCtxs, ctx)
	return b.path, b.err
}

func (b *stubBackupWorker) Calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *stubBackupWorker) GotCtxs() []context.Context {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]context.Context(nil), b.gotCtxs...)
}

func installBackupWorkerStub(t *testing.T, path string, err error) *stubBackupWorker {
	t.Helper()

	stub := &stubBackupWorker{path: path, err: err}
	db.SetBackupWorker(stub)
	t.Cleanup(func() { db.SetBackupWorker(&stubBackupWorker{}) })
	return stub
}

func wi(dbName, coll string) x.WatchInfo {
	return x.WatchInfo{DB: dbName, Collection: coll}
}

// ================= Delta 接收辅助 =================
// task.SyncTaskUpdater() 返回 chan<- SyncTaskDelta（只发送），
// 而它底层的 updateSyncTaskChan 是【无缓冲】且未导出，本包无法直接起接收方；
// 没有接收方时 APIForceSyncColl 会永久阻塞。
// channel 的方向只是编译期约束，chan<- T 与 chan T 的内存表示相同（都是 *hchan），
// 所以测试里把它转回双向 chan 才能收到 Delta。
// 这是测试专用的 hack；若 task 包将来导出接收侧，应改用导出接口。
func startDeltaReceiver(t *testing.T) <-chan task.SyncTaskDelta {
	t.Helper()

	sendOnly := task.SyncTaskUpdater()
	bidir := *(*chan task.SyncTaskDelta)(unsafe.Pointer(&sendOnly))

	got := make(chan task.SyncTaskDelta, 1)
	go func() {
		select {
		case d := <-bidir:
			got <- d
		case <-time.After(2 * time.Second):
			// 不投递，由用例侧的 select 超时报错
		}
	}()
	return got
}

// ================= 框架契约 =================
// go-tinyserver 的 api.Add 用反射扫描 suite 上所有 API 前缀的方法，
// 并按【方法签名】取 requestType / responseType 注册路由。
// 签名写错不会编译失败，只会在 add() 里被 Warning 后跳过，接口就静默消失了。

var wantAPINames = []string{
	"APIBackupDB",
	"APIClearWC",
	"APIForceFullSync",
	"APIForceSyncColl",
	"APIGetWatchCols",
	"APISetWatchCols",
}

func apiMethodNames() []string {
	typ := reflect.TypeOf(&suite{})
	var names []string
	for i := 0; i < typ.NumMethod(); i++ {
		name := typ.Method(i).Name
		if strings.HasPrefix(name, "API") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func TestAPIMethodSet(t *testing.T) {
	got := apiMethodNames()
	want := append([]string(nil), wantAPINames...)
	sort.Strings(want)

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("API 方法集合 = %v, want %v", got, want)
	}
}

// 每个 API 方法必须满足：
//   - 3 个入参（receiver, context.Context, *req）
//   - 1 个返回值，且是指针
//
// 特别钉住 APIForceFullSync 的 request 必须是 *fullSyncReq：
// 框架按 mType.In(2) 取 requestType，若误写成 *clearReq，
// 虽然两个都是空结构体、运行时看起来一样，但注册进去的 requestType 就是错的。
func TestAPIMethodSignatures(t *testing.T) {
	typ := reflect.TypeOf(&suite{})
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()

	for _, name := range wantAPINames {
		m, ok := typ.MethodByName(name)
		if !ok {
			t.Errorf("缺少方法 %s", name)
			continue
		}

		mt := m.Type
		if mt.NumIn() != 3 {
			t.Errorf("%s: NumIn = %d, want 3 (receiver, ctx, *req)", name, mt.NumIn())
			continue
		}
		if mt.In(1) != ctxType {
			t.Errorf("%s: In(1) = %v, want context.Context", name, mt.In(1))
		}
		if mt.In(2).Kind() != reflect.Ptr {
			t.Errorf("%s: In(2) = %v, 应为指针类型", name, mt.In(2))
		}
		if mt.NumOut() != 1 {
			t.Errorf("%s: NumOut = %d, want 1", name, mt.NumOut())
		} else if mt.Out(0).Kind() != reflect.Ptr {
			t.Errorf("%s: Out(0) = %v, 应为指针类型", name, mt.Out(0))
		}

		if name == "APIForceFullSync" {
			want := reflect.TypeOf((*fullSyncReq)(nil))
			if mt.In(2) != want {
				t.Errorf("%s: request 类型 = %v, want %v", name, mt.In(2), want)
			}
		}
	}
}

// AddAPI() 会往 go-tinyserver 的全局注册表写入路由（重复注册可能 panic），
// 且会启动反射扫描，单测里不覆盖；注册结果的正确性由上面两个签名用例保证。
