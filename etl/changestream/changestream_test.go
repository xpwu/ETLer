package changestream

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ================= WatchCollection 替身 =================
// newCsr 通过 db.WatchCollection() 读取监听集合，db.SetXxx 是包级全局单例，
// 所以这些用例不能 t.Parallel()，且每个用例开头都要重新 installWatchCollectionStub(t)。

type stubWatchCollectionDB struct {
	mu            sync.Mutex
	calls         []string
	versions      map[uint64][]x.WatchInfo
	latestVersion uint64
	latestSynced  uint64
	needSyncing   bool
	syncingVer    uint64
}

func newStubWatchCollectionDB() *stubWatchCollectionDB {
	return &stubWatchCollectionDB{versions: make(map[uint64][]x.WatchInfo)}
}

func (m *stubWatchCollectionDB) record(name string) {
	m.calls = append(m.calls, name)
}

func (m *stubWatchCollectionDB) Calls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.calls...)
}

func (m *stubWatchCollectionDB) LatestVersion(ctx context.Context) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("LatestVersion")
	return m.latestVersion
}

func (m *stubWatchCollectionDB) Save(ctx context.Context, w []x.WatchInfo, version uint64) (uint64, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("Save")
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
	m.record("Get")
	return append([]x.WatchInfo(nil), m.versions[version]...)
}

func (m *stubWatchCollectionDB) DelLessThan(ctx context.Context, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("DelLessThan")
}

func (m *stubWatchCollectionDB) MarkSyncing(ctx context.Context, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("MarkSyncing")
	m.needSyncing = true
	m.syncingVer = version
}

func (m *stubWatchCollectionDB) NeedSyncing(ctx context.Context) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("NeedSyncing")
	return m.syncingVer, m.needSyncing
}

func (m *stubWatchCollectionDB) ClearSyncingAndMarkSynced(ctx context.Context, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("ClearSyncingAndMarkSynced")
	m.needSyncing = false
	m.latestSynced = version
}

func (m *stubWatchCollectionDB) LatestSynced(ctx context.Context) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("LatestSynced")
	return m.latestSynced
}

func (m *stubWatchCollectionDB) Clear(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("Clear")
	m.versions = make(map[uint64][]x.WatchInfo)
	m.latestVersion = 0
	m.latestSynced = 0
	m.needSyncing = false
	m.syncingVer = 0
}

func installWatchCollectionStub(t *testing.T) *stubWatchCollectionDB {
	t.Helper()

	wc := newStubWatchCollectionDB()
	db.SetWatchCollection(wc)
	// 留一个空替身，避免后续用例拿到 nil 接口
	t.Cleanup(func() { db.SetWatchCollection(newStubWatchCollectionDB()) })
	return wc
}

func drainSignals() {
	for {
		select {
		case <-newStreamChan:
		case <-watchCollectionUpdated:
		default:
			return
		}
	}
}

func infos(names ...string) []x.WatchInfo {
	out := make([]x.WatchInfo, 0, len(names))
	for _, n := range names {
		out = append(out, x.WatchInfo{DB: "db1", Collection: n})
	}
	return out
}

// ================= 通道/入口 =================

func TestExportedChannels(t *testing.T) {
	if NeedForceSync() != (<-chan SyncAckChan)(syncChan) {
		t.Error("NeedForceSync() 应返回 syncChan")
	}
	if OnStreamChanged() != (<-chan struct{})(newStreamChan) {
		t.Error("OnStreamChanged() 应返回 newStreamChan")
	}
}

// 模拟 initWatching 与 scheduler 之间的握手：
// watcher 把只发送通道交给 scheduler，等 scheduler 回 ack 后才继续保存 resumeToken。
func TestSyncAckHandshake(t *testing.T) {
	ack := make(chan struct{}, 1)
	done := make(chan struct{})

	go func() {
		syncChan <- ack // 无缓冲，阻塞直到 NeedForceSync() 的接收方就绪
		<-ack
		close(ack)
		close(done)
	}()

	var got SyncAckChan
	select {
	case got = <-NeedForceSync():
	case <-time.After(time.Second):
		t.Fatal("syncChan 上没有 handshake 请求")
	}
	if got == nil {
		t.Fatal("SyncAckChan 不应为 nil")
	}

	got <- struct{}{} // scheduler 侧只能通过只发送通道回 ack

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watcher 未收到 ack")
	}

	if _, ok := <-ack; ok {
		t.Fatal("watcher 收 ack 后应 close(ack)")
	}
}

func TestPostStreamChangedNonBlocking(t *testing.T) {
	drainSignals()
	t.Cleanup(drainSignals)

	done := make(chan struct{})
	go func() {
		postStreamChanged()
		postStreamChanged() // 缓冲为 1，第二次必须走 default 而不是阻塞
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("postStreamChanged 阻塞了")
	}

	// 连续变更合并成一个信号
	if n := len(newStreamChan); n != 1 {
		t.Fatalf("newStreamChan 中信号数 = %d, want 1", n)
	}
	select {
	case <-OnStreamChanged():
	default:
		t.Fatal("OnStreamChanged() 应能读到信号")
	}
}

func TestWatchCollectionUpdatedNonBlocking(t *testing.T) {
	drainSignals()
	t.Cleanup(drainSignals)

	done := make(chan struct{})
	go func() {
		WatchCollectionUpdated()
		WatchCollectionUpdated()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WatchCollectionUpdated 阻塞了")
	}

	if n := len(watchCollectionUpdated); n != 1 {
		t.Fatalf("watchCollectionUpdated 中信号数 = %d, want 1", n)
	}
}

// 两种信号是独立通道，不能互相串台
func TestSignalChannelIndependence(t *testing.T) {
	drainSignals()
	t.Cleanup(drainSignals)

	WatchCollectionUpdated()

	if n := len(newStreamChan); n != 0 {
		t.Fatalf("watchCollectionUpdated 不应影响 newStreamChan, got %d", n)
	}
	select {
	case <-watchCollectionUpdated:
	default:
		t.Fatal("watchCollectionUpdated 应有信号")
	}

	postStreamChanged()

	if n := len(watchCollectionUpdated); n != 0 {
		t.Fatalf("postStreamChanged 不应影响 watchCollectionUpdated, got %d", n)
	}
	select {
	case <-newStreamChan:
	default:
		t.Fatal("newStreamChan 应有信号")
	}
}

// ================= newCsr =================

func TestNewCsrBuildsWatchColl(t *testing.T) {
	wc := installWatchCollectionStub(t)
	wc.versions[7] = infos("A", "B", "C")
	wc.latestVersion = 7

	r := newCsr(context.Background(), nil)

	if len(r.watchColl) != 3 {
		t.Fatalf("watchColl 数量 = %d, want 3: %+v", len(r.watchColl), r.watchColl)
	}
	for _, id := range []string{"db1.A", "db1.B", "db1.C"} {
		if !r.watchColl[id] {
			t.Errorf("watchColl 缺少 %s", id)
		}
	}
	if r.ctx == nil {
		t.Error("ctx 应被保存")
	}

	// 必须先取 LatestVersion，再用它去 Get
	calls := wc.Calls()
	if len(calls) != 2 || calls[0] != "LatestVersion" || calls[1] != "Get" {
		t.Fatalf("调用顺序 = %v, want [LatestVersion Get]", calls)
	}
}

func TestNewCsrEmptyWatchCollection(t *testing.T) {
	wc := installWatchCollectionStub(t)
	wc.latestVersion = 0
	wc.versions[0] = nil

	r := newCsr(context.Background(), nil)

	if r.watchColl == nil {
		t.Fatal("watchColl 应被初始化为非 nil 的 map")
	}
	if len(r.watchColl) != 0 {
		t.Fatalf("watchColl 应为空, got %+v", r.watchColl)
	}
}

// key 必须是 WatchInfo.Id()，即 "DB.Collection"，而不是单独的 Collection 名
func TestNewCsrKeyIsWatchInfoId(t *testing.T) {
	wc := installWatchCollectionStub(t)
	wc.latestVersion = 3
	wc.versions[3] = []x.WatchInfo{
		{DB: "db1", Collection: "c1"},
		{DB: "db2", Collection: "c1"}, // 同名 collection，不同 db
	}

	r := newCsr(context.Background(), nil)

	if len(r.watchColl) != 2 {
		t.Fatalf("watchColl 数量 = %d, want 2: %+v", len(r.watchColl), r.watchColl)
	}
	if !r.watchColl["db1.c1"] || !r.watchColl["db2.c1"] {
		t.Fatalf("key 应为 DB.Collection, got %+v", r.watchColl)
	}
}

func TestNewCsrDeduplicates(t *testing.T) {
	wc := installWatchCollectionStub(t)
	wc.latestVersion = 3
	wc.versions[3] = append(infos("A"), infos("A")...) // 重复项

	r := newCsr(context.Background(), nil)

	if len(r.watchColl) != 1 || !r.watchColl["db1.A"] {
		t.Fatalf("重复项应被 map 去重, got %+v", r.watchColl)
	}
}

// ================= event =================

// bson.Raw 实现了 fmt.Stringer，String() 会把内容当 BSON 文档解析。
// 非文档字节（短字节、nil、长度不足）解析失败返回空串 ——
// 这正是调试日志里 "resume token: , value: " 为空的原因。
// 此处把第三方库的这个行为固化下来：将来若换成 %q 打印，用例会失败并提醒同步更新。
func TestBsonRawStringOnNonDocument(t *testing.T) {
	cases := []struct {
		name string
		raw  bson.Raw
	}{
		{"nil", nil},
		{"空切片", bson.Raw{}},
		{"长度前缀为 0", bson.Raw([]byte{0, 0, 0, 0})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.raw.String(); got != "" {
				t.Errorf("String() = %q, want 空串", got)
			}
		})
	}
}

func TestEventString(t *testing.T) {
	e := &event{}
	e.Ns.Db = "db1"
	e.Ns.Coll = "c1"
	e.OperationType = "insert"
	e.Id = bson.Raw([]byte{0, 0, 0, 0})
	e.DocumentKey = bson.Raw([]byte{0, 0, 0, 0})

	got := e.String()
	want := "db1.c1 insert at _id:, with resumetoken: "
	if got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestEventStringWithValidBson(t *testing.T) {
	idDoc, err := bson.Marshal(bson.D{{"_id", "abc"}})
	if err != nil {
		t.Fatalf("Marshal(id) failed: %v", err)
	}
	keyDoc, err := bson.Marshal(bson.D{{"_id", 1}})
	if err != nil {
		t.Fatalf("Marshal(documentKey) failed: %v", err)
	}

	e := &event{}
	e.Ns.Db = "db2"
	e.Ns.Coll = "c2"
	e.OperationType = "invalidate"
	e.Id = bson.Raw(idDoc)
	e.DocumentKey = bson.Raw(keyDoc)

	got := e.String()
	if !strings.Contains(got, "db2.c2 invalidate") {
		t.Fatalf("String() = %q, 应包含 db/coll/operationType", got)
	}
	if strings.Contains(got, "at _id:, with") {
		t.Fatalf("合法的 BSON 文档不应被打印成空串: %q", got)
	}
	if !strings.Contains(got, "with resumetoken:") {
		t.Errorf("String() = %q, 应包含 resumetoken 段", got)
	}
}

// ================= processStream =================

// 零值 *mongo.ChangeStream 的 ResumeToken() 为 nil：
// 这是 processStream 的第一道防线，不依赖真实 mongo 游标即可覆盖。
func TestProcessStreamNilResumeToken(t *testing.T) {
	r := &changeStreamRunner{
		ctx:       context.Background(),
		watchColl: map[string]bool{"db1.c1": true},
	}

	got := r.processStream(&mongo.ChangeStream{})

	if got == nil {
		t.Fatal("resumeToken 为 nil 时应返回错误")
	}
	if got.Code != ErrRetryBackoff.Code {
		t.Errorf("Code = %d, want %d", got.Code, ErrRetryBackoff.Code)
	}
	if got.Reason != "processStream: resumeToken = nil" {
		t.Errorf("Reason = %q", got.Reason)
	}
	if !errors.Is(got, ErrRetryBackoff) {
		t.Fatal("应匹配 ErrRetryBackoff")
	}
}

// 说明：
//   - startAndBlock / watch / initWatching 依赖真实 *mongo.Client 与 *mongo.ChangeStream
//     （ChangeStream.Decode 要求内部 cursor 非 nil，而 changeStreamCursor 是驱动内部接口），
//     单测里无法无侵入地构造，故不覆盖；StartWatching 会启动常驻 goroutine，同样不覆盖。
//   - processStream 中"命中监听集合 → Save(value) + postStreamChanged"、
//     "非监听集合/invalidate → Save(nil)" 两条分支需要 Decode 成功，同样需要真实游标。
//     其错误分类由 error_test.go 覆盖，握手协议由 TestSyncAckHandshake 覆盖。
