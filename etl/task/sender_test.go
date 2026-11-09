package task

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ================= 本包测试共用的替身 =================
// sender_test.go 与 scheduler_test.go 都会用到，故集中定义在此。
// 注意：db.SetXxx 是包级全局单例，所以这些用例绝不能 t.Parallel()，
// 且每个用例开头都要重新调用 installStubs(t)。

type sendCall struct {
	ty   Type
	db   string
	coll string
	data []bson.Raw
}

type stubNetLayer struct {
	mu     sync.Mutex
	calls  []sendCall
	err    error
	onSend func(ctx context.Context, c sendCall) error
}

func (n *stubNetLayer) Send(ctx context.Context, ty Type, dbName, coll string, data []bson.Raw) error {
	c := sendCall{ty: ty, db: dbName, coll: coll, data: append([]bson.Raw(nil), data...)}

	n.mu.Lock()
	n.calls = append(n.calls, c)
	onSend := n.onSend
	err := n.err
	n.mu.Unlock()

	if onSend != nil {
		return onSend(ctx, c)
	}
	return err
}

func (n *stubNetLayer) Calls() []sendCall {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]sendCall(nil), n.calls...)
}

// ---------- SyncTask ----------

type stubTaskIter struct {
	tasks    []db.Task
	i        int
	err      error
	released bool
}

func (it *stubTaskIter) Next(ctx context.Context) bool {
	if err := ctx.Err(); err != nil {
		it.err = err
		return false
	}
	if it.i >= len(it.tasks) {
		return false
	}
	it.i++
	return true
}

func (it *stubTaskIter) Current() db.Task {
	if it.i == 0 || it.i > len(it.tasks) {
		return db.Task{}
	}
	return it.tasks[it.i-1]
}

func (it *stubTaskIter) Err() error { return it.err }

func (it *stubTaskIter) Release(ctx context.Context) { it.released = true }

type syncTaskOp struct {
	kind  string
	tasks []db.Task
	ids   []string
}

type stubSyncTaskDB struct {
	mu        sync.Mutex
	ops       []syncTaskOp
	iterTasks []db.Task
}

func (m *stubSyncTaskDB) All(ctx context.Context) db.SyncTaskIterator {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &stubTaskIter{tasks: append([]db.Task(nil), m.iterTasks...)}
}

func (m *stubSyncTaskDB) InsertOrUpdate(ctx context.Context, t db.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, syncTaskOp{kind: "InsertOrUpdate", tasks: []db.Task{t}})
}

func (m *stubSyncTaskDB) InsertOrUpdateBatch(ctx context.Context, tasks []db.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, syncTaskOp{kind: "InsertOrUpdateBatch", tasks: append([]db.Task(nil), tasks...)})
}

func (m *stubSyncTaskDB) Del(ctx context.Context, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, syncTaskOp{kind: "Del", ids: []string{id}})
}

func (m *stubSyncTaskDB) DelBatch(ctx context.Context, ids []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, syncTaskOp{kind: "DelBatch", ids: append([]string(nil), ids...)})
}

func (m *stubSyncTaskDB) Clear(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, syncTaskOp{kind: "Clear"})
}

func (m *stubSyncTaskDB) Ops() []syncTaskOp {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]syncTaskOp(nil), m.ops...)
}

// ---------- ChangeStream ----------

type csBatch struct {
	values []db.StreamValue
	lastId db.StreamId
}

type stubCSIter struct {
	batches  []csBatch
	i        int
	cur      csBatch
	err      error
	limits   []int
	released bool
}

func (it *stubCSIter) Next(ctx context.Context, limit int) bool {
	it.limits = append(it.limits, limit)
	if err := ctx.Err(); err != nil {
		it.err = err
		return false
	}
	if it.i >= len(it.batches) {
		return false
	}
	it.cur = it.batches[it.i]
	it.i++
	return true
}

func (it *stubCSIter) Values() []db.StreamValue { return it.cur.values }

func (it *stubCSIter) LastStreamId() db.StreamId {
	if len(it.cur.values) == 0 {
		return nil
	}
	return it.cur.lastId
}

func (it *stubCSIter) Err() error { return it.err }

func (it *stubCSIter) Release(ctx context.Context) { it.released = true }

func (it *stubCSIter) Limits() []int { return append([]int(nil), it.limits...) }

type stubChangeStreamDB struct {
	mu     sync.Mutex
	iter   db.ChangeStreamIterator
	iterFn func() db.ChangeStreamIterator
	marked []db.StreamId
	lastId db.StreamId
}

func (m *stubChangeStreamDB) Save(ctx context.Context, rt db.ResumeToken, v db.StreamValue) (db.StreamId, error) {
	return nil, nil
}

func (m *stubChangeStreamDB) DeleteAll(ctx context.Context) {}

func (m *stubChangeStreamDB) MarkSentUpTo(ctx context.Context, id db.StreamId) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.marked = append(m.marked, append(db.StreamId(nil), id...))
}

func (m *stubChangeStreamDB) AllNotSent(ctx context.Context) db.ChangeStreamIterator {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.iterFn != nil {
		return m.iterFn()
	}
	if m.iter == nil {
		return &stubCSIter{}
	}
	return m.iter
}

func (m *stubChangeStreamDB) LastStreamId() db.StreamId { return m.lastId }

func (m *stubChangeStreamDB) ResumeToken(ctx context.Context) db.ResumeToken { return nil }

func (m *stubChangeStreamDB) Marked() []db.StreamId {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]db.StreamId(nil), m.marked...)
}

// ---------- WatchCollection ----------

type wcOp struct {
	kind    string
	version uint64
	infos   []x.WatchInfo
}

type stubWatchCollectionDB struct {
	mu            sync.Mutex
	ops           []wcOp
	versions      map[uint64][]x.WatchInfo
	latestVersion uint64
	latestSynced  uint64
	needSyncing   bool
	syncingVer    uint64
}

func newStubWatchCollectionDB() *stubWatchCollectionDB {
	return &stubWatchCollectionDB{versions: make(map[uint64][]x.WatchInfo)}
}

func (m *stubWatchCollectionDB) LatestVersion(ctx context.Context) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latestVersion
}

func (m *stubWatchCollectionDB) Save(ctx context.Context, w []x.WatchInfo, version uint64) (uint64, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old := m.latestVersion
	m.ops = append(m.ops, wcOp{kind: "Save", version: version, infos: append([]x.WatchInfo(nil), w...)})
	m.versions[version] = append([]x.WatchInfo(nil), w...)
	if version > m.latestVersion {
		m.latestVersion = version
	}
	return old, m.latestVersion
}

func (m *stubWatchCollectionDB) Get(ctx context.Context, version uint64) []x.WatchInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]x.WatchInfo(nil), m.versions[version]...)
}

func (m *stubWatchCollectionDB) DelLessThan(ctx context.Context, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, wcOp{kind: "DelLessThan", version: version})
}

func (m *stubWatchCollectionDB) MarkSyncing(ctx context.Context, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, wcOp{kind: "MarkSyncing", version: version})
	m.needSyncing = true
	m.syncingVer = version
}

func (m *stubWatchCollectionDB) NeedSyncing(ctx context.Context) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.syncingVer, m.needSyncing
}

func (m *stubWatchCollectionDB) ClearSyncingAndMarkSynced(ctx context.Context, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, wcOp{kind: "ClearSyncingAndMarkSynced", version: version})
	m.needSyncing = false
	m.latestSynced = version
}

func (m *stubWatchCollectionDB) LatestSynced(ctx context.Context) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latestSynced
}

func (m *stubWatchCollectionDB) Clear(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ops = append(m.ops, wcOp{kind: "Clear"})
	m.versions = make(map[uint64][]x.WatchInfo)
	m.latestVersion = 0
	m.latestSynced = 0
	m.needSyncing = false
	m.syncingVer = 0
}

func (m *stubWatchCollectionDB) Ops() []wcOp {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]wcOp(nil), m.ops...)
}

// ---------- 辅助 ----------

func installStubs(t *testing.T) (*stubSyncTaskDB, *stubChangeStreamDB, *stubWatchCollectionDB) {
	t.Helper()

	st := &stubSyncTaskDB{}
	cs := &stubChangeStreamDB{iter: &stubCSIter{}}
	wc := newStubWatchCollectionDB()

	db.SetSyncTask(st)
	db.SetChangeStream(cs)
	db.SetWatchCollection(wc)

	return st, cs, wc
}

func waitCode(t *testing.T, s *Sender, timeout time.Duration) SenderCode {
	t.Helper()
	select {
	case c := <-s.Done():
		return c
	case <-time.After(timeout):
		t.Fatalf("等待 sender.Done() 超时(%s)", timeout)
		return 0
	}
}

// ================= sender.go 用例 =================

func TestSenderCodeStr(t *testing.T) {
	cases := []struct {
		code SenderCode
		want string
	}{
		{Ok, "OK"},
		{ByStopped, "ByStopped"},
		{NeedForceSync, "NeedForceSync"},
		{SendFailed, "SendFailed"},
		{UnknownErr, "UnknownErr"},
		{SenderCode(99), "UnknownCode"},
	}
	for _, c := range cases {
		if got := SenderCodeStr(c.code); got != c.want {
			t.Errorf("SenderCodeStr(%d) = %q, want %q", c.code, got, c.want)
		}
	}
}

func TestNewSender2InitiallyStopped(t *testing.T) {
	installStubs(t)
	s := NewSender2(context.Background(), nil, 2, &stubNetLayer{})

	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("初始状态应为 stopped，Stop() 不应阻塞")
	}

	select {
	case <-s.StopNotWait():
	case <-time.After(time.Second):
		t.Fatal("StopNotWait() 返回的 channel 应已关闭")
	}

	select {
	case c := <-s.Done():
		t.Fatalf("未启动不应产生 Done 事件, got %v", c)
	default:
	}
}

// 空 SyncTask + 空 ChangeStream：sender 跑完一轮后应报告 Ok
func TestSenderOkOnEmpty(t *testing.T) {
	installStubs(t)
	s := NewSender2(context.Background(), nil, 2, &stubNetLayer{})

	s.Start()
	if got := waitCode(t, s, 3*time.Second); got != Ok {
		t.Fatalf("Done = %s, want OK", SenderCodeStr(got))
	}

	// 已经自行停止，Stop() 必须立即返回，不能死锁
	stopped := make(chan struct{})
	go func() {
		s.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("结束后 Stop() 不应阻塞")
	}
}

// 运行期间重复 Start 必须返回 busy=true。
// 用阻塞的 Send 固定住"运行中"这一时刻，避免与 goroutine 自行结束产生竞态
func TestSenderStartIsBusyWhileRunning(t *testing.T) {
	_, cs, _ := installStubs(t)

	cs.iterFn = func() db.ChangeStreamIterator {
		return &stubCSIter{batches: []csBatch{
			{values: []db.StreamValue{bson.Raw([]byte("v"))}, lastId: db.StreamId([]byte("id"))},
		}}
	}

	entered := make(chan struct{})
	layer := &stubNetLayer{onSend: func(ctx context.Context, c sendCall) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-ctx.Done()
		return ctx.Err()
	}}

	s := NewSender2(context.Background(), nil, 2, layer)
	if busy := s.Start(); busy {
		t.Fatal("第一次 Start 应返回 false")
	}

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Send 未被调用")
	}
	if busy := s.Start(); !busy {
		t.Fatal("运行中的第二次 Start 应返回 true")
	}

	s.Stop()
}

// 跑完一轮后必须能再次启动，且 doneChan 中残留的旧事件被清掉
func TestSenderRestartAfterDone(t *testing.T) {
	installStubs(t)
	s := NewSender2(context.Background(), nil, 2, &stubNetLayer{})

	s.Start()
	if got := waitCode(t, s, 3*time.Second); got != Ok {
		t.Fatalf("第一轮 Done = %s, want OK", SenderCodeStr(got))
	}

	if busy := s.Start(); busy {
		t.Fatal("结束后的 Start 应返回 false")
	}
	if got := waitCode(t, s, 3*time.Second); got != Ok {
		t.Fatalf("第二轮 Done = %s, want OK", SenderCodeStr(got))
	}
}

// ChangeStream 多批次：按序发送、按序 MarkSentUpTo
func TestSenderSendsChangeStreamInOrder(t *testing.T) {
	_, cs, _ := installStubs(t)

	id1 := db.StreamId([]byte("id-1"))
	id2 := db.StreamId([]byte("id-2"))
	cs.iterFn = func() db.ChangeStreamIterator {
		return &stubCSIter{batches: []csBatch{
			{values: []db.StreamValue{bson.Raw([]byte("v1")), bson.Raw([]byte("v2"))}, lastId: id1},
			{values: []db.StreamValue{bson.Raw([]byte("v3"))}, lastId: id2},
		}}
	}

	layer := &stubNetLayer{}
	s := NewSender2(context.Background(), nil, 2, layer)
	s.Start()
	if got := waitCode(t, s, 3*time.Second); got != Ok {
		t.Fatalf("Done = %s, want OK", SenderCodeStr(got))
	}

	calls := layer.Calls()
	if len(calls) != 2 {
		t.Fatalf("Send 调用次数 = %d, want 2", len(calls))
	}
	for i, c := range calls {
		if c.ty != ChangeStream {
			t.Errorf("calls[%d].ty = %v, want ChangeStream", i, c.ty)
		}
		if c.db != "" || c.coll != "" {
			t.Errorf("calls[%d] Ns = {%q,%q}, 应为空", i, c.db, c.coll)
		}
	}
	if len(calls[0].data) != 2 || string(calls[0].data[0]) != "v1" || string(calls[0].data[1]) != "v2" {
		t.Errorf("第一批 data = %q, want [v1 v2]", calls[0].data)
	}
	if len(calls[1].data) != 1 || string(calls[1].data[0]) != "v3" {
		t.Errorf("第二批 data = %q, want [v3]", calls[1].data)
	}

	marked := cs.Marked()
	if len(marked) != 2 || string(marked[0]) != "id-1" || string(marked[1]) != "id-2" {
		t.Fatalf("MarkSentUpTo = %q, want [id-1 id-2]", marked)
	}
}

// batch 必须作为 limit 原样传给迭代器
func TestSenderPassesBatchAsLimit(t *testing.T) {
	_, cs, _ := installStubs(t)

	iter := &stubCSIter{batches: []csBatch{
		{values: []db.StreamValue{bson.Raw([]byte("v"))}, lastId: db.StreamId([]byte("id"))},
	}}
	cs.iterFn = func() db.ChangeStreamIterator { return iter }

	s := NewSender2(context.Background(), nil, 7, &stubNetLayer{})
	s.Start()
	if got := waitCode(t, s, 3*time.Second); got != Ok {
		t.Fatalf("Done = %s, want OK", SenderCodeStr(got))
	}

	limits := iter.Limits()
	if len(limits) == 0 {
		t.Fatal("迭代器 Next 未被调用")
	}
	for i, l := range limits {
		if l != 7 {
			t.Errorf("limits[%d] = %d, want 7", i, l)
		}
	}
}

// 发送失败立即停止，后续批次不再发送也不推进已发送点
func TestSenderSendFailed(t *testing.T) {
	_, cs, _ := installStubs(t)

	cs.iterFn = func() db.ChangeStreamIterator {
		return &stubCSIter{batches: []csBatch{
			{values: []db.StreamValue{bson.Raw([]byte("v1"))}, lastId: db.StreamId([]byte("id-1"))},
			{values: []db.StreamValue{bson.Raw([]byte("v2"))}, lastId: db.StreamId([]byte("id-2"))},
		}}
	}

	layer := &stubNetLayer{err: ErrSendFailed}
	s := NewSender2(context.Background(), nil, 2, layer)
	s.Start()
	if got := waitCode(t, s, 3*time.Second); got != SendFailed {
		t.Fatalf("Done = %s, want SendFailed", SenderCodeStr(got))
	}
	if n := len(layer.Calls()); n != 1 {
		t.Fatalf("Send 调用次数 = %d, want 1", n)
	}
	if n := len(cs.Marked()); n != 0 {
		t.Fatalf("失败时不应 MarkSentUpTo, got %d 次", n)
	}
}

func TestSenderNeedForceSync(t *testing.T) {
	_, cs, _ := installStubs(t)
	cs.iterFn = func() db.ChangeStreamIterator {
		return &stubCSIter{err: db.ErrNotFoundSentPoint}
	}

	s := NewSender2(context.Background(), nil, 2, &stubNetLayer{})
	s.Start()
	if got := waitCode(t, s, 3*time.Second); got != NeedForceSync {
		t.Fatalf("Done = %s, want NeedForceSync", SenderCodeStr(got))
	}
}

func TestSenderUnknownErr(t *testing.T) {
	_, cs, _ := installStubs(t)
	cs.iterFn = func() db.ChangeStreamIterator {
		return &stubCSIter{err: errors.New("boom")}
	}

	s := NewSender2(context.Background(), nil, 2, &stubNetLayer{})
	s.Start()
	if got := waitCode(t, s, 3*time.Second); got != UnknownErr {
		t.Fatalf("Done = %s, want UnknownErr", SenderCodeStr(got))
	}
}

// 父 ctx 被取消（非主动 Stop）：cancelF 仍在，应向外报告 ByStopped
func TestSenderByStoppedOnParentCancel(t *testing.T) {
	_, cs, _ := installStubs(t)

	cs.iterFn = func() db.ChangeStreamIterator {
		return &stubCSIter{batches: []csBatch{
			{values: []db.StreamValue{bson.Raw([]byte("v"))}, lastId: db.StreamId([]byte("id"))},
		}}
	}

	entered := make(chan struct{})
	var once sync.Once
	layer := &stubNetLayer{onSend: func(ctx context.Context, c sendCall) error {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return ctx.Err()
	}}

	parent, cancel := context.WithCancel(context.Background())
	s := NewSender2(parent, nil, 2, layer)
	s.Start()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Send 未被调用")
	}
	cancel()

	if got := waitCode(t, s, 3*time.Second); got != ByStopped {
		t.Fatalf("Done = %s, want ByStopped", SenderCodeStr(got))
	}
}

// 主动 Stop 属于外部控制，不应产生 Done 事件（否则 scheduler 会误判为一次自发结束）
func TestSenderStopDoesNotEmitDone(t *testing.T) {
	_, cs, _ := installStubs(t)

	cs.iterFn = func() db.ChangeStreamIterator {
		return &stubCSIter{batches: []csBatch{
			{values: []db.StreamValue{bson.Raw([]byte("v"))}, lastId: db.StreamId([]byte("id"))},
		}}
	}

	entered := make(chan struct{})
	layer := &stubNetLayer{onSend: func(ctx context.Context, c sendCall) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-ctx.Done()
		return ctx.Err()
	}}

	s := NewSender2(context.Background(), nil, 2, layer)
	s.Start()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Send 未被调用")
	}
	s.Stop()

	select {
	case c := <-s.Done():
		t.Fatalf("主动 Stop 不应产生 Done 事件, got %s", SenderCodeStr(c))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestSenderStopNotWait(t *testing.T) {
	_, cs, _ := installStubs(t)

	cs.iterFn = func() db.ChangeStreamIterator {
		return &stubCSIter{batches: []csBatch{
			{values: []db.StreamValue{bson.Raw([]byte("v"))}, lastId: db.StreamId([]byte("id"))},
		}}
	}

	entered := make(chan struct{})
	layer := &stubNetLayer{onSend: func(ctx context.Context, c sendCall) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-ctx.Done()
		return ctx.Err()
	}}

	s := NewSender2(context.Background(), nil, 2, layer)
	s.Start()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Send 未被调用")
	}

	stopped := s.StopNotWait()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("StopNotWait 返回的 channel 未被关闭")
	}
}

// 说明：sync() 中真正访问 MongoDB 的分页查询路径需要真实 mongo 实例，单测不覆盖。
// 以上用例通过空 SyncTask 迭代器让 sync() 直接返回，从而覆盖 sendChangeStream 全链路。
