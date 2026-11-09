package db

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/xpwu/ETLer/x"
)

// ================= WatchCollectionDBer 替身 =================
// 本文件只测 WatchCollectionDB 的三个包装方法，
// 所以 stub 只需记录 MarkSyncing / NeedSyncing 的入参与返回。
// watchCollection 是包级全局单例，用例不能 t.Parallel()，且要恢复原值。

type fullSyncStub struct {
	mu sync.Mutex

	markVersions []uint64
	markCtxs     []context.Context

	needVersion uint64
	needOK      bool
	needCalls   int
}

func (m *fullSyncStub) LatestVersion(ctx context.Context) uint64 { return 0 }

func (m *fullSyncStub) Save(ctx context.Context, w []x.WatchInfo, version uint64) (uint64, uint64) {
	return 0, 0
}

func (m *fullSyncStub) Get(ctx context.Context, version uint64) []x.WatchInfo { return nil }

func (m *fullSyncStub) DelLessThan(ctx context.Context, version uint64) {}

func (m *fullSyncStub) MarkSyncing(ctx context.Context, version uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markVersions = append(m.markVersions, version)
	m.markCtxs = append(m.markCtxs, ctx)
}

func (m *fullSyncStub) NeedSyncing(ctx context.Context) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.needCalls++
	return m.needVersion, m.needOK
}

func (m *fullSyncStub) ClearSyncingAndMarkSynced(ctx context.Context, version uint64) {}

func (m *fullSyncStub) LatestSynced(ctx context.Context) uint64 { return 0 }

func (m *fullSyncStub) Clear(ctx context.Context) {}

func (m *fullSyncStub) MarkVersions() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]uint64(nil), m.markVersions...)
}

func (m *fullSyncStub) MarkCtxs() []context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]context.Context(nil), m.markCtxs...)
}

func (m *fullSyncStub) NeedCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.needCalls
}

func installFullSyncStub(t *testing.T) *fullSyncStub {
	t.Helper()

	stub := &fullSyncStub{}
	old := watchCollection
	SetWatchCollection(stub)
	t.Cleanup(func() { watchCollection = old })
	return stub
}

type ctxKey struct{}

// ================= 三个包装方法 =================

// MarkFullSyncing 必须把 math.MaxUint64 透传给 MarkSyncing，
// 这是 scheduler 里 full sync 优先级最高的标记值。
func TestMarkFullSyncing(t *testing.T) {
	stub := installFullSyncStub(t)
	ctx := context.WithValue(context.Background(), ctxKey{}, "from-api")

	WatchCollection().MarkFullSyncing(ctx)

	versions := stub.MarkVersions()
	if len(versions) != 1 {
		t.Fatalf("MarkSyncing 调用次数 = %d, want 1", len(versions))
	}
	if versions[0] != math.MaxUint64 {
		t.Errorf("version = %d, want math.MaxUint64 (%d)", versions[0], uint64(math.MaxUint64))
	}

	ctxs := stub.MarkCtxs()
	if len(ctxs) != 1 {
		t.Fatalf("ctx 记录数 = %d, want 1", len(ctxs))
	}
	if got := ctxs[0].Value(ctxKey{}); got != "from-api" {
		t.Errorf("ctx 未透传, Value = %v", got)
	}
}

// NeedFullSyncing 的判定是 need && version == MaxUint64，
// 两种条件缺一不可：普通 delta 版本、以及 need=false 都不能误判为全量同步。
func TestNeedFullSyncing(t *testing.T) {
	cases := []struct {
		name    string
		version uint64
		need    bool
		want    bool
	}{
		{"全量同步标记", math.MaxUint64, true, true},
		{"无同步标记但版本是 Max", math.MaxUint64, false, false},
		{"普通 delta 版本", 3, true, false},
		{"版本 0 且无标记", 0, false, false},
		{"版本 0 且有标记", 0, true, false},
		{"MaxUint64-1 且有标记", math.MaxUint64 - 1, true, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stub := installFullSyncStub(t)
			stub.needVersion = c.version
			stub.needOK = c.need

			got := WatchCollection().NeedFullSyncing(context.Background())
			if got != c.want {
				t.Fatalf("NeedFullSyncing() = %v, want %v (version=%d, need=%v)",
					got, c.want, c.version, c.need)
			}
			if stub.NeedCalls() != 1 {
				t.Errorf("NeedSyncing 调用次数 = %d, want 1", stub.NeedCalls())
			}
		})
	}
}

// IsFullSyncing 是纯函数（不碰 db），逻辑与 NeedFullSyncing 一致。
// 目前全仓无调用点，此处把行为固化：若将来删除该方法，本用例会编译失败并提醒。
func TestIsFullSyncing(t *testing.T) {
	cases := []struct {
		name    string
		version uint64
		need    bool
		want    bool
	}{
		{"全量同步标记", math.MaxUint64, true, true},
		{"无同步标记但版本是 Max", math.MaxUint64, false, false},
		{"普通 delta 版本", 3, true, false},
		{"版本 0 且无标记", 0, false, false},
		{"版本 0 且有标记", 0, true, false},
		{"MaxUint64-1 且有标记", math.MaxUint64 - 1, true, false},
	}

	wc := &WatchCollectionDB{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := wc.IsFullSyncing(c.version, c.need)
			if got != c.want {
				t.Fatalf("IsFullSyncing(%d, %v) = %v, want %v", c.version, c.need, got, c.want)
			}
		})
	}
}

// SetWatchCollection 把 dber 包进 *WatchCollectionDB，WatchCollection() 返回它。
func TestSetWatchCollection(t *testing.T) {
	stub := installFullSyncStub(t)

	wc := WatchCollection()
	if wc == nil {
		t.Fatal("WatchCollection() 不应为 nil")
	}
	if wc.WatchCollectionDBer != stub {
		t.Fatal("WatchCollection() 应包装 SetWatchCollection 传入的 dber")
	}
}

// 其余内容（接口声明、类型别名、Task 结构体、ErrNotFoundSentPoint、
// Set/Get 系列全局单例、ConfigVersion）在本包内没有可观察行为，
// 已由 bboltdb / etl / etl/task / etl/changestream 各包的用例间接覆盖，故不重复写用例。
