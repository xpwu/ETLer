package task

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// 共享替身（stubSyncTaskDB / stubChangeStreamDB / stubWatchCollectionDB / installStubs）
// 定义在 sender_test.go 中，本文件直接复用。

// diffSyncTask 的 add/del 来自 map 遍历，顺序不确定，必须用集合比较
func infoSet(infos []x.WatchInfo) map[string]bool {
	m := make(map[string]bool, len(infos))
	for _, i := range infos {
		m[i.Id()] = true
	}
	return m
}

func assertSameInfos(t *testing.T, got, want []x.WatchInfo, name string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("%s: 数量 = %d, want %d (got=%v)", name, len(got), len(want), got)
	}
	g, w := infoSet(got), infoSet(want)
	for id := range w {
		if !g[id] {
			t.Errorf("%s: 缺少 %s", name, id)
		}
	}
	for id := range g {
		if !w[id] {
			t.Errorf("%s: 多出 %s", name, id)
		}
	}
}

func assertSameIDs(t *testing.T, got, want []string, name string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("%s: 数量 = %d, want %d (got=%v)", name, len(got), len(want), got)
	}
	g := make(map[string]bool, len(got))
	for _, id := range got {
		g[id] = true
	}
	for _, id := range want {
		if !g[id] {
			t.Errorf("%s: 缺少 %s", name, id)
		}
	}
}

func taskInfos(tasks []db.Task) []x.WatchInfo {
	out := make([]x.WatchInfo, 0, len(tasks))
	for _, tk := range tasks {
		out = append(out, tk.WatchInfo)
	}
	return out
}

func infos(names ...string) []x.WatchInfo {
	out := make([]x.WatchInfo, 0, len(names))
	for _, n := range names {
		out = append(out, x.WatchInfo{DB: "d", Collection: n})
	}
	return out
}

func drainSignals() {
	for {
		select {
		case <-forceFullSync:
		case <-watchCollectionUpdated:
		default:
			return
		}
	}
}

// ================= 纯函数 =================

func TestMinKeyTask(t *testing.T) {
	info := x.WatchInfo{DB: "db1", Collection: "c1"}
	tk := MinKeyTask(info)

	if tk.UntilDocId.Type != bson.TypeMinKey {
		t.Errorf("UntilDocId.Type = %v, want TypeMinKey", tk.UntilDocId.Type)
	}
	if len(tk.UntilDocId.Value) != 0 {
		t.Errorf("UntilDocId.Value = %v, want empty", tk.UntilDocId.Value)
	}
	if tk.DB != info.DB || tk.Collection != info.Collection {
		t.Errorf("WatchInfo = %+v, want %+v", tk.WatchInfo, info)
	}
	if tk.Id() != info.Id() {
		t.Errorf("Id() = %q, want %q", tk.Id(), info.Id())
	}
}

func TestDiffSyncTask(t *testing.T) {
	cases := []struct {
		name       string
		newer, old []x.WatchInfo
		add, del   []x.WatchInfo
	}{
		{"全部为空", nil, nil, nil, nil},
		{"新增", infos("A", "B"), nil, infos("A", "B"), nil},
		{"删除", nil, infos("A", "B"), nil, infos("A", "B")},
		{"混合", infos("A", "D"), infos("A", "B", "C"), infos("D"), infos("B", "C")},
		{"无变化", infos("A", "B"), infos("A", "B"), nil, nil},
		{"输入重复项被去重", append(infos("A", "B"), infos("A")...), infos("B"),
			infos("A"), nil},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := diffSyncTask(c.newer, c.old)
			assertSameInfos(t, got.Add, c.add, "Add")
			assertSameInfos(t, got.Del, c.del, "Del")
		})
	}
}

func TestUpdateSyncTask(t *testing.T) {
	st, _, _ := installStubs(t)

	updateSyncTask(context.Background(), SyncTaskDelta{
		Add: infos("new"),
		Del: infos("old"),
	})

	ops := st.Ops()
	if len(ops) != 2 {
		t.Fatalf("ops 数量 = %d, want 2", len(ops))
	}
	if ops[0].kind != "InsertOrUpdateBatch" {
		t.Errorf("ops[0] = %s, want InsertOrUpdateBatch", ops[0].kind)
	}
	if ops[1].kind != "DelBatch" {
		t.Errorf("ops[1] = %s, want DelBatch", ops[1].kind)
	}
	if len(ops[0].tasks) != 1 {
		t.Fatalf("新增任务数 = %d, want 1", len(ops[0].tasks))
	}
	if ops[0].tasks[0].UntilDocId.Type != bson.TypeMinKey {
		t.Error("新增任务不是 MinKeyTask")
	}
	assertSameInfos(t, taskInfos(ops[0].tasks), infos("new"), "InsertOrUpdateBatch")
	assertSameIDs(t, ops[1].ids, []string{"d.old"}, "DelBatch")
}

func TestFullSyncTaskify(t *testing.T) {
	st, cs, wc := installStubs(t)

	wc.versions[3] = infos("A", "B")
	wc.latestVersion = 3
	cs.lastId = db.StreamId([]byte("last-id"))

	fullSyncTaskify(context.Background())

	// 全量同步标记 → 最高优先级的 MarkSyncing
	wops := wc.Ops()
	if len(wops) != 2 {
		t.Fatalf("WatchCollection ops = %d, want 2: %+v", len(wops), wops)
	}
	if wops[0].kind != "MarkSyncing" || wops[0].version != math.MaxUint64 {
		t.Errorf("wops[0] = %+v, want MarkSyncing(MaxUint64)", wops[0])
	}
	if wops[1].kind != "ClearSyncingAndMarkSynced" || wops[1].version != 3 {
		t.Errorf("wops[1] = %+v, want ClearSyncingAndMarkSynced(3)", wops[1])
	}

	// 先清空旧任务，再按最新版本重建
	sops := st.Ops()
	if len(sops) != 2 {
		t.Fatalf("SyncTask ops = %d, want 2: %+v", len(sops), sops)
	}
	if sops[0].kind != "Clear" {
		t.Errorf("sops[0] = %s, want Clear", sops[0].kind)
	}
	if sops[1].kind != "InsertOrUpdateBatch" {
		t.Errorf("sops[1] = %s, want InsertOrUpdateBatch", sops[1].kind)
	}
	assertSameInfos(t, taskInfos(sops[1].tasks), infos("A", "B"), "全量任务")
	for _, tk := range sops[1].tasks {
		if tk.UntilDocId.Type != bson.TypeMinKey {
			t.Error("全量任务不是 MinKeyTask")
		}
	}

	// 全量同步之前的 change stream 都应标记为已发送
	marked := cs.Marked()
	if len(marked) != 1 || string(marked[0]) != "last-id" {
		t.Fatalf("MarkSentUpTo = %q, want [last-id]", marked)
	}
}

func TestBackFillSyncTaskifyFullSync(t *testing.T) {
	st, _, wc := installStubs(t)

	wc.latestVersion = 2
	wc.versions[2] = infos("A")
	wc.needSyncing = true
	wc.syncingVer = math.MaxUint64 // full sync 标记

	backFillSyncTaskify(context.Background())

	sops := st.Ops()
	if len(sops) == 0 || sops[0].kind != "Clear" {
		t.Fatalf("full sync 标记应触发 fullSyncTaskify, got %+v", sops)
	}
}

func TestBackFillSyncTaskifyDelta(t *testing.T) {
	st, _, wc := installStubs(t)

	wc.versions[1] = infos("A")
	wc.versions[2] = infos("A", "B")
	wc.latestVersion = 2
	wc.latestSynced = 1

	backFillSyncTaskify(context.Background())

	sops := st.Ops()
	if len(sops) != 2 {
		t.Fatalf("SyncTask ops = %d, want 2: %+v", len(sops), sops)
	}
	assertSameInfos(t, taskInfos(sops[0].tasks), infos("B"), "增量新增")
}

func TestBackFillSyncTaskifyNoop(t *testing.T) {
	st, _, wc := installStubs(t)

	wc.latestVersion = 2
	wc.latestSynced = 2
	wc.versions[2] = infos("A")

	backFillSyncTaskify(context.Background())

	if len(st.Ops()) != 0 {
		t.Fatalf("已同步完不应产生任务, got %+v", st.Ops())
	}
}

func TestDeltaSyncTaskifyNoChange(t *testing.T) {
	st, _, wc := installStubs(t)

	wc.latestVersion = 2
	wc.latestSynced = 2

	deltaSyncTaskify(context.Background())

	if len(st.Ops()) != 0 || len(wc.Ops()) != 0 {
		t.Fatalf("无变化不应有副作用: st=%+v wc=%+v", st.Ops(), wc.Ops())
	}
}

// 对应源码注释里的 v1/v2/v3 场景：
// v1={A,B,C} v2={A,D} v3={A,B,C,E}，且 v2 处于 syncing 中。
// 必须先把 syncing 的版本任务化，再任务化 latest，否则 D 会残留。
func TestDeltaSyncTaskifyWithPendingSyncing(t *testing.T) {
	st, _, wc := installStubs(t)

	wc.versions[1] = infos("A", "B", "C")
	wc.versions[2] = infos("A", "D")
	wc.versions[3] = infos("A", "B", "C", "E")
	wc.latestVersion = 3
	wc.latestSynced = 1
	wc.needSyncing = true
	wc.syncingVer = 2

	deltaSyncTaskify(context.Background())

	sops := st.Ops()
	if len(sops) != 4 {
		t.Fatalf("SyncTask ops = %d, want 4: %+v", len(sops), sops)
	}
	// 第一组：先补完 v2 相对 v1 的差异
	assertSameInfos(t, taskInfos(sops[0].tasks), infos("D"), "v2 新增")
	assertSameIDs(t, sops[1].ids, []string{"d.B", "d.C"}, "v2 删除")
	// 第二组：再任务化 v3 相对 v2 的差异
	assertSameInfos(t, taskInfos(sops[2].tasks), infos("B", "C", "E"), "v3 新增")
	assertSameIDs(t, sops[3].ids, []string{"d.D"}, "v3 删除")

	wops := wc.Ops()
	wantOps := []wcOp{
		{kind: "ClearSyncingAndMarkSynced", version: 2},
		{kind: "MarkSyncing", version: 3},
		{kind: "ClearSyncingAndMarkSynced", version: 3},
		{kind: "DelLessThan", version: 3},
	}
	if len(wops) != len(wantOps) {
		t.Fatalf("WatchCollection ops = %+v, want %+v", wops, wantOps)
	}
	for i, want := range wantOps {
		if wops[i].kind != want.kind || wops[i].version != want.version {
			t.Errorf("wops[%d] = %+v, want %+v", i, wops[i], want)
		}
	}
}

// syncing 版本落后于 latestSynced 属于异常状态，应降级为全量同步
func TestDeltaSyncTaskifySyncingLessThanSynced(t *testing.T) {
	st, _, wc := installStubs(t)

	wc.versions[3] = infos("A")
	wc.latestVersion = 3
	wc.latestSynced = 2
	wc.needSyncing = true
	wc.syncingVer = 1

	deltaSyncTaskify(context.Background())

	sops := st.Ops()
	if len(sops) == 0 || sops[0].kind != "Clear" {
		t.Fatalf("异常状态应降级为 fullSyncTaskify, got %+v", sops)
	}
}

func TestInitTaskFromConfigSkipsWhenAPIVersion(t *testing.T) {
	st, _, wc := installStubs(t)

	// 已有 API 设置的版本时，配置文件不能覆盖
	wc.latestVersion = 1

	if !InitTaskFromConfig(context.Background()) {
		t.Fatal("succeed 应为 true")
	}
	if len(st.Ops()) != 0 || len(wc.Ops()) != 0 {
		t.Fatalf("不应产生任何副作用: st=%+v wc=%+v", st.Ops(), wc.Ops())
	}
}

func TestInitTaskFromConfigApplies(t *testing.T) {
	old := config.Watch.Collections
	config.Watch.Collections = infos("new")
	t.Cleanup(func() { config.Watch.Collections = old })

	st, _, wc := installStubs(t)
	wc.versions[0] = infos("old")
	wc.latestVersion = 0

	if !InitTaskFromConfig(context.Background()) {
		t.Fatal("succeed 应为 true")
	}

	// 先按配置差异更新任务
	sops := st.Ops()
	if len(sops) != 2 {
		t.Fatalf("SyncTask ops = %d, want 2: %+v", len(sops), sops)
	}
	assertSameInfos(t, taskInfos(sops[0].tasks), infos("new"), "配置新增")
	assertSameIDs(t, sops[1].ids, []string{"d.old"}, "配置删除")

	// 必须在最后才 Clear/Save/标记同步完成
	wops := wc.Ops()
	wantOps := []wcOp{
		{kind: "Clear"},
		{kind: "Save", version: db.ConfigVersion},
		{kind: "ClearSyncingAndMarkSynced", version: db.ConfigVersion},
	}
	if len(wops) != len(wantOps) {
		t.Fatalf("WatchCollection ops = %+v, want %+v", wops, wantOps)
	}
	for i, want := range wantOps {
		if wops[i].kind != want.kind || wops[i].version != want.version {
			t.Errorf("wops[%d] = %+v, want %+v", i, wops[i], want)
		}
	}
}

// InitTaskFromConfig 内置 recover，panic 时返回 false 而不是让进程崩溃
func TestInitTaskFromConfigRecoversFromPanic(t *testing.T) {
	defer installStubs(t) // 恢复全局单例，避免影响后续用例

	db.SetWatchCollection(nil) // 内嵌 nil 接口，调用方法会 panic
	if InitTaskFromConfig(context.Background()) {
		t.Fatal("发生 panic 时 succeed 应为 false")
	}
}

// ================= 通道/入口 =================

func TestSyncTaskUpdaterChannel(t *testing.T) {
	ch := SyncTaskUpdater() // chan<- 只能发送，接收侧用包内双向变量
	want := SyncTaskDelta{Add: infos("A")}

	got := make(chan SyncTaskDelta, 1)
	go func() { got <- <-updateSyncTaskChan }()

	ch <- want

	select {
	case g := <-got:
		assertSameInfos(t, g.Add, want.Add, "SyncTaskDelta.Add")
	case <-time.After(time.Second):
		t.Fatal("SyncTaskUpdater() 返回的 channel 无法收发")
	}
}

func TestPostForceFullSyncIsNonBlocking(t *testing.T) {
	drainSignals()
	t.Cleanup(drainSignals)

	PostForceFullSync()
	PostForceFullSync() // 缓冲为 1，第二次必须走 default 而不是阻塞

	if len(forceFullSync) != 1 {
		t.Fatalf("forceFullSync 中信号数 = %d, want 1", len(forceFullSync))
	}
}

func TestWatchCollectionUpdatedIsNonBlocking(t *testing.T) {
	drainSignals()
	t.Cleanup(drainSignals)

	WatchCollectionUpdated()
	WatchCollectionUpdated()

	if len(watchCollectionUpdated) != 1 {
		t.Fatalf("watchCollectionUpdated 中信号数 = %d, want 1", len(watchCollectionUpdated))
	}
}

func TestBatchAndRetryConstants(t *testing.T) {
	if batch != 1000 {
		t.Errorf("batch = %d, want 1000", batch)
	}
	if retry != 15*time.Second {
		t.Errorf("retry = %s, want 15s", retry)
	}
}

// 说明：startAndBlock 是无限循环且依赖真实 MongoDB 客户端，单测不覆盖；
// 其对 SenderCode 的各分支处理由 sender_test.go 中对应的 Done 事件用例间接保证。
