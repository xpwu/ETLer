package httpapi

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/xpwu/ETLer/x"
)

func TestAPIForceSyncCollAccepted(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 5
	stub.versions[5] = []x.WatchInfo{wi("db1", "A"), wi("db1", "B")}

	req := &syncCollectionReq{Collections: []x.WatchInfo{wi("db1", "A")}}
	got := startDeltaReceiver(t)

	res := (&suite{}).APIForceSyncColl(context.Background(), req)

	if res == nil || !res.Succeed {
		t.Fatalf("res = %+v, want Succeed=true", res)
	}

	select {
	case d := <-got:
		if !reflect.DeepEqual(d.Add, req.Collections) {
			t.Errorf("Delta.Add = %+v, want %+v", d.Add, req.Collections)
		}
		if len(d.Del) != 0 {
			t.Errorf("Delta.Del = %+v, 应为空", d.Del)
		}
	case <-time.After(time.Second):
		t.Fatal("没有收到 SyncTaskDelta")
	}
}

// 不在监听集合中的项必须被拒绝，且不能发出 Delta。
// updateSyncTaskChan 是无缓冲的，一旦误发就会因为没有接收方而阻塞；
// 所以"handler 及时返回"本身就证明了没有发送。
func TestAPIForceSyncCollRejected(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 5
	stub.versions[5] = []x.WatchInfo{wi("db1", "A")}

	req := &syncCollectionReq{Collections: []x.WatchInfo{wi("db9", "Z")}}
	done := make(chan *syncWcRes, 1)
	go func() {
		done <- (&suite{}).APIForceSyncColl(context.Background(), req)
	}()

	select {
	case res := <-done:
		if res == nil || res.Succeed {
			t.Fatalf("res = %+v, want Succeed=false", res)
		}
	case <-time.After(time.Second):
		t.Fatal("不应发送 SyncTaskDelta（无接收方时会阻塞）")
	}
}

// etl.IsInWatchCollection 对空 checking 恒返回 true，
// 所以空 Collections 也会走到发送 Delta 的分支。此处固化该行为。
func TestAPIForceSyncCollEmptyCollections(t *testing.T) {
	installWatchCollectionStub(t)

	got := startDeltaReceiver(t)
	res := (&suite{}).APIForceSyncColl(context.Background(), &syncCollectionReq{})

	if res == nil || !res.Succeed {
		t.Fatalf("res = %+v, want Succeed=true", res)
	}

	select {
	case d := <-got:
		if len(d.Add) != 0 {
			t.Errorf("Delta.Add = %+v, 应为空", d.Add)
		}
		if len(d.Del) != 0 {
			t.Errorf("Delta.Del = %+v, 应为空", d.Del)
		}
	case <-time.After(time.Second):
		t.Fatal("没有收到 SyncTaskDelta")
	}
}

// syncCollectionReq 的字段名是协议的一部分
func TestSyncCollectionReqJSON(t *testing.T) {
	var req syncCollectionReq
	if err := json.Unmarshal([]byte(`{"colls":[{"DB":"db1","Collection":"c1"}]}`), &req); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	want := []x.WatchInfo{wi("db1", "c1")}
	if !reflect.DeepEqual(req.Collections, want) {
		t.Fatalf("Collections = %+v, want %+v", req.Collections, want)
	}
}

// syncWcRes 没有 json tag，序列化后字段名是 "Succeed"。此处固化现状。
func TestSyncWcResJSON(t *testing.T) {
	bs, err := json.Marshal(&syncWcRes{Succeed: true})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if want := `{"Succeed":true}`; string(bs) != want {
		t.Fatalf("JSON = %s, want %s", bs, want)
	}
}

// ---------------------------------------------------------------------------
// ctx 已取消 + 无接收方：handler 必须立即返回 Succeed=false，不得挂死。
// ---------------------------------------------------------------------------
func TestAPIForceSyncCollRespectsCanceledCtx(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 5
	stub.versions[5] = []x.WatchInfo{wi("db1", "A")}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// 故意不启动 startDeltaReceiver：updateSyncTaskChan 无缓冲且无接收方。
	done := make(chan *syncWcRes, 1)
	go func() {
		done <- (&suite{}).APIForceSyncColl(ctx, &syncCollectionReq{
			Collections: []x.WatchInfo{wi("db1", "A")},
		})
	}()

	select {
	case res := <-done:
		if res == nil || res.Succeed {
			t.Fatalf("ctx 已取消应返回 Succeed=false, got %+v", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ctx 已取消但 handler 仍阻塞在无缓冲的 SyncTaskUpdater send 上")
	}
}

// ---------------------------------------------------------------------------
// ctx 在等待期间取消：先让 delta 接收方就位但不取走，用短 deadline 触发取消。
// 与上一个用例互补，覆盖「两个 case 都不 ready → 之后 ctx.Done() 变 ready」。
// ---------------------------------------------------------------------------
func TestAPIForceSyncCollRespectsCtxDeadline(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 5
	stub.versions[5] = []x.WatchInfo{wi("db1", "A")}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	done := make(chan *syncWcRes, 1)
	go func() {
		done <- (&suite{}).APIForceSyncColl(ctx, &syncCollectionReq{
			Collections: []x.WatchInfo{wi("db1", "A")},
		})
	}()

	select {
	case res := <-done:
		if res == nil || res.Succeed {
			t.Fatalf("ctx 超时后应返回 Succeed=false, got %+v", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ctx 超时后 handler 仍未返回")
	}
}
