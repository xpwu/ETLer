package httpapi

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xpwu/ETLer/x"
)

func TestAPIGetWatchCols(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 7
	stub.versions[7] = []x.WatchInfo{wi("db1", "A"), wi("db2", "B")}
	stub.versions[6] = []x.WatchInfo{wi("db1", "OLD")}

	res := (&suite{}).APIGetWatchCols(context.Background(), &getWcReq{})

	if res == nil {
		t.Fatal("返回值不应为 nil")
	}
	if res.Version != 7 {
		t.Errorf("Version = %d, want 7", res.Version)
	}
	want := []x.WatchInfo{wi("db1", "A"), wi("db2", "B")}
	if !reflect.DeepEqual(res.WatchInfos, want) {
		t.Fatalf("WatchInfos = %+v, want %+v", res.WatchInfos, want)
	}

	// Get 必须用 LatestVersion 返回的那个版本号，不能读旧版本
	calls := stub.GetCalls()
	if len(calls) != 1 || calls[0] != 7 {
		t.Fatalf("Get 调用 = %v, want [7]", calls)
	}
}

func TestAPIGetWatchColsEmpty(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 0
	stub.versions[0] = nil

	res := (&suite{}).APIGetWatchCols(context.Background(), &getWcReq{})

	if res == nil {
		t.Fatal("返回值不应为 nil")
	}
	if res.Version != 0 {
		t.Errorf("Version = %d, want 0", res.Version)
	}
	if len(res.WatchInfos) != 0 {
		t.Errorf("WatchInfos = %+v, 应为空", res.WatchInfos)
	}
}

// 版本切换后必须读到新版本那一份
func TestAPIGetWatchColsFollowsLatestVersion(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.versions[1] = []x.WatchInfo{wi("db1", "v1")}
	stub.versions[2] = []x.WatchInfo{wi("db1", "v2")}
	s := &suite{}

	stub.latestVersion = 1
	if r := s.APIGetWatchCols(context.Background(), &getWcReq{}); r.Version != 1 || len(r.WatchInfos) != 1 || r.WatchInfos[0].Collection != "v1" {
		t.Fatalf("v1 = %+v", r)
	}

	stub.latestVersion = 2
	if r := s.APIGetWatchCols(context.Background(), &getWcReq{}); r.Version != 2 || len(r.WatchInfos) != 1 || r.WatchInfos[0].Collection != "v2" {
		t.Fatalf("v2 = %+v", r)
	}
}

// log.WithCtx(ctx) 不能丢掉上游 ctx 的 value
func TestAPIGetWatchColsPassesContext(t *testing.T) {
	type key struct{}
	stub := installWatchCollectionStub(t)
	stub.latestVersion = 1

	(&suite{}).APIGetWatchCols(context.WithValue(context.Background(), key{}, "v"), &getWcReq{})

	ctxs := stub.GotCtxs()
	if len(ctxs) != 2 { // LatestVersion + Get
		t.Fatalf("ctx 记录数 = %d, want 2", len(ctxs))
	}
	for i, c := range ctxs {
		if got := c.Value(key{}); got != "v" {
			t.Errorf("ctxs[%d].Value = %v, want %q", i, got, "v")
		}
	}
}

// x.WatchInfo 没有 json tag，所以序列化后字段名是 "DB" / "Collection"。
// 此处固化现状：若将来给它加了 tag，本用例会失败并提醒同步核对协议。
func TestGetWcResJSON(t *testing.T) {
	bs, err := json.Marshal(&getWcRes{
		Version:    3,
		WatchInfos: []x.WatchInfo{wi("db1", "c1")},
	})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	want := `{"version":3,"watchInfos":[{"DB":"db1","Collection":"c1"}]}`
	if string(bs) != want {
		t.Fatalf("JSON = %s, want %s", bs, want)
	}
}
