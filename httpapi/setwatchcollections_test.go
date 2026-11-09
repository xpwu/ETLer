package httpapi

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/xpwu/ETLer/x"
)

// 生效条件 old != version && now == version 的四种组合都要跑到。
// 注意：etl.WatchCollectionUpdated() 往 task / changestream 两个 buffered(1) 通道发信号，
// 那两个 channel 都未导出，本包无法直接断言"信号是否投递"；
// 所以这里验证的是前置条件（Save 的返回值与入参）、返回结构、以及 handler 不阻塞。
// 信号投递本身由 etl/task 与 etl/changestream 各自包内的通道用例覆盖。
func TestAPISetWatchCols(t *testing.T) {
	cases := []struct {
		name       string
		reqVersion uint64
		old        uint64
		now        uint64
	}{
		{"生效_旧版本落后且设置成功", 3, 2, 3},
		{"不生效_已是该版本", 3, 3, 3},
		{"不生效_设置未成功", 3, 2, 2},
		{"不生效_旧版本等于请求但now不同", 3, 3, 4},
		{"生效_从0到1", 1, 0, 1},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stub := installWatchCollectionStub(t)
			stub.oldVersion = c.old
			stub.nowVersion = c.now

			req := &setWcReq{
				Version:    c.reqVersion,
				WatchInfos: []x.WatchInfo{wi("db1", "A"), wi("db2", "B")},
			}
			res := (&suite{}).APISetWatchCols(context.Background(), req)

			if res == nil {
				t.Fatal("返回值不应为 nil")
			}
			if res.OldVersion != c.old || res.NowVersion != c.now {
				t.Fatalf("res = %+v, want {OldVersion:%d NowVersion:%d}", res, c.old, c.now)
			}

			calls := stub.SaveCalls()
			if len(calls) != 1 {
				t.Fatalf("Save 调用次数 = %d, want 1", len(calls))
			}
			if calls[0].version != c.reqVersion {
				t.Errorf("Save 的 version = %d, want %d", calls[0].version, c.reqVersion)
			}
			if !reflect.DeepEqual(calls[0].infos, req.WatchInfos) {
				t.Errorf("Save 的 WatchInfos = %+v, want %+v", calls[0].infos, req.WatchInfos)
			}
		})
	}
}

// 空集合 / nil 集合也必须原样传给 Save，不能被替换成默认值
func TestAPISetWatchColsEmptyInfos(t *testing.T) {
	cases := []struct {
		name  string
		infos []x.WatchInfo
	}{
		{"nil", nil},
		{"空切片", []x.WatchInfo{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stub := installWatchCollectionStub(t)
			stub.oldVersion, stub.nowVersion = 0, 1

			res := (&suite{}).APISetWatchCols(context.Background(), &setWcReq{Version: 1, WatchInfos: c.infos})
			if res.OldVersion != 0 || res.NowVersion != 1 {
				t.Fatalf("res = %+v", res)
			}

			calls := stub.SaveCalls()
			if len(calls) != 1 {
				t.Fatalf("Save 调用次数 = %d, want 1", len(calls))
			}
			if len(calls[0].infos) != 0 {
				t.Fatalf("Save 的 WatchInfos = %+v, 应为空", calls[0].infos)
			}
		})
	}
}

// WatchCollectionUpdated 是 select+default 非阻塞，连续调用不能把 handler 卡住
func TestAPISetWatchColsDoesNotBlock(t *testing.T) {
	stub := installWatchCollectionStub(t)
	stub.oldVersion, stub.nowVersion = 1, 2

	done := make(chan struct{})
	go func() {
		s := &suite{}
		for i := 0; i < 5; i++ {
			s.APISetWatchCols(context.Background(), &setWcReq{Version: 2})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("连续调用 APISetWatchCols 出现阻塞")
	}
}

// setWcRes 曾经返回的是空结构体，oldver/nowver 全丢。
// 这里把字段名、json tag、以及值一起钉住。
func TestSetWcResJSON(t *testing.T) {
	bs, err := json.Marshal(&setWcRes{OldVersion: 1, NowVersion: 2})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if want := `{"oldver":1,"nowver":2}`; string(bs) != want {
		t.Fatalf("JSON = %s, want %s", bs, want)
	}
}
