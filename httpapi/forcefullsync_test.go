package httpapi

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xpwu/go-tinyserver/api"
)

// PostForceFullSync 是 buffered(1)+default 非阻塞；
// 连续调用不能把 handler 卡住，且每次都要返回非 nil 的响应。
func TestAPIForceFullSync(t *testing.T) {
	s := &suite{}
	var last *api.EmptyResponse

	done := make(chan struct{})
	go func() {
		for i := 0; i < 5; i++ {
			last = s.APIForceFullSync(context.Background(), &fullSyncReq{})
		}
		close(done)
	}()

	select {
	case <-done:
		if last == nil {
			t.Fatal("返回值不应为 nil")
		}
	case <-time.After(time.Second):
		t.Fatal("连续调用 APIForceFullSync 出现阻塞")
	}
}

// fullSyncReq 是空结构体，框架把 {} 反序列化后应得到可用实例
func TestFullSyncReqJSON(t *testing.T) {
	var req fullSyncReq
	if err := json.Unmarshal([]byte(`{}`), &req); err != nil {
		t.Fatalf("Unmarshal({}) failed: %v", err)
	}
}

// handler 完全不读取 request，所以传 nil 也不会 panic
// （go-tinyserver 运行时永远通过反射传入非 nil 指针）。
// 参数类型本身由 suit_test.go 的 TestAPIMethodSignatures 钉住。
func TestAPIForceFullSyncIgnoresRequest(t *testing.T) {
	if res := (&suite{}).APIForceFullSync(context.Background(), nil); res == nil {
		t.Fatal("返回值不应为 nil")
	}
}
