package httpapi

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xpwu/go-tinyserver/api"
)

func TestAPIClearWC(t *testing.T) {
	stub := installWatchCollectionStub(t)

	res := (&suite{}).APIClearWC(context.Background(), &clearReq{})

	if res == nil {
		t.Fatal("返回值不应为 nil")
	}
	if n := stub.ClearCalls(); n != 1 {
		t.Fatalf("Clear 调用次数 = %d, want 1", n)
	}
}

// 清除之后必须通知两侧（etl.WatchCollectionUpdated），
// 它走 buffered(1)+default，连续调用不能阻塞。
func TestAPIClearWCDoesNotBlock(t *testing.T) {
	installWatchCollectionStub(t)

	done := make(chan struct{})
	go func() {
		s := &suite{}
		for i := 0; i < 5; i++ {
			s.APIClearWC(context.Background(), &clearReq{})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("连续调用 APIClearWC 出现阻塞")
	}
}

// log.WithCtx(ctx) 不能丢掉上游 ctx 的 value
func TestAPIClearWCPassesContext(t *testing.T) {
	type key struct{}
	stub := installWatchCollectionStub(t)

	(&suite{}).APIClearWC(context.WithValue(context.Background(), key{}, "v"), &clearReq{})

	ctxs := stub.GotCtxs()
	if len(ctxs) != 1 {
		t.Fatalf("ctx 记录数 = %d, want 1", len(ctxs))
	}
	if got := ctxs[0].Value(key{}); got != "v" {
		t.Fatalf("ctx.Value = %v, want %q", got, "v")
	}
}

func TestEmptyResponseJSON(t *testing.T) {
	bs, err := json.Marshal(&api.EmptyResponse{})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if string(bs) != "{}" {
		t.Fatalf("JSON = %s, want {}", bs)
	}
}
