package task

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/xpwu/ETLer/etl/config"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// capturedRequest 是 Request 序列化后需要校验的字段。
// Request.T 是 byte 别名，JSON 里是数字；Data 保留原始 JSON，
// 与本地 json.Marshal([]bson.Raw) 的结果比对，避免依赖 bson.Raw 的具体编码形式
type capturedRequest struct {
	T    int
	Ns   struct{ DB, Coll string }
	Data json.RawMessage
}

type recordingServer struct {
	mu       sync.Mutex
	requests []capturedRequest
	status   int
	srv      *httptest.Server
}

func newRecordingServer(t *testing.T, status int) *recordingServer {
	t.Helper()

	rec := &recordingServer{status: status}
	rec.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var req capturedRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("unmarshal body %q: %v", body, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		rec.mu.Lock()
		rec.requests = append(rec.requests, req)
		rec.mu.Unlock()

		w.WriteHeader(rec.status)
	}))
	t.Cleanup(rec.srv.Close)

	return rec
}

func (r *recordingServer) URL() string { return r.srv.URL }

func (r *recordingServer) Requests() []capturedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]capturedRequest(nil), r.requests...)
}

// config.Watch.SendToUrls 是全局配置，每个用例都要设置并在结束后恢复
func setSendToUrls(t *testing.T, urls ...string) {
	t.Helper()
	old := config.Watch.SendToUrls
	config.Watch.SendToUrls = urls
	t.Cleanup(func() { config.Watch.SendToUrls = old })
}

func assertDataJSON(t *testing.T, got json.RawMessage, want []bson.Raw) {
	t.Helper()

	var arr []json.RawMessage
	if err := json.Unmarshal(got, &arr); err != nil {
		t.Fatalf("Data 不是数组: %s (%v)", got, err)
	}
	if len(arr) != len(want) {
		t.Fatalf("Data 元素数 = %d, want %d (%s)", len(arr), len(want), got)
	}

	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal(want) failed: %v", err)
	}
	if string(got) != string(wantJSON) {
		t.Fatalf("Data = %s, want %s", got, wantJSON)
	}
}

func TestRequestJSONShape(t *testing.T) {
	r := &Request{
		T:    Sync,
		Ns:   ns{DB: "db1", Coll: "c1"},
		Data: []bson.Raw{bson.Raw([]byte("d1")), bson.Raw([]byte("d2"))},
	}

	bs, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Marshal(Request) failed: %v", err)
	}

	var got capturedRequest
	if err := json.Unmarshal(bs, &got); err != nil {
		t.Fatalf("Unmarshal(%q) failed: %v", bs, err)
	}
	if got.T != int(Sync) {
		t.Errorf("T = %d, want %d", got.T, Sync)
	}
	if got.Ns.DB != "db1" || got.Ns.Coll != "c1" {
		t.Errorf("Ns = %+v, want {db1 c1}", got.Ns)
	}
	assertDataJSON(t, got.Data, r.Data)

	// Response 目前是空结构，必须能稳定编码为 {}
	rbs, err := json.Marshal(&Response{})
	if err != nil || string(rbs) != "{}" {
		t.Fatalf("Marshal(Response) = %q, %v, want {}", rbs, err)
	}
}

func TestHttpNetLayerSendSuccess(t *testing.T) {
	rec := newRecordingServer(t, http.StatusOK)
	setSendToUrls(t, rec.URL())

	raws := []bson.Raw{bson.Raw([]byte("a")), bson.Raw([]byte("b"))}
	if err := (&HttpNetLayer{}).Send(context.Background(), Sync, "db1", "c1", raws); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	reqs := rec.Requests()
	if len(reqs) != 1 {
		t.Fatalf("请求数 = %d, want 1", len(reqs))
	}
	if reqs[0].T != int(Sync) {
		t.Errorf("T = %d, want %d", reqs[0].T, Sync)
	}
	if reqs[0].Ns.DB != "db1" || reqs[0].Ns.Coll != "c1" {
		t.Errorf("Ns = %+v, want {db1 c1}", reqs[0].Ns)
	}
	assertDataJSON(t, reqs[0].Data, raws)
}

// ChangeStream 的 Ns 必须是空串，这是处理服务器区分两类数据的约定
func TestHttpNetLayerSendChangeStreamNsEmpty(t *testing.T) {
	rec := newRecordingServer(t, http.StatusOK)
	setSendToUrls(t, rec.URL())

	if err := (&HttpNetLayer{}).Send(context.Background(), ChangeStream, "", "",
		[]bson.Raw{bson.Raw([]byte("e1"))}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	reqs := rec.Requests()
	if len(reqs) != 1 {
		t.Fatalf("请求数 = %d, want 1", len(reqs))
	}
	if reqs[0].T != int(ChangeStream) {
		t.Errorf("T = %d, want %d", reqs[0].T, ChangeStream)
	}
	if reqs[0].Ns.DB != "" || reqs[0].Ns.Coll != "" {
		t.Errorf("Ns = %+v, 应当为空", reqs[0].Ns)
	}
}

// 第一个成功后立即返回，不会继续请求后面的地址
func TestHttpNetLayerSendStopsOnFirstSuccess(t *testing.T) {
	first := newRecordingServer(t, http.StatusOK)
	second := newRecordingServer(t, http.StatusOK)
	setSendToUrls(t, first.URL(), second.URL())

	if err := (&HttpNetLayer{}).Send(context.Background(), Sync, "db", "c", nil); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if len(first.Requests()) != 1 {
		t.Fatalf("第一个地址请求数 = %d, want 1", len(first.Requests()))
	}
	if len(second.Requests()) != 0 {
		t.Fatalf("第二个地址不应被请求, got %d", len(second.Requests()))
	}
}

// 第一个地址失败后按配置顺序尝试第二个
func TestHttpNetLayerSendFallback(t *testing.T) {
	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := closed.URL
	closed.Close() // 关闭后该地址必然连接失败

	good := newRecordingServer(t, http.StatusOK)
	setSendToUrls(t, closedURL, good.URL())

	if err := (&HttpNetLayer{}).Send(context.Background(), Sync, "db", "c",
		[]bson.Raw{bson.Raw([]byte("x"))}); err != nil {
		t.Fatalf("第二个地址成功时应返回 nil, got %v", err)
	}
	if len(good.Requests()) != 1 {
		t.Fatalf("第二个地址请求数 = %d, want 1", len(good.Requests()))
	}
}

// 非 200 视为失败，全部失败后返回 ErrSendFailed
func TestHttpNetLayerSendAllFailed(t *testing.T) {
	a := newRecordingServer(t, http.StatusInternalServerError)
	b := newRecordingServer(t, http.StatusServiceUnavailable)
	setSendToUrls(t, a.URL(), b.URL())

	err := (&HttpNetLayer{}).Send(context.Background(), Sync, "db", "c", nil)
	if !errors.Is(err, ErrSendFailed) {
		t.Fatalf("err = %v, want ErrSendFailed", err)
	}
	if len(a.Requests()) != 1 || len(b.Requests()) != 1 {
		t.Fatalf("应依次尝试两个地址, got %d / %d", len(a.Requests()), len(b.Requests()))
	}
}

func TestHttpNetLayerSendNoUrls(t *testing.T) {
	setSendToUrls(t) // 空列表

	err := (&HttpNetLayer{}).Send(context.Background(), Sync, "db", "c", nil)
	if !errors.Is(err, ErrSendFailed) {
		t.Fatalf("err = %v, want ErrSendFailed", err)
	}
}

func TestHttpNetLayerSendCanceledContext(t *testing.T) {
	rec := newRecordingServer(t, http.StatusOK)
	setSendToUrls(t, rec.URL())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := (&HttpNetLayer{}).Send(ctx, ChangeStream, "", "", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// 说明：url.Error.Timeout() 触发的"等待 1s 后对同一地址重发一次"分支，
// 需要服务端挂起超过 10s 才能稳定构造，单测里不覆盖该分支。
