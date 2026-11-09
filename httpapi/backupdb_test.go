package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/xpwu/ETLer/etl/db"
)

func TestAPIBackupDBSuccess(t *testing.T) {
	worker := installBackupWorkerStub(t, "backup/2026-11-09.db", nil)

	res := (&suite{}).APIBackupDB(context.Background(), &backupReq{})

	if res == nil {
		t.Fatal("返回值不应为 nil")
	}
	if res.FileName != "backup/2026-11-09.db" {
		t.Fatalf("FileName = %q, want %q", res.FileName, "backup/2026-11-09.db")
	}
	if n := worker.Calls(); n != 1 {
		t.Fatalf("Backup 调用次数 = %d, want 1", n)
	}
}

// 备份失败时必须返回空文件名；即使 worker 同时返回了非空 path，也不能透出去
func TestAPIBackupDBFailure(t *testing.T) {
	wantErr := errors.New("disk full")
	worker := installBackupWorkerStub(t, "should-not-be-returned", wantErr)

	res := (&suite{}).APIBackupDB(context.Background(), &backupReq{})

	if res == nil {
		t.Fatal("返回值不应为 nil")
	}
	if res.FileName != "" {
		t.Fatalf("失败时 FileName 应为 %q, got %q", "", res.FileName)
	}
	if n := worker.Calls(); n != 1 {
		t.Fatalf("Backup 调用次数 = %d, want 1", n)
	}
}

// handler 把 log.WithCtx(ctx) 生成的 ctx 传给 worker，不能丢掉上游的 value
func TestAPIBackupDBPassesContext(t *testing.T) {
	type key struct{}
	worker := installBackupWorkerStub(t, "ok.db", nil)

	(&suite{}).APIBackupDB(context.WithValue(context.Background(), key{}, "v"), &backupReq{})

	ctxs := worker.GotCtxs()
	if len(ctxs) != 1 {
		t.Fatalf("ctx 记录数 = %d, want 1", len(ctxs))
	}
	if got := ctxs[0].Value(key{}); got != "v" {
		t.Fatalf("ctx.Value = %v, want %q", got, "v")
	}
}

// 接口的 JSON 字段名是协议的一部分
func TestBackupResJSON(t *testing.T) {
	bs, err := json.Marshal(&backupRes{FileName: "a.db"})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if want := `{"file_name":"a.db"}`; string(bs) != want {
		t.Fatalf("JSON = %s, want %s", bs, want)
	}
}

// db.GetBackupWorker 的存取是全局单例，Setter/Getter 必须成对可用
func TestBackupWorkerGlobal(t *testing.T) {
	worker := &stubBackupWorker{path: "x.db"}
	old := db.GetBackupWorker()
	t.Cleanup(func() {
		if old == nil {
			db.SetBackupWorker(&stubBackupWorker{})
		} else {
			db.SetBackupWorker(old)
		}
	})
	db.SetBackupWorker(worker)

	if db.GetBackupWorker() != worker {
		t.Fatal("GetBackupWorker 应返回 SetBackupWorker 传入的实例")
	}
}
