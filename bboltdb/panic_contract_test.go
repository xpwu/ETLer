package bboltdb

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpwu/ETLer/etl/db"
	"go.etcd.io/bbolt"
)

// 落盘位置：<repo>/bboltdb/panic_contract_test.go（新建，同包）
// 本文件自带 requirePanicCS，不依赖 db_failure_test.go 的 helper，可单独落盘。
//
// 钉住产品侧新契约「所有 DBer 接口出现服务不可运行的异常，直接 panic」，
// 以及迭代器的【例外】：ChangeStreamIter.Next 的 View 失败走 it.err → Err()，
// 不 panic。后者是 sender 的 UnknownErr → 5s 重试兜底所依赖的，
// 不能为了「统一 panic」而改掉。


func requirePanicCS(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("%s: DB 事务失败应 panic，实际静默返回", name)
		}
	}()
	fn()
}

func newClosedChangeStream(t *testing.T) *ChangeStream {
	t.Helper()

	dir := t.TempDir()
	underlying, err := bbolt.Open(filepath.Join(dir, "panic.db"), 0600, &bbolt.Options{
		NoSync:         true,
		NoFreelistSync: true,
		// 不设 InitialMmapSize，与生产 New() 的 grow 路径一致
	})
	if err != nil {
		t.Fatalf("open bbolt failed: %v", err)
	}
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { _ = underlying.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})

	cs, err := NewChangeStream(context.Background(), &DB{Dir: dir, Underlying: underlying})
	if err != nil {
		t.Fatalf("NewChangeStream failed: %v", err)
	}
	if err := underlying.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	return cs
}

// ---------------------------------------------------------------------------
// ChangeStream 六个公开方法在底层 DB 关闭后都必须 panic。
// 覆盖上一轮列的 DeleteAll 失败 → ErrTokenExpired 死循环、ResumeToken/LastStreamId
// 读失败 → 静默零值等路径。
// ---------------------------------------------------------------------------
func TestChangeStreamMethodsPanicOnClosedDB(t *testing.T) {
	cs := newClosedChangeStream(t)
	ctx := context.Background()

	requirePanicCS(t, "ChangeStream.Save", func() {
		cs.Save(ctx, db.ResumeToken([]byte("tok")), db.StreamValue([]byte("v")))
	})
	requirePanicCS(t, "ChangeStream.MarkSentUpTo", func() {
		cs.MarkSentUpTo(ctx, csUint64ToBytes(1))
	})
	requirePanicCS(t, "ChangeStream.LastStreamId", func() { _ = cs.LastStreamId() })
	requirePanicCS(t, "ChangeStream.ResumeToken", func() { _ = cs.ResumeToken(ctx) })
	requirePanicCS(t, "ChangeStream.DeleteAll", func() { cs.DeleteAll(ctx) })
	requirePanicCS(t, "ChangeStream.Compact", func() { cs.Compact(ctx) })
}

// ---------------------------------------------------------------------------
// 例外（重要护栏）：迭代器不 panic，View 失败经 Err() 上抛。
//
// AllNotSent() 只是 new 一个结构体不开事务；真正的 View 在 Next() 里，
// 失败时填 it.err 并返回 false。sender.sendChangeStream() 返回 iter.Err()
// → Sender.Start() 映射为 UnknownErr → scheduler 5s 后 sender.Start()。
// 若有人把这里也改成 panic，本用例 FAIL，提示不要误"统一"。
// ---------------------------------------------------------------------------
func TestChangeStreamIterNextReportsErrWithoutPanic(t *testing.T) {
	cs := newClosedChangeStream(t)
	ctx := context.Background()

	it := cs.AllNotSent(ctx)
	defer it.Release(ctx)

	panicked := false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		if it.Next(ctx, 10) {
			t.Error("底层 DB 已关闭，Next 应返回 false")
		}
	}()

	if panicked {
		t.Fatal("迭代器 View 失败不应 panic，应通过 Err() 上抛")
	}
	if it.Err() == nil {
		t.Fatal("底层 DB 已关闭，Next 返回 false 后 Err() 应带错误")
	}
}

// ---------------------------------------------------------------------------
// 业务哨兵不是 DB 故障：缺 lastsent 时返回 ErrNotFoundSentPoint，不得 panic。
// 这是 sender → NeedForceSync → fullSyncTaskify 的正常握手路径，
// 若被误升级成 panic 会破坏首次启动的全量同步。
// ---------------------------------------------------------------------------
func TestChangeStreamIterNotFoundSentPointIsNotPanic(t *testing.T) {
	dir := t.TempDir()
	underlying, err := bbolt.Open(filepath.Join(dir, "sent.db"), 0600, &bbolt.Options{
		NoSync: true, NoFreelistSync: true,
	})
	if err != nil {
		t.Fatalf("open bbolt failed: %v", err)
	}
	t.Cleanup(func() { _ = underlying.Close() })

	cs, err := NewChangeStream(context.Background(), &DB{Dir: dir, Underlying: underlying})
	if err != nil {
		t.Fatalf("NewChangeStream failed: %v", err)
	}
	// 写一条有效数据但不设 lastsent
	cs.Save(context.Background(),
		db.ResumeToken([]byte("tok")), db.StreamValue([]byte("v")))

	it := cs.AllNotSent(context.Background())
	defer it.Release(context.Background())

	if it.Next(context.Background(), 10) {
		t.Fatal("缺 lastsent 时 Next 应返回 false")
	}
	if it.Err() != db.ErrNotFoundSentPoint {
		t.Fatalf("Err() = %v, want ErrNotFoundSentPoint", it.Err())
	}
}
