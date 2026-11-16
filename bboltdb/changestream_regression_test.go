package bboltdb

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/xpwu/ETLer/etl/db"
	"go.etcd.io/bbolt"
)

// newSmallMmapChangeStream 故意不设 InitialMmapSize。
//
// db_test.go 的 newTestDB 设了 InitialMmapSize: 64<<20，把 bbolt 的 mmap 扩容
// 分支整个屏蔽了；而生产 New() 只有 Timeout，默认初始 mmap 很小，第一次写就会
// 触发 grow。单测全绿、e2e 卡死正是被这个配置差异掩盖的。
// 所以下面所有"迭代期间写"的回归用例都必须用这个 helper，不能用 newTestDB。
func newSmallMmapChangeStream(t *testing.T) (*ChangeStream, *bbolt.DB) {
	t.Helper()

	dir := t.TempDir()
	underlying, err := bbolt.Open(filepath.Join(dir, "small.db"), 0600, &bbolt.Options{
		NoSync:         true,
		NoFreelistSync: true,
		// 关键：不设 InitialMmapSize，让 db.mmap() 的 grow 分支保持可达
	})
	if err != nil {
		t.Fatalf("open bbolt failed: %v", err)
	}

	// 死锁场景下 Close() 会等 mmaplock.RLock，而排队的 writer 会抢先阻塞它，
	// 直接 Close 会让 cleanup 永久挂住。带超时放弃，进程退出时由 OS 回收。
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
	return cs, underlying
}

// saveRaw 不带 *testing.T，可在 goroutine 里安全调用
// （mustSave 里有 t.Fatalf，只能跑在测试 goroutine）。
func saveRaw(cs *ChangeStream, token, value string) (db.StreamId, error) {
	return cs.Save(context.Background(),
		db.ResumeToken([]byte(token)),
		db.StreamValue([]byte(value)))
}

// ---------------------------------------------------------------------------
// 回归 1：Next() 返回 true 后立刻执行写事务，不得死锁
//
// 调用方 etl/task/sender.go:262-278 的真实顺序是：
//
//	for iter.Next(ctx, batch) {
//	    netLayer.Send(...)
//	    db.ChangeStream().MarkSentUpTo(ctx, iter.LastStreamId())   // ← 循环内写
//	}
//
// 旧实现把读事务一直挂到"下一次 Next / Release"，写事务 Commit 触发
// bbolt db.mmap() 取 mmaplock.Lock()，与读事务的 RLock 互等；且 db.Update
// 不响应 ctx，Sender.Stop() 也救不回来 —— 真实链路表现为永久停摆。
// 修复后迭代器每次 Next 用短事务读完即关，循环内任意写都合法。
// ---------------------------------------------------------------------------
func TestChangeStreamIterAllowsWriteWhileNextIsTrue(t *testing.T) {
	cs, _ := newSmallMmapChangeStream(t)
	ctx := context.Background()

	// lastsent 必须先存在，否则 AllNotSent 直接返回 ErrNotFoundSentPoint
	if _, err := saveRaw(cs, "t1", ""); err != nil { // id=1: nil value 占位
		t.Fatalf("Save(seed) failed: %v", err)
	}
	cs.MarkSentUpTo(ctx, csUint64ToBytes(1))
	if _, err := saveRaw(cs, "t2", "v2"); err != nil {
		t.Fatalf("Save(v2) failed: %v", err)
	}

	it := cs.AllNotSent(ctx)
	defer it.Release(ctx)

	if !it.Next(ctx, 1) {
		t.Fatalf("Next 应为 true, err = %v", it.Err())
	}
	if len(it.Values()) != 1 || string(it.Values()[0]) != "v2" {
		t.Fatalf("Values() = %q, want [v2]", it.Values())
	}
	last := it.LastStreamId()
	if csBytesToUint64(last) != 2 {
		t.Fatalf("LastStreamId = %d, want 2", csBytesToUint64(last))
	}

	// 此刻若迭代器仍持有读事务，下面的写事务就会卡在 mmaplock。
	// 放 goroutine + 超时，避免用例本身挂死。
	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		defer close(done)

		cs.MarkSentUpTo(ctx, last) // 复刻 sender 循环内那一句

		// 再写一批数据，确保至少一次 mmap grow 发生在"迭代期间"。
		// 只 MarkSentUpTo 一次未必能顶到当前 mmap 边界，多写几条才稳。
		for i := 0; i < 500; i++ {
			if _, err := saveRaw(cs,
				fmt.Sprintf("t%d", i+3), fmt.Sprintf("v%d", i+3)); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}()

	select {
	case <-done:
		select {
		case err := <-errCh:
			t.Fatalf("迭代期间 Save failed: %v", err)
		default:
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Next() 返回 true 后的写操作死锁：迭代器仍持有 bbolt 读事务（mmaplock.Lock 与读事务 RLock 互等）")
	}

	if v, ok := readLastSent(t, cs); !ok || v != 2 {
		t.Fatalf("lastsent = %d (ok=%v), want 2", v, ok)
	}
}

// ---------------------------------------------------------------------------
// 回归 2：复刻 sender 的完整循环 —— 每批 Next 成功后立刻 MarkSentUpTo
//
// 与回归 1 互补：1 逼出 grow，2 验证多批迭代下"每批后写"不会让后续
// Next 拿错批次，也不会在中途卡死。
// ---------------------------------------------------------------------------
func TestChangeStreamIterMarkSentUpToAfterEveryBatch(t *testing.T) {
	cs, _ := newSmallMmapChangeStream(t)
	ctx := context.Background()

	if _, err := saveRaw(cs, "t1", ""); err != nil {
		t.Fatalf("Save(seed) failed: %v", err)
	}
	cs.MarkSentUpTo(ctx, csUint64ToBytes(1))

	const total = 6
	for i := 0; i < total; i++ {
		if _, err := saveRaw(cs,
			fmt.Sprintf("t%d", i+2), fmt.Sprintf("v%d", i+2)); err != nil {
			t.Fatalf("Save(%d) failed: %v", i+2, err)
		}
	}

	done := make(chan struct{})
	batches := make(chan int, 1)
	go func() {
		defer close(done)

		it := cs.AllNotSent(ctx)
		defer it.Release(ctx)

		n := 0
		for it.Next(ctx, 2) {
			if len(it.Values()) == 0 {
				return
			}
			// 每批发送成功后立刻标记，等价于 sender.go:277
			cs.MarkSentUpTo(ctx, it.LastStreamId())
			n++
			if n > total {
				return
			}
		}
		batches <- n
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("多批迭代期间 MarkSentUpTo 死锁（旧实现的长读事务）")
	}

	select {
	case n := <-batches:
		if n != 3 {
			t.Fatalf("批次数 = %d, want 3 (6 条 / 每批 2)", n)
		}
	default:
		t.Fatal("迭代循环没有正常结束")
	}

	if v, ok := readLastSent(t, cs); !ok || v != 7 {
		t.Fatalf("lastsent = %d (ok=%v), want 7", v, ok)
	}
}

// ---------------------------------------------------------------------------
// 契约：迭代器/ResumeToken 返回的数据必须是独立副本
//
// 修复把 csDecodeEntry 改成 make+copy。原来的 resumeToken/value 直接切自
// bbolt 的 mmap，读事务一关就是悬空引用；现有 TestCsEntryEncodeDecode 只
// 验证 bytes.Equal，抓不到这一点。
// ---------------------------------------------------------------------------
func TestCsDecodeEntryReturnsIndependentCopy(t *testing.T) {
	buf := csEncodeEntry(&csEntry{
		resumeToken: db.ResumeToken([]byte("tok")),
		value:       db.StreamValue([]byte("val")),
	})
	got := csDecodeEntry(buf)
	if got == nil {
		t.Fatal("csDecodeEntry returned nil")
	}

	// 篡改原始 buffer，decode 结果不应受影响
	buf[4] = 'X'
	buf[7] = 'X'

	if !bytes.Equal(got.resumeToken, []byte("tok")) {
		t.Fatalf("resumeToken 指向了输入 buffer: %q", got.resumeToken)
	}
	if !bytes.Equal(got.value, []byte("val")) {
		t.Fatalf("value 指向了输入 buffer: %q", got.value)
	}
}
