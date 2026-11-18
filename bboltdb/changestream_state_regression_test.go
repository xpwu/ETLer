package bboltdb

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xpwu/ETLer/etl/db"
	"go.etcd.io/bbolt"
)

func stateOpenCS(t *testing.T) (*ChangeStream, *bbolt.DB) {
	t.Helper()
	dir := t.TempDir()
	underlying, err := bbolt.Open(filepath.Join(dir, "state.db"), 0600, &bbolt.Options{
		NoSync:         true,
		NoFreelistSync: true,
		// 不设 InitialMmapSize：保持 mmap grow 分支可达
	})
	if err != nil {
		t.Fatalf("open bbolt failed: %v", err)
	}

	// 死锁/关闭场景下 Close() 可能等锁，带超时放弃，进程退出由 OS 回收
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { _ = underlying.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})

	// NewChangeStream 内部已 CreateBucketIfNotExists，bucket 恒存在
	cs, err := NewChangeStream(context.Background(), &DB{Dir: dir, Underlying: underlying})
	if err != nil {
		t.Fatalf("NewChangeStream failed: %v", err)
	}
	return cs, underlying
}

// stateSeed 直接写底层 bbolt，构造「lastsent 与 stream key 不同步」的状态。
// 绕过 Save 是有意的：只有这样才能造出 Compact 需要判断的边界值。
func stateSeed(cs *ChangeStream, lastSent uint64, ids ...uint64) {
	_ = cs.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		_ = b.Put([]byte(csKeyLastSent), csUint64ToBytes(lastSent))
		for _, id := range ids {
			_ = b.Put(csKeyStream(id), csEncodeEntry(&csEntry{
				resumeToken: db.ResumeToken([]byte(fmt.Sprintf("tok%d", id))),
				value:       db.StreamValue([]byte(fmt.Sprintf("v%d", id))),
			}))
		}
		return nil
	})
}

func stateStreamIds(t *testing.T, cs *ChangeStream) []uint64 {
	t.Helper()
	var ids []uint64
	err := cs.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		if b == nil {
			return fmt.Errorf("bucket %s 不存在", csBucketName)
		}
		cur := b.Cursor()
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			if id, ok := csParseStreamKey(k); ok {
				ids = append(ids, id)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("读取 stream ids failed: %v", err)
	}
	return ids
}

// ---------------------------------------------------------------------------
// 用例 1（修前 FAIL / 修后 PASS，核心回归）
//
// lastsent=1002，stream key 只有 1000/1001/1002 → 真实已发送数量是 2，
// 小于阈值 1000，不该清理。
//   - 556c（minStreamId 内存缓存，构造期扫成 0）：1002-0=1002 >= 1000 →
//     删掉 id<1002 的 key，只剩 [1002] → FAIL。
//   - 改后的版本：不再依赖该缓存 → 三条都在 → PASS。
//
// 同时钉住一点：csKeyLastSent("lastsent") 字典序早于 csKeyPrefixStream("stream:")，
// cursor.First() 先拿到它；若新实现靠游标找最小 stream id，遇到这个非 stream key
// 必须 continue 跳过，写成 break 会让 min 退成 0 并再次触发误清理 → FAIL。
// ---------------------------------------------------------------------------
func TestCompactUsesDBMinStreamId(t *testing.T) {
	cs, _ := stateOpenCS(t)
	stateSeed(cs, 1002, 1000, 1001, 1002)

	cs.Compact()

	got := stateStreamIds(t, cs)
	if len(got) != 3 || got[0] != 1000 || got[1] != 1001 || got[2] != 1002 {
		t.Fatalf("Compact 后 ids = %v, want [1000 1001 1002]（按阈值不该清理）", got)
	}
}

// ---------------------------------------------------------------------------
// 用例 2（护栏）：lastSentId < 最小 stream id 时不得因 uint64 下溢误清理
//
// 两种基线下都 PASS：556c 的缓存 min=0 → 3-0=3 < 1000，不下溢；
// 改后版本不再依赖该缓存，同样不清理。
// 它防的是「将来若有人把 min 改回内存缓存且推到 lastSentId 之前，
// 3-5000 下溢成 1.8e19 → 每次 MarkSentUpTo 都进 Compact」。
// ---------------------------------------------------------------------------
func TestCompactGuardWhenLastSentBehindMinStreamId(t *testing.T) {
	cs, _ := stateOpenCS(t)
	stateSeed(cs, 3, 5000)

	cs.Compact()

	got := stateStreamIds(t, cs)
	if len(got) != 1 || got[0] != 5000 {
		t.Fatalf("Compact 后 ids = %v, want [5000]", got)
	}
}

// ---------------------------------------------------------------------------
// 用例 3（护栏）：DeleteAll ∥ Compact 真实并发入口
//
// 对应协议允许的并发：DeleteAll 在 watcher 的 ErrTokenExpired 路径；
// Compact 由 MarkSentUpTo(scheduler/sender)、Backup(HTTP handler) 触发。
// bbolt 的 beginRWTx 拿 rwlock.Lock() 串行所有写事务，所以 -race 抓不到
// data race；用例钉的是「交错之后 bucket 恒存在、只读方法不 panic」。
// ---------------------------------------------------------------------------
func TestDeleteAllConcurrentWithCompact(t *testing.T) {
	cs, _ := stateOpenCS(t)
	ctx := context.Background()

	for round := 0; round < 200; round++ {
		stateSeed(cs, uint64(round+1), uint64(round+1))

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cs.DeleteAll(ctx) }()
		go func() { defer wg.Done(); cs.Compact() }()
		wg.Wait()

		// 不变量：bucket 恒存在（New 建过；DeleteAll 删后在同一事务立即重建；
		// 写事务被 bbolt 串行，Compact 看到的必是提交后的状态）
		err := cs.db.View(func(tx *bbolt.Tx) error {
			if tx.Bucket([]byte(csBucketName)) == nil {
				return fmt.Errorf("第 %d 轮交错后 bucket 不存在", round)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		// 交错后只读路径仍可安全调用（空 bucket 下 Next 返回 false，不 panic）
		it := cs.AllNotSent(ctx)
		for it.Next(ctx, 10) {
		}
		_ = it.Err()
		it.Release(ctx)
	}
}

// ---------------------------------------------------------------------------
// 用例 4（护栏）：Update 失败时内存状态不得领先于未提交的事务
//
// 用关闭底层 DB 注入 Begin 失败。556c 把 currentSeq 的清零写在回调内，但
// Begin 失败时 fn 根本不执行，所以旧代码同样 PASS —— 它只是护栏，不是
// 修前 FAIL 的复现。真正能区分的 Commit 失败路径外部无法稳定注入
// （bbolt 无公开 Commit hook），故不伪造。
// ---------------------------------------------------------------------------
func TestDeleteAllKeepsSeqWhenDBClosed(t *testing.T) {
	cs, underlying := stateOpenCS(t)
	for i := 0; i < 3; i++ {
		if _, err := cs.Save(context.Background(),
			db.ResumeToken([]byte(fmt.Sprintf("t%d", i))),
			db.StreamValue([]byte(fmt.Sprintf("v%d", i)))); err != nil {
			t.Fatalf("Save failed: %v", err)
		}
	}
	before := cs.currentSeq

	_ = underlying.Close()
	cs.DeleteAll(context.Background())

	if cs.currentSeq != before {
		t.Fatalf("Update 失败后 currentSeq = %d, want %d（内存状态不应领先于未提交的事务）",
			cs.currentSeq, before)
	}
}
