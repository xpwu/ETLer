package bboltdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/go-x/exe"
	"go.etcd.io/bbolt"
)

var backupDirSeq atomic.Uint64

// newBackupDB 用 New() 而不是 newTestDB()：
// Backup 内部 backupDir = filepath.Join(exe.AbsDir, db.Dir)，要求 db.Dir 是【相对路径】；
// newTestDB 的 Dir 是 t.TempDir() 绝对路径，会被拼到 exe.AbsDir 下面，测不到真实路径。
func newBackupDB(t *testing.T) (*DB, string) {
	t.Helper()

	dir := fmt.Sprintf("bbolt_backup_%d_%d", time.Now().UnixNano(), backupDirSeq.Add(1))
	abs := filepath.Join(exe.AbsDir, dir)
	t.Cleanup(func() { _ = os.RemoveAll(abs) })

	d, err := New(context.Background(), dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	// 关闭失败时忽略：若用例主动 Close 过，这里再 Close 会返回 error
	t.Cleanup(func() { _ = d.Underlying.Close() })

	return d, abs
}

// assertValidBoltFile 用只读方式打开备份文件并遍历顶层 bucket，
// 能完成即说明 meta / freelist / 页结构完整。
// 注意：它验证的是【结构完整】，不验证快照内容的单调性；
// 同时 filepath.Join(exe.AbsDir, name) 隐含要求 Backup 返回的是相对路径。
func assertValidBoltFile(t *testing.T, path string) {
	t.Helper()

	b, err := bbolt.Open(path, 0600, &bbolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		t.Fatalf("备份文件 %s 无法只读打开: %v", path, err)
	}
	err = b.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(_ []byte, _ *bbolt.Bucket) error { return nil })
	})
	_ = b.Close()
	if err != nil {
		t.Fatalf("备份文件 %s 结构不完整: %v", path, err)
	}
}

// ---------------------------------------------------------------------------
// 已知局限（测试抓不到，属产品侧可选改进）：db.go:126 的 defer f.Close()
// 在 :135 os.Rename 之后才执行，且全程没有 f.Sync()。POSIX 上没问题，
// Windows 上 rename 已打开的文件会失败；崩溃场景下页缓存可能未落盘。
// ---------------------------------------------------------------------------
func TestBackupFileIsValidBoltDB(t *testing.T) {
	d, _ := newBackupDB(t)
	if _, err := NewChangeStream(context.Background(), d); err != nil {
		t.Fatalf("NewChangeStream failed: %v", err)
	}

	name, err := d.Backup(context.Background())
	if err != nil || name == "" {
		t.Fatalf("Backup failed: name=%q err=%v", name, err)
	}
	assertValidBoltFile(t, filepath.Join(exe.AbsDir, name))
}

// ---------------------------------------------------------------------------
// 注意：这条覆盖的是「Begin 失败前不创建临时文件」，不是「Rename 失败后清理」；
// 后者在 db.go:130 / :137 已显式 os.Remove(tmpPath)，另两条路径外部无法稳定注入。
// ---------------------------------------------------------------------------
func TestBackupFailureLeavesNoTempFile(t *testing.T) {
	d, abs := newBackupDB(t)

	// 注册一个真实的 CompactAble，覆盖「Compact 在已关闭 DB 上不 panic」
	if _, err := NewChangeStream(context.Background(), d); err != nil {
		t.Fatalf("NewChangeStream failed: %v", err)
	}

	if err := d.Underlying.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	name, err := d.Backup(context.Background())
	if err == nil {
		t.Fatal("底层 DB 已关闭，Backup 应返回 error")
	}
	if name != "" {
		t.Fatalf("失败时 backupName = %q, want 空字符串", name)
	}

	entries, err := os.ReadDir(abs)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("Backup 失败后遗留临时文件: %s", e.Name())
		}
	}
}

func TestBackupDuringWritesProducesReadableFile(t *testing.T) {
	d, _ := newBackupDB(t)
	cs, err := NewChangeStream(context.Background(), d)
	if err != nil {
		t.Fatalf("NewChangeStream failed: %v", err)
	}

	ctx := context.Background()

	// 先同步写几条，保证快照里确实有内容，不是空库
	for i := 0; i < 5; i++ {
		if _, err := cs.Save(ctx,
			db.ResumeToken([]byte(fmt.Sprintf("seedtok%d", i))),
			db.StreamValue([]byte(fmt.Sprintf("seedv%d", i)))); err != nil {
			t.Fatalf("预热 Save(%d) failed: %v", i, err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = cs.Save(ctx,
				db.ResumeToken([]byte(fmt.Sprintf("tok%d", i))),
				db.StreamValue([]byte(fmt.Sprintf("v%d", i))))
		}
	}()

	name, err := d.Backup(ctx)
	close(stop)
	wg.Wait()

	if err != nil || name == "" {
		t.Fatalf("并发写期间 Backup failed: name=%q err=%v", name, err)
	}
	assertValidBoltFile(t, filepath.Join(exe.AbsDir, name))
}
