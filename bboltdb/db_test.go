package bboltdb

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/xpwu/go-x/exe"
	"go.etcd.io/bbolt"
)

// newTestDB 所有测试共用的临时 bbolt 实例。
// 不使用 New()，因为 New 依赖 exe.AbsDir，测试环境下路径不可控。
func newTestDB(t *testing.T) *DB {
	t.Helper()

	dir := t.TempDir()
	underlying, err := bbolt.Open(filepath.Join(dir, "test.db"), 0600, nil)
	if err != nil {
		t.Fatalf("open bbolt failed: %v", err)
	}
	t.Cleanup(func() { _ = underlying.Close() })

	return &DB{Dir: dir, Underlying: underlying}
}

type countCompact struct{ n *int }

func (c countCompact) Compact() { *c.n++ }

// New 使用 exe.AbsDir，所以这里用唯一目录名并在结束后清理
func TestNewCreatesDBFile(t *testing.T) {
	dir := "bbolt_new_test_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	abs := filepath.Join(exe.AbsDir, dir)
	t.Cleanup(func() { _ = os.RemoveAll(abs) })

	d, err := New(context.Background(), dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	t.Cleanup(func() { _ = d.Underlying.Close() })

	if d.Dir != dir {
		t.Fatalf("Dir = %q, want %q", d.Dir, dir)
	}
	if _, err := os.Stat(filepath.Join(abs, dbName)); err != nil {
		t.Fatalf("db file %s not created: %v", dbName, err)
	}
}

func TestBackupCompactsBeforeDumping(t *testing.T) {
	dir := "bbolt_backup_test_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	abs := filepath.Join(exe.AbsDir, dir)
	t.Cleanup(func() { _ = os.RemoveAll(abs) })

	d, err := New(context.Background(), dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	t.Cleanup(func() { _ = d.Underlying.Close() })

	n := 0
	d.CompactAbles = append(d.CompactAbles, countCompact{n: &n})

	name, err := d.Backup(context.Background())
	if err != nil {
		t.Fatalf("Backup failed: %v", err)
	}
	if name == "" {
		t.Fatal("Backup 返回的相对路径为空")
	}
	if n != 1 {
		t.Fatalf("Backup 应先 Compact 一次, got %d", n)
	}
	if _, err := os.Stat(filepath.Join(exe.AbsDir, name)); err != nil {
		t.Fatalf("backup file %s not found: %v", name, err)
	}
}
