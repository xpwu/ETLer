package bboltdb

import (
	"context"
	"fmt"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/go-log/log"
	"github.com/xpwu/go-x/exe"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
)

func Init() error {
	ctx := context.Background()
	localDB, err := New(ctx, "bboltdb")
	if err != nil {
		return err
	}

	cs, err := NewChangeStream(ctx, localDB)
	if err != nil {
		return err
	}

	st, err := NewSyncTask(ctx, localDB)
	if err != nil {
		return err
	}

	wc, err := NewWatchCollection(ctx, localDB)
	if err != nil {
		return err
	}

	db.SetBackupWorker(localDB)
	db.SetSyncTask(st)
	db.SetWatchCollection(wc)
	db.SetChangeStream(cs)

	return nil
}

const dbName = "etlworkdb"

// CompactAble 出现服务不可运行的异常，直接 panic
type CompactAble interface {
	Compact(ctx context.Context)
}

type DB struct {
	Dir          string
	Underlying   *bbolt.DB
	CompactAbles []CompactAble
}

// New dir 相对于服务运行目录的相对目录
func New(ctx context.Context, dir string) (*DB, error) {
	localDB := &DB{}
	localDB.Dir = dir

	_, logger := log.WithCtx(ctx)

	logger.PushPrefix(fmt.Sprintf("open localDB in: %s", dir))

	dbPath := filepath.Join(exe.AbsDir, dir)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		logger.Error(err)
		return nil, err
	}

	dbPath = filepath.Join(dbPath, dbName)

	underlying, err := bbolt.Open(dbPath, 0644, &bbolt.Options{
		Timeout:         1 * time.Second,
		InitialMmapSize: 64 << 20,
	})
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	localDB.Underlying = underlying

	return localDB, nil
}

// Backup path 返回此次生成的备份文件相对于 elt 服务运行目录的相对路径及备份文件名
func (db *DB) Backup(ctx context.Context) (backupName string, err error) {
	ctx, logger := log.WithCtx(ctx)
	suffixName := time.Now().Format("20060102150405")

	logger.PushPrefix("bbolt backup: " + suffixName)

	for _, c := range db.CompactAbles {
		c.Compact(ctx)
	}

	// New 中已经创建过了，也可以再创建路径
	backupDir := filepath.Join(exe.AbsDir, db.Dir)
	if err = os.MkdirAll(backupDir, 0755); err != nil {
		logger.Error(err)
		return "", err
	}

	// 备份文件名 = etlworkdb-backup-{suffixName}
	backupName = "etlworkdb-backup-" + suffixName
	backupPath := filepath.Join(backupDir, backupName)
	tmpPath := backupPath + ".tmp"

	// 只读事务
	tx, err := db.Underlying.Begin(false)
	if err != nil {
		logger.Error(err)
		return "", err
	}
	defer tx.Rollback()

	// 写临时文件
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logger.Error(err)
		return "", err
	}

	// 这里按 POSIX 行为写：f 还开着就 os.Rename，POSIX 允许 rename 已打开的文件，
	// 页缓存里的内容也仍可读，所以备份可用。已知两个限制，当前部署接受：
	//   1) Windows 不允许 rename 仍被打开的文件，这一步会直接失败；
	//   2) 没有 f.Sync()，进程崩溃时页缓存可能未落盘，备份文件可能不完整。
	// 若要跨平台或保证崩溃安全，应 f.Sync() 后先 Close，再 Rename。
	defer f.Close()

	if _, err = tx.WriteTo(f); err != nil {
		logger.Error(err)
		_ = os.Remove(tmpPath)
		return "", err
	}

	// 原子 rename
	if err = os.Rename(tmpPath, backupPath); err != nil {
		logger.Error(err)
		_ = os.Remove(tmpPath)
		return "", err
	}

	logger.Info("succeed")

	return filepath.Join(db.Dir, backupName), nil
}
