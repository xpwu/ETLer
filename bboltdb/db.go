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

type CompactAble interface {
	Compact()
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
		Timeout: 1 * time.Second,
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
		c.Compact()
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
	defer f.Close()

	if _, err = tx.WriteTo(f); err != nil {
		logger.Error(err)
		os.Remove(tmpPath)
		return "", err
	}

	// 原子 rename
	if err = os.Rename(tmpPath, backupPath); err != nil {
		logger.Error(err)
		os.Remove(tmpPath)
		return "", err
	}

	logger.Info("succeed")

	return filepath.Join(db.Dir, backupName), nil
}
