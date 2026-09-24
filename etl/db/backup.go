package db

import "context"

type BackupWorker interface {
	// Backup path 返回此次生成的备份文件相对于 elt 服务运行目录的相对路径及备份文件名
	Backup(ctx context.Context) (path string, err error)
}
