package db

import "context"

type BackupWorker interface {
	// Backup path 返回此次生成的备份文件相对于 elt 服务运行目录的相对路径及备份文件名
	// 注意：备份前会先调用各 CompactAble 整理本地数据库；若本地数据库出现服务不可运行的
	// 异常，会直接 panic（由 httpapi 侧转成 http 500），不走下面的 error 返回值。
	Backup(ctx context.Context) (path string, err error)
}

var backupWorker BackupWorker

func SetBackupWorker(b BackupWorker) {
	backupWorker = b
}

func GetBackupWorker() BackupWorker {
	return backupWorker
}
