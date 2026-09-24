package httpapi

import (
	"context"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/go-log/log"
)

type backupReq struct {
}

type backupRes struct {
	// 相对于 etl 服务运行路径的相对路径及备份的文件名
	// 如果备份失败，返回 ""
	FileName string `json:"file_name"`
}

func (s *suite) APIBackupDB(ctx context.Context, request *backupReq) *backupRes {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("api backup db")

	p, err := db.GetBackupWorker().Backup(ctx)
	if err != nil {
		logger.Error(err)
		p = ""
	}

	return &backupRes{p}
}
