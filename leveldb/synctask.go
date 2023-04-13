package leveldb

import (
	"context"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xpwu/ETLer/etl/db"
	"path"
)

type syncTask struct {
	db *leveldb.DB
}

func (s *syncTask) First(ctx context.Context) (task db.Task, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (s *syncTask) Next(ctx context.Context, afterId string) (task db.Task, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (s *syncTask) InsertOrUpdate(ctx context.Context, task db.Task) {
	//TODO implement me
	panic("implement me")
}

func (s *syncTask) InsertOrUpdateBatch(ctx context.Context, tasks []db.Task) {
	//TODO implement me
	panic("implement me")
}

func (s *syncTask) Del(ctx context.Context, id string) {
	//TODO implement me
	panic("implement me")
}

func (s *syncTask) DelBatch(ctx context.Context, ids []string) {
	//TODO implement me
	panic("implement me")
}

func (s *syncTask) DelAll(ctx context.Context) {
	//TODO implement me
	panic("implement me")
}

func newSyncTask(root string) *syncTask {
	p := path.Join(root, "synctask")
	ldb, err := leveldb.OpenFile(p, nil)
	if err != nil {
		panic(err)
	}
	return &syncTask{db: ldb}
}