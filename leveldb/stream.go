package leveldb

import (
	"context"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xpwu/ETLer/etl/db"
	"path"
)

type stream struct {
	db *leveldb.DB
}

func (s *stream) Save(ctx context.Context, id db.StreamId, value db.StreamValue) {
	//TODO implement me
	panic("implement me")
}

func (s *stream) Get(ctx context.Context, id db.StreamId) (value db.StreamValue, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (s *stream) GetLastOne(ctx context.Context) (id db.StreamId, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (s *stream) First(ctx context.Context) (id db.StreamId, value db.StreamValue, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (s *stream) Next(ctx context.Context, id db.StreamId, limit int) (values []db.StreamValue, lastId db.StreamId, ok bool) {
	//TODO implement me
	panic("implement me")
}

func newStream(root string) *stream {
	p := path.Join(root, "stream")
	ldb, err := leveldb.OpenFile(p, nil)
	if err != nil {
		panic(err)
	}
	return &stream{db: ldb}
}
