package leveldb

import (
	"context"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/xpwu/ETLer/etl/db"
	"path"
)

type stream struct {
	db *leveldb.DB
}

/**

[0] ==> max seq

 */

/**

streamId : type seq token
type: === 1
seq: 8 byte, big-end
token: business id

 */

type streamIter struct {
	iter iterator.Iterator
}

func (s *streamIter) First(ctx context.Context) (id db.StreamId, value db.StreamValue, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (s *streamIter) Last(ctx context.Context) (id db.StreamId, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (s *streamIter) Next(ctx context.Context, limit int) (values []db.StreamValue, lastId db.StreamId, ok bool) {
	//TODO implement me
	panic("implement me")
}

func (s *streamIter) Release() {
	s.iter.Release()
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

func (s *stream) All(ctx context.Context) db.StreamIterator {
	//TODO implement me
	panic("implement me")
}

func (s *stream) StartWith(ctx context.Context, id db.StreamId) db.StreamIterator {
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
