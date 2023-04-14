package leveldb

import (
	"context"
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/go-log/log"
	"path"
)

/**

[0] ==> max seq

*/

/**

以下的编码方式，可以通过seq比较stream的大小，并满足 leveldb 的默认比较方式

streamId : type seq token
type: === 1
seq: 8 byte, big-end
token: business id

*/

const (
	maxSeqKey   = 0
	streamIdPre = 1
)

func streamId(seq uint64, token []byte) db.StreamId {
	id := make([]byte, 1+8, 1+8+len(token))
	id[0] = streamIdPre
	binary.BigEndian.PutUint64(id[1:], seq)

	return append(id, token...)
}

type streamIter struct {
	iter iterator.Iterator
}

func (s *streamIter) First(ctx context.Context) (id db.StreamId, value db.StreamValue, ok bool) {
	ok = s.iter.First()
	if !ok {
		return nil, nil, false
	}

	id = s.iter.Key()
	value = s.iter.Value()

	return
}

func (s *streamIter) Last(ctx context.Context) (id db.StreamId, ok bool) {
	ok = s.iter.Last()
	if !ok {
		return nil, false
	}

	id = s.iter.Key()

	return
}

func (s *streamIter) Next(ctx context.Context, limit int) (values []db.StreamValue, lastId db.StreamId, ok bool) {
	values = make([]db.StreamValue, 0, limit)

	for s.iter.Next() {
		lastId = s.iter.Key()
		values = append(values, s.iter.Value())
		ok = true
	}

	return
}

func (s *streamIter) Release() {
	s.iter.Release()
}

type stream struct {
	db     *leveldb.DB
	maxSeq uint64
}

// Save 不是并发安全的, save 的调用 本就应该是串行的
func (s *stream) Save(ctx context.Context, token []byte, value db.StreamValue) (id db.StreamId) {
	_, logger := log.WithCtx(ctx)

	s.maxSeq += 1
	id = streamId(s.maxSeq, token)

	batch := &leveldb.Batch{}
	batch.Put([]byte{maxSeqKey}, uint642bytes(s.maxSeq))
	batch.Put(id, value)

	err := s.db.Write(batch, nil)
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	return
}

func (s *stream) Get(ctx context.Context, id db.StreamId) (value db.StreamValue, ok bool) {
	_, logger := log.WithCtx(ctx)

	value, err := s.db.Get(id, nil)
	if err == leveldb.ErrNotFound {
		return nil, false
	}
	if err != nil {
		logger.Error(err)
		panic(err)
	}

	return value, true
}

func uint642bytes(i uint64) []byte {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, i)
	return ret
}

func (s *stream) GetLastOne(ctx context.Context) (id db.StreamId, ok bool) {
	iter := &streamIter{
		iter: s.db.NewIterator(&util.Range{
			// 防止 max seq 的边界问题，这里 -1
			Start: uint642bytes(s.maxSeq - 1),
			Limit: []byte{2},
		}, nil)}

	defer iter.Release()

	return iter.Last(ctx)
}

func (s *stream) All(ctx context.Context) db.StreamIterator {
	return &streamIter{
		iter: s.db.NewIterator(&util.Range{
			Start: []byte{streamIdPre},
			Limit: []byte{2},
		}, nil)}
}

func (s *stream) StartWith(ctx context.Context, id db.StreamId) db.StreamIterator {
	return &streamIter{
		iter: s.db.NewIterator(&util.Range{
			Start: id,
			Limit: []byte{2},
		}, nil)}
}

func newStream(root string) *stream {
	p := path.Join(root, "stream")
	ldb, err := leveldb.OpenFile(p, nil)
	if err != nil {
		panic(err)
	}

	max, err := ldb.Get([]byte{maxSeqKey}, nil)
	if err == leveldb.ErrNotFound {
		// max = 1, 防止边界问题，所以初始化为1
		max = uint642bytes(1)
		err = nil
	}
	if err != nil {
		panic(err)
	}

	return &stream{db: ldb, maxSeq: binary.BigEndian.Uint64(max)}
}
