package bboltdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/xpwu/ETLer/etl/db"
	"go.etcd.io/bbolt/errors"

	"github.com/xpwu/ETLer/x"
	"go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const stBucketName = "synctask"

// ---- 编码 / 解码 ----

func encodeTask(t db.Task) []byte {
	taskDB := t.WatchInfo.DB
	coll := t.WatchInfo.Collection

	totalLen := 4 + len(taskDB) + 4 + len(coll) + 1 // type byte
	if t.UntilDocId.Value != nil {
		totalLen += 4 + len(t.UntilDocId.Value)
	}

	buf := make([]byte, 0, totalLen)

	// taskDB
	dbLen := make([]byte, 4)
	binary.BigEndian.PutUint32(dbLen, uint32(len(taskDB)))
	buf = append(buf, dbLen...)
	buf = append(buf, taskDB...)

	// collection
	collLen := make([]byte, 4)
	binary.BigEndian.PutUint32(collLen, uint32(len(coll)))
	buf = append(buf, collLen...)
	buf = append(buf, coll...)

	// UntilDocId.Type
	buf = append(buf, byte(t.UntilDocId.Type))

	// UntilDocId.Value
	valLen := make([]byte, 4)
	binary.BigEndian.PutUint32(valLen, uint32(len(t.UntilDocId.Value)))
	buf = append(buf, valLen...)
	buf = append(buf, t.UntilDocId.Value...)

	return buf
}

func decodeTask(data []byte) (db.Task, error) {
	if len(data) < 4 {
		return db.Task{}, fmt.Errorf("decodeTask: data too short")
	}

	offset := 0

	// db
	dbLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	if offset+int(dbLen) > len(data) {
		return db.Task{}, fmt.Errorf("decodeTask: db length overflow")
	}
	taskDB := string(data[offset : offset+int(dbLen)])
	offset += int(dbLen)

	// collection
	if offset+4 > len(data) {
		return db.Task{}, fmt.Errorf("decodeTask: collection length prefix overflow")
	}
	collLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	if offset+int(collLen) > len(data) {
		return db.Task{}, fmt.Errorf("decodeTask: collection length overflow")
	}
	collection := string(data[offset : offset+int(collLen)])
	offset += int(collLen)

	// UntilDocId.Type
	if offset+1 > len(data) {
		return db.Task{}, fmt.Errorf("decodeTask: type byte overflow")
	}
	typ := bson.Type(data[offset])
	offset += 1

	// UntilDocId.Value
	if offset+4 > len(data) {
		return db.Task{}, fmt.Errorf("decodeTask: value length prefix overflow")
	}
	valLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	var rawValue []byte
	if valLen > 0 {
		if offset+int(valLen) > len(data) {
			return db.Task{}, fmt.Errorf("decodeTask: value overflow")
		}
		rawValue = data[offset : offset+int(valLen)]
	}

	return db.Task{
		WatchInfo: x.WatchInfo{
			DB:         taskDB,
			Collection: collection,
		},
		UntilDocId: bson.RawValue{
			Type:  typ,
			Value: rawValue,
		},
	}, nil
}

// ---- SyncTaskIterator ----

type syncTaskIter struct {
	tx       *bbolt.Tx
	cursor   *bbolt.Cursor
	current  db.Task
	err      error
	released bool
}

func (it *syncTaskIter) Next(ctx context.Context) bool {
	if it.released {
		return false
	}

	select {
	case <-ctx.Done():
		it.err = ctx.Err()
		return false
	default:
	}

	k, v := it.cursor.Next()
	if k == nil {
		return false
	}

	task, err := decodeTask(v)
	if err != nil {
		it.err = err
		return false
	}

	it.current = task
	return true
}

func (it *syncTaskIter) Current() db.Task {
	return it.current
}

func (it *syncTaskIter) Err() error {
	return it.err
}

func (it *syncTaskIter) Release(ctx context.Context) {
	if it.released {
		return
	}
	it.released = true
	if it.tx != nil {
		_ = it.tx.Rollback()
		it.tx = nil
	}
}

// ---- SyncTask ----

type SyncTask struct {
	db *bbolt.DB
}

func NewSyncTask(ctx context.Context, db *DB) *SyncTask {
	_ = db.Underlying.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(stBucketName))
		return err
	})
	return &SyncTask{db.Underlying}
}

func (s *SyncTask) All(ctx context.Context) db.SyncTaskIterator {
	tx, err := s.db.Begin(false) // readonly
	if err != nil {
		return &syncTaskIter{err: err, released: true}
	}

	b := tx.Bucket([]byte(stBucketName))
	if b == nil {
		_ = tx.Rollback()
		return &syncTaskIter{err: fmt.Errorf("bucket %s not found", stBucketName), released: true}
	}

	iter := &syncTaskIter{
		tx:     tx,
		cursor: b.Cursor(),
	}

	// cursor 定位到第一条之前，Next 里调 cursor.Next()
	// bbolt cursor 初始位置在第一个 key 之前，所以 Next 里直接 cursor.Next() 即可

	return iter
}

func (s *SyncTask) InsertOrUpdate(ctx context.Context, task db.Task) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	_ = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(stBucketName))
		if b == nil {
			return fmt.Errorf("bucket %s not found", stBucketName)
		}
		return b.Put([]byte(task.Id()), encodeTask(task))
	})
}

func (s *SyncTask) InsertOrUpdateBatch(ctx context.Context, tasks []db.Task) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	_ = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(stBucketName))
		if b == nil {
			return fmt.Errorf("bucket %s not found", stBucketName)
		}
		for _, task := range tasks {
			if err := b.Put([]byte(task.Id()), encodeTask(task)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SyncTask) Del(ctx context.Context, id string) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	_ = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(stBucketName))
		if b == nil {
			return nil
		}
		return b.Delete([]byte(id))
	})
}

func (s *SyncTask) DelBatch(ctx context.Context, ids []string) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	_ = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(stBucketName))
		if b == nil {
			return nil
		}
		for _, id := range ids {
			if err := b.Delete([]byte(id)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SyncTask) Clear(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	_ = s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket([]byte(stBucketName)); err != nil {
			if err == errors.ErrBucketNotFound {
				return nil
			}
			return err
		}
		_, err := tx.CreateBucket([]byte(stBucketName))
		return err
	})
}
