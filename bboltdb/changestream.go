package bboltdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/go-log/log"
	"go.etcd.io/bbolt"
	bboltErrors "go.etcd.io/bbolt/errors"
)

const csBucketName = "changestream"

const (
	csKeyLastSent = "lastsent"
)

// 阈值：已发送未清理的数量超过此值时触发 compact
const csCompactThreshold = uint64(1000)

func csKeyStream(id uint64) []byte {
	return []byte(fmt.Sprintf("stream:%016x", id))
}

func csParseStreamKey(k []byte) (uint64, bool) {
	if len(k) != 24 || string(k[:7]) != "stream:" {
		return 0, false
	}
	var id uint64
	_, err := fmt.Sscanf(string(k[7:]), "%016x", &id)
	return id, err == nil
}

// csEntry 内部编码格式：resumeTokenLen(4字节) + resumeToken(bson.Raw) + value(bson.Raw)
type csEntry struct {
	resumeToken db.ResumeToken
	value       db.StreamValue
}

func csEncodeEntry(e *csEntry) []byte {
	buf := make([]byte, 4+len(e.resumeToken)+len(e.value))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(e.resumeToken)))
	copy(buf[4:4+len(e.resumeToken)], e.resumeToken)
	copy(buf[4+len(e.resumeToken):], e.value)
	return buf
}

func csDecodeEntry(data []byte) *csEntry {
	if len(data) < 4 {
		return nil
	}
	tokenLen := binary.BigEndian.Uint32(data[:4])
	if 4+uint32(tokenLen) > uint32(len(data)) {
		return nil
	}
	return &csEntry{
		resumeToken: db.ResumeToken(data[4 : 4+tokenLen]),
		value:       db.StreamValue(data[4+tokenLen:]),
	}
}

// ============ ChangeStream ============

type ChangeStream struct {
	db          *bbolt.DB
	currentSeq  uint64 // 内存：当前最大 stream id
	minStreamId uint64 // 内存：当前最小 stream id
}

func NewChangeStream(ctx context.Context, db *DB) (*ChangeStream, error) {
	cs := &ChangeStream{db: db.Underlying}
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("NewChangeStream")

	// 创建 bucket
	err := cs.db.Update(func(tx *bbolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists([]byte(csBucketName))
		return e
	})
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	// 扫描初始化 currentSeq / minStreamId
	err = cs.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		if b == nil {
			return nil
		}
		cursor := b.Cursor()

		// 找最大 id：从后往前
		for k, _ := cursor.Last(); k != nil; k, _ = cursor.Prev() {
			if id, ok := csParseStreamKey(k); ok {
				cs.currentSeq = id
				break
			}
		}
		// 找最小 id：从前往后
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			if id, ok := csParseStreamKey(k); ok {
				cs.minStreamId = id
				break
			}
		}
		return nil
	})
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	db.CompactAbles = append(db.CompactAbles, cs)
	return cs, nil
}

// Save / DeleteAll 不会被并发调用，这两个方法相互之间也不会并发调用，
// 但是 Save / DeleteAll 与其它方法相互之间会并发调用，其它方法本身也可能并发调用
func (c *ChangeStream) Save(ctx context.Context, resumeToken db.ResumeToken, value db.StreamValue) (id db.StreamId, err error) {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("change stream:save")

	c.currentSeq++
	id = csUint64ToBytes(c.currentSeq)

	err = c.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		entry := &csEntry{resumeToken: resumeToken, value: value}
		return b.Put(csKeyStream(csBytesToUint64(id)), csEncodeEntry(entry))
	})
	if err != nil {
		// 回滚序号（避免空洞，可选）
		c.currentSeq--
		logger.Error(err)
		return nil, err
	}

	logger.Debug(fmt.Sprintf("resume token: %s, value: %s", resumeToken, value))
	return id, nil
}

func (c *ChangeStream) MarkSentUpTo(ctx context.Context, id db.StreamId) {
	streamId := csBytesToUint64(id)
	wrote := false

	// 只能向后推进，不能回退
	_ = c.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		raw := b.Get([]byte(csKeyLastSent))
		if raw != nil {
			cur := csBytesToUint64(raw)
			if streamId <= cur {
				return nil // 不回退
			}
		}

		wrote = true
		return b.Put([]byte(csKeyLastSent), csUint64ToBytes(streamId))
	})

	// 触发清理检查
	if wrote {
		c.Compact()
	}
}

func (c *ChangeStream) AllNotSent(ctx context.Context) db.ChangeStreamIterator {
	return &ChangeStreamIter{
		cs: c,
	}
}

func (c *ChangeStream) LastStreamId() db.StreamId {
	var lastId db.StreamId = csUint64ToBytes(0)
	_ = c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		if b == nil {
			return nil
		}
		cursor := b.Cursor()
		for k, _ := cursor.Last(); k != nil; k, _ = cursor.Prev() {
			if id, ok := csParseStreamKey(k); ok {
				lastId = csUint64ToBytes(id)
				break
			}
		}
		return nil
	})
	return lastId
}

func (c *ChangeStream) ResumeToken(ctx context.Context) (resumeToken db.ResumeToken) {
	_ = c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		if b == nil {
			return nil
		}
		cursor := b.Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			if _, ok := csParseStreamKey(k); ok {
				if entry := csDecodeEntry(v); entry != nil {
					resumeToken = entry.resumeToken
				}
				break
			}
		}
		return nil
	})
	return
}

// DeleteAll / Save 不会被并发调用，这两个方法相互之间也不会并发调用，
// 但是 Save / DeleteAll 与其它方法相互之间会并发调用，其它方法本身也可能并发调用
func (c *ChangeStream) DeleteAll(ctx context.Context) {
	_ = c.db.Update(func(tx *bbolt.Tx) error {
		// 直接删 bucket
		if err := tx.DeleteBucket([]byte(csBucketName)); err != nil {
			if err == bboltErrors.ErrBucketNotFound {
				return nil
			}
			return err
		}
		// 重建
		_, err := tx.CreateBucket([]byte(csBucketName))

		// 重置内存状态
		c.currentSeq = 0
		c.minStreamId = 0

		return err
	})
}

// Compact 清理已发送数据，保留最后一条 sent（保证 ResumeToken 始终有数据）
func (c *ChangeStream) Compact() {
	_ = c.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		raw := b.Get([]byte(csKeyLastSent))
		if raw == nil {
			return nil
		}
		lastSentId := csBytesToUint64(raw)

		sentCount := lastSentId - c.minStreamId
		if sentCount < csCompactThreshold {
			return nil
		}

		// 删除 id < lastSentId 的所有 stream key
		var toDelete [][]byte
		cursor := b.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			id, ok := csParseStreamKey(k)
			if !ok {
				continue
			}
			if id >= lastSentId {
				break
			}
			toDelete = append(toDelete, append([]byte(nil), k...))
		}

		for _, k := range toDelete {
			b.Delete(k)
		}

		c.minStreamId = lastSentId
		return nil
	})
}

// ============ ChangeStreamIter ============

type ChangeStreamIter struct {
	cs *ChangeStream
	tx *bbolt.Tx

	values    []db.StreamValue
	lastId    db.StreamId
	err       error
	exhausted bool
	released  bool
	started   bool
	lastSent  uint64
}

func (it *ChangeStreamIter) Next(ctx context.Context, limit int) bool {
	if it.released || it.exhausted {
		return false
	}

	select {
	case <-ctx.Done():
		it.err = ctx.Err()
		return false
	default:
	}

	// 每次 Next 开新读事务，保证看到最新数据
	if it.tx != nil {
		it.tx.Rollback()
	}
	tx, err := it.cs.db.Begin(false)
	if err != nil {
		it.err = err
		return false
	}
	it.tx = tx

	b := tx.Bucket([]byte(csBucketName))
	if b == nil {
		it.exhausted = true
		return false
	}

	// 首次：读 lastsent
	if !it.started {
		it.started = true
		raw := b.Get([]byte(csKeyLastSent))
		if raw == nil {
			it.err = db.ErrNotFoundSentPoint
			return false
		}
		it.lastSent = csBytesToUint64(raw)

		// 判断 lastSent 对应的 stream 数据是否还在
		if b.Get(csKeyStream(it.lastSent)) == nil {
			it.err = db.ErrNotFoundSentPoint
			return false
		}
	}

	// seek 起点
	var seekId uint64
	if len(it.values) > 0 {
		seekId = csBytesToUint64(it.lastId) + 1
	} else {
		seekId = it.lastSent + 1
	}

	it.values = it.values[:0]

	cursor := b.Cursor()
	count := 0
	for k, v := cursor.Seek(csKeyStream(seekId)); k != nil; k, v = cursor.Next() {
		select {
		case <-ctx.Done():
			it.err = ctx.Err()
			return false
		default:
		}

		id, ok := csParseStreamKey(k)
		if !ok {
			break // 不是 stream: 前缀，到末尾了
		}

		entry := csDecodeEntry(v)
		if entry == nil {
			continue
		}
		it.values = append(it.values, entry.value)
		it.lastId = csUint64ToBytes(id)
		count++

		if limit > 0 && count >= limit {
			break
		}
	}

	if len(it.values) == 0 {
		it.exhausted = true
		// 耗完就该释放
		it.Release()
		return false
	}
	return true
}

func (it *ChangeStreamIter) Values() []db.StreamValue {
	return it.values
}

func (it *ChangeStreamIter) LastStreamId() db.StreamId {
	if len(it.values) == 0 {
		return nil
	}
	return it.lastId
}

func (it *ChangeStreamIter) Err() error {
	return it.err
}

func (it *ChangeStreamIter) Release() {
	if it.released {
		return
	}
	it.released = true
	if it.tx != nil {
		it.tx.Rollback()
		it.tx = nil
	}
	it.values = nil
}

// ============ uint64 <-> []byte ============

func csUint64ToBytes(id uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, id)
	return buf
}

func csBytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}
