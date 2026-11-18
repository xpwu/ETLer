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
	csKeyPrefixStream = "stream:"
	csKeyStreamFmt    = "%016x"
	csKeyLastSent     = "lastsent"

	// 阈值：已发送未清理的数量超过此值时触发 compact
	csCompactThreshold = uint64(1000)
)

func csKeyStream(id uint64) []byte {
	return []byte(fmt.Sprintf(csKeyPrefixStream+csKeyStreamFmt, id))
}

func csParseStreamKey(k []byte) (uint64, bool) {
	if len(k) != len(csKeyPrefixStream)+16 {
		return 0, false
	}
	if string(k[:len(csKeyPrefixStream)]) != csKeyPrefixStream {
		return 0, false
	}
	var id uint64
	_, err := fmt.Sscanf(string(k[len(csKeyPrefixStream):]), csKeyStreamFmt, &id)
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
	// data 来自 bbolt 的 mmap，事务关闭后即失效，必须复制。
	// ResumeToken() 也会把 resumeToken 传出 View 之外，依赖这里的拷贝。
	token := make([]byte, int(tokenLen))
	copy(token, data[4:4+int(tokenLen)])
	value := make([]byte, len(data)-4-int(tokenLen))
	copy(value, data[4+int(tokenLen):])
	return &csEntry{
		resumeToken: db.ResumeToken(token),
		value:       db.StreamValue(value),
	}
}

// ============ ChangeStream ============

type ChangeStream struct {
	db         *bbolt.DB
	currentSeq uint64 // 内存：当前最大 stream id
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

	// 扫描初始化 currentSeq
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

	return id, nil
}

func (c *ChangeStream) MarkSentUpTo(ctx context.Context, id db.StreamId) {
	streamId := csBytesToUint64(id)
	wrote := false
	_, logger := log.WithCtx(ctx)
	logger.PushPrefix("MarkSentUpTo")

	// 只能向后推进，不能回退
	err := c.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		raw := b.Get([]byte(csKeyLastSent))
		if raw != nil {
			cur := csBytesToUint64(raw)
			if streamId <= cur {
				return nil // 不回退
			}
		}

		if b.Get(csKeyStream(streamId)) == nil {
			// 不存在 id 对应的 item，不做任何操作
			return nil
		}

		// 已发送点之后紧跟着的连续 nil 数据，既然不需要发送，就等价于"已经处理完"
		cursor := b.Cursor()
		for k, v := cursor.Seek(csKeyStream(streamId + 1)); k != nil; k, v = cursor.Next() {
			id, ok := csParseStreamKey(k)
			if !ok {
				break
			}

			if entry := csDecodeEntry(v); entry != nil && len(entry.value) != 0 {
				break // 第一条有效数据，停下；有效数据不能跨过：它还没发送
			}

			// 只有不需要发送的 nil 才等价于已处理完
			streamId = id
		}

		wrote = true
		return b.Put([]byte(csKeyLastSent), csUint64ToBytes(streamId))
	})

	if err != nil {
		logger.Error(err)
		return
	}

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
	_, logger := log.WithCtx(ctx)
	logger.PushPrefix("DeleteAll")

	err := c.db.Update(func(tx *bbolt.Tx) error {
		// 直接删 bucket
		if err := tx.DeleteBucket([]byte(csBucketName)); err != nil {
			if err == bboltErrors.ErrBucketNotFound {
				return nil
			}
			return err
		}
		// 重建
		_, err := tx.CreateBucket([]byte(csBucketName))

		return err
	})

	if err != nil {
		logger.Error(err)
		return
	}

	// 只有事务真正提交成功才重置内存状态
	c.currentSeq = 0
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

		// 从前向后，找当前最小 stream id
		var minStreamId uint64
		cur := b.Cursor()
		for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
			if id, ok := csParseStreamKey(k); ok {
				minStreamId = id
				break
			}
		}

		// 堵住下溢
		if minStreamId == 0 || lastSentId <= minStreamId {
			return nil
		}

		if lastSentId-minStreamId < csCompactThreshold {
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
			_ = b.Delete(k)
		}

		return nil
	})
}

// ============ ChangeStreamIter ============

// ChangeStreamIter 迭代器对外不持有事务：每次 Next 用一个短只读事务，读完即关。
// 调用方在 Next() 返回 true 后会立刻执行 MarkSentUpTo / Save 等写事务，
// 长读事务会与 bbolt 的 mmap 扩容互等（且 db.Update 不响应 ctx）。
type ChangeStreamIter struct {
	cs *ChangeStream

	values    []db.StreamValue
	lastId    db.StreamId
	err       error
	exhausted bool
	released  bool
	started   bool
	lastSent  uint64
}

func (it *ChangeStreamIter) Next(ctx context.Context, limit int) bool {
	if it.released || it.exhausted || it.err != nil {
		return false
	}
	select {
	case <-ctx.Done():
		it.err = ctx.Err()
		return false
	default:
	}

	it.values = nil

	var values []db.StreamValue
	var lastId db.StreamId

	viewErr := it.cs.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		if b == nil {
			return nil
		}

		// 库里一条数据都没有：没有 lastsent 也就谈不上"找不到已发送点"，
		// 这是正常空状态，不是错误。
		firstKey, _ := b.Cursor().First()
		if firstKey == nil {
			return nil
		}

		if !it.started {
			it.started = true
			raw := b.Get([]byte(csKeyLastSent))
			if raw == nil {
				it.err = db.ErrNotFoundSentPoint
				return nil
			}
			it.lastSent = csBytesToUint64(raw)
			if b.Get(csKeyStream(it.lastSent)) == nil {
				it.err = db.ErrNotFoundSentPoint
				return nil
			}
		}

		seekId := it.lastSent + 1
		if len(it.lastId) > 0 {
			seekId = csBytesToUint64(it.lastId) + 1
		}

		cursor := b.Cursor()
		count := 0
		for k, v := cursor.Seek(csKeyStream(seekId)); k != nil; k, v = cursor.Next() {
			select {
			case <-ctx.Done():
				it.err = ctx.Err()
				return nil
			default:
			}

			id, ok := csParseStreamKey(k)
			if !ok {
				break
			}

			entry := csDecodeEntry(v)
			if entry == nil || len(entry.value) == 0 {
				continue
			}

			values = append(values, append(db.StreamValue(nil), entry.value...))
			lastId = csUint64ToBytes(id)
			count++

			if limit > 0 && count >= limit {
				break
			}
		}
		return nil
	})

	if viewErr != nil {
		it.err = viewErr
		return false
	}
	if it.err != nil {
		return false
	}

	if len(values) == 0 {
		it.exhausted = true
		it.Release(ctx)
		return false
	}

	it.values = values
	it.lastId = lastId
	return true
}

func (it *ChangeStreamIter) Values() []db.StreamValue { return it.values }

func (it *ChangeStreamIter) LastStreamId() db.StreamId {
	if len(it.values) == 0 {
		return nil
	}
	return it.lastId
}

func (it *ChangeStreamIter) Err() error { return it.err }

func (it *ChangeStreamIter) Release(ctx context.Context) {
	if it.released {
		return
	}
	it.released = true
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
