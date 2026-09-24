package bboltdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/xpwu/go-log/log"
	"go.etcd.io/bbolt/errors"
	"math"

	"github.com/xpwu/ETLer/x"
	"go.etcd.io/bbolt"
)

const wcBucketName = "watchcollection"

const (
	wcKeyPrefixVersion = "version:%016x"
	wcKeyMetaSyncing   = "meta:syncing"
	wcKeyMetaSynced    = "meta:latest_synced"
)

func wcVersionKey(version uint64) []byte {
	return []byte(fmt.Sprintf(wcKeyPrefixVersion, version))
}

func wcParseVersionKey(k []byte) (uint64, bool) {
	prefix := []byte("version:")
	if len(k) < len(prefix)+16 {
		return 0, false
	}
	if string(k[:len(prefix)]) != "version:" {
		return 0, false
	}
	var id uint64
	_, err := fmt.Sscanf(string(k[len(prefix):]), "%016x", &id)
	return id, err == nil
}

func wcUint64ToBytes(v uint64) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, v)
	return bs
}

func wcBytesToUint64(bs []byte) uint64 {
	if len(bs) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(bs[:8])
}

// encodeWatchInfos 编码 []x.WatchInfo → []byte
// 格式: count(uint32) + 每条 { dbLen(uint32) + db + collLen(uint32) + collection }
func encodeWatchInfos(infos []x.WatchInfo) []byte {
	totalLen := 4 // count
	for _, info := range infos {
		totalLen += 4 + len(info.DB) + 4 + len(info.Collection)
	}

	buf := make([]byte, 0, totalLen)

	countBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(countBytes, uint32(len(infos)))
	buf = append(buf, countBytes...)

	for _, info := range infos {
		dbLen := make([]byte, 4)
		binary.BigEndian.PutUint32(dbLen, uint32(len(info.DB)))
		buf = append(buf, dbLen...)
		buf = append(buf, info.DB...)

		collLen := make([]byte, 4)
		binary.BigEndian.PutUint32(collLen, uint32(len(info.Collection)))
		buf = append(buf, collLen...)
		buf = append(buf, info.Collection...)
	}

	return buf
}

// decodeWatchInfos 解码 []byte → []x.WatchInfo
func decodeWatchInfos(data []byte) []x.WatchInfo {
	if len(data) < 4 {
		return nil
	}
	count := binary.BigEndian.Uint32(data[:4])
	infos := make([]x.WatchInfo, 0, count)

	offset := 4
	for i := uint32(0); i < count; i++ {
		// db
		if offset+4 > len(data) {
			break
		}
		dbLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if offset+int(dbLen) > len(data) {
			break
		}
		db := string(data[offset : offset+int(dbLen)])
		offset += int(dbLen)

		// collection
		if offset+4 > len(data) {
			break
		}
		collLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if offset+int(collLen) > len(data) {
			break
		}
		collection := string(data[offset : offset+int(collLen)])
		offset += int(collLen)

		infos = append(infos, x.WatchInfo{
			DB:         db,
			Collection: collection,
		})
	}

	return infos
}

type WatchCollection struct {
	db *bbolt.DB
}

func NewWatchCollection(ctx context.Context, db *DB) (wc *WatchCollection, err error) {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("NewWatchCollection")

	err = db.Underlying.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(wcBucketName))
		return err
	})
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &WatchCollection{db.Underlying}, nil
}

func (w *WatchCollection) LatestVersion(ctx context.Context) uint64 {
	var version uint64
	_ = w.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(wcBucketName))
		if b == nil {
			return nil
		}
		cursor := b.Cursor()
		for k, _ := cursor.Last(); k != nil; k, _ = cursor.Prev() {
			if id, ok := wcParseVersionKey(k); ok {
				version = id
				break
			}
		}
		return nil
	})
	return version
}

func (w *WatchCollection) Save(ctx context.Context, info []x.WatchInfo, version uint64) (oldVersion, nowVersion uint64) {
	_ = w.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(wcBucketName))
		if b == nil {
			return nil
		}

		// 找当前最大 version
		var curMax uint64
		cursor := b.Cursor()
		for k, _ := cursor.Last(); k != nil; k, _ = cursor.Prev() {
			if id, ok := wcParseVersionKey(k); ok {
				curMax = id
				break
			}
		}

		oldVersion = curMax

		// version 守卫：latestVersion >= version 不修改
		if curMax >= version {
			nowVersion = curMax
			return nil
		}

		encoded := encodeWatchInfos(info)
		if err := b.Put(wcVersionKey(version), encoded); err != nil {
			return err
		}

		nowVersion = version
		return nil
	})
	return oldVersion, nowVersion
}

func (w *WatchCollection) Get(ctx context.Context, version uint64) []x.WatchInfo {
	var result []x.WatchInfo
	_ = w.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(wcBucketName))
		if b == nil {
			return nil
		}
		raw := b.Get(wcVersionKey(version))
		if raw == nil {
			return nil
		}
		result = decodeWatchInfos(raw)
		return nil
	})
	return result
}

func (w *WatchCollection) DelLessThan(ctx context.Context, version uint64) {
	_ = w.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(wcBucketName))
		if b == nil {
			return nil
		}

		var toDelete [][]byte
		cursor := b.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			id, ok := wcParseVersionKey(k)
			if !ok {
				continue
			}
			if id < version {
				toDelete = append(toDelete, append([]byte(nil), k...))
			}
		}

		for _, k := range toDelete {
			b.Delete(k)
		}

		return nil
	})
}

func (w *WatchCollection) MarkSyncing(ctx context.Context, version uint64) {
	_ = w.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(wcBucketName))
		if b == nil {
			return nil
		}

		// 读 oldSyncingVersion
		oldRaw := b.Get([]byte(wcKeyMetaSyncing))
		var oldVersion uint64
		if oldRaw != nil {
			oldVersion = wcBytesToUint64(oldRaw)
		}

		// full sync 优先级最高，直接覆盖
		if version == math.MaxUint64 {
			return b.Put([]byte(wcKeyMetaSyncing), wcUint64ToBytes(math.MaxUint64))
		}

		// delta sync: version 必须大于 oldSyncingVersion 才能修改
		if oldVersion >= version {
			return nil
		}

		return b.Put([]byte(wcKeyMetaSyncing), wcUint64ToBytes(version))
	})
}

func (w *WatchCollection) NeedSyncing(ctx context.Context) (version uint64, need bool) {
	_ = w.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(wcBucketName))
		if b == nil {
			return nil
		}
		raw := b.Get([]byte(wcKeyMetaSyncing))
		if raw == nil {
			return nil
		}
		version = wcBytesToUint64(raw)
		need = true
		return nil
	})
	return version, need
}

func (w *WatchCollection) ClearSyncingAndMarkSynced(ctx context.Context, version uint64) {
	_ = w.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(wcBucketName))
		if b == nil {
			return nil
		}

		b.Delete([]byte(wcKeyMetaSyncing))
		return b.Put([]byte(wcKeyMetaSynced), wcUint64ToBytes(version))
	})
}

func (w *WatchCollection) LatestSynced(ctx context.Context) (version uint64) {
	_ = w.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(wcBucketName))
		if b == nil {
			return nil
		}
		raw := b.Get([]byte(wcKeyMetaSynced))
		if raw == nil {
			return nil
		}
		version = wcBytesToUint64(raw)
		return nil
	})
	return version
}

func (w *WatchCollection) Clear(ctx context.Context) {
	_ = w.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket([]byte(wcBucketName)); err != nil {
			if err == errors.ErrBucketNotFound {
				return nil
			}
			return err
		}
		_, err := tx.CreateBucket([]byte(wcBucketName))
		return err
	})
}
