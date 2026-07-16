package leveldb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/xpwu/ETLer/x"
	"math"
	"path"
)

type watchCollection struct {
	db    *leveldb.DB
	oldWc *cache
}

func newWatchCollection(root string, oldWc *cache) *watchCollection {
	p := path.Join(root, "watchcollections")
	ldb, err := leveldb.OpenFile(p, nil)
	if err != nil {
		panic(err)
	}
	return &watchCollection{db: ldb, oldWc: oldWc}
}

const (
	configVersion uint64 = 0

	// available version 1 ~ reserved

	reserved            uint64 = math.MaxUint64 - 1000
	latestVersion              = reserved + 1
	latestSyncVersion          = reserved + 2
	deltaSyncingVersion        = reserved + 3
	fullSyncing                = reserved + 4
)

var (
	latestVersionKey       = uint64toKey(latestVersion)
	latestSyncVersionKey   = uint64toKey(latestSyncVersion)
	deltaSyncingVersionKey = uint64toKey(deltaSyncingVersion)
	fullSyncingKey         = uint64toKey(fullSyncing)
)

type wi struct {
	D []x.WatchInfo
}

var emptyWi = []byte("{\"D\":[]}")

func toJson(d []x.WatchInfo) []byte {
	r, err := json.Marshal(&wi{D: d})
	if err != nil {
		panic(err)
	}

	return r
}

func fromJson(d []byte) []x.WatchInfo {
	r := &wi{}
	err := json.Unmarshal(d, r)
	if err != nil {
		panic(err)
	}

	return r.D
}

func uint64toKey(k uint64) []byte {
	ret := make([]byte, 8, 8)
	binary.BigEndian.PutUint64(ret, k)
	return ret
}

func uint64toValue(k uint64) []byte {
	ret := make([]byte, 8, 8)
	binary.BigEndian.PutUint64(ret, k)
	return ret
}

func valueToUint64(v []byte) uint64 {
	return binary.BigEndian.Uint64(v)
}

type accessor interface {
	Get(key []byte, ro *opt.ReadOptions) ([]byte, error)
	Put(key, value []byte, wo *opt.WriteOptions) error
	Delete(key []byte, wo *opt.WriteOptions) error
	Has(key []byte, ro *opt.ReadOptions) (ret bool, err error)
}

func get(a accessor, key, defaultValue []byte) []byte {
	r, err := a.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return defaultValue
	}

	if err != nil {
		panic(err)
	}

	return r
}

func put(a accessor, key, value []byte) {
	err := a.Put(key, value, nil)
	if err != nil {
		panic(err)
	}
}

func has(a accessor, key []byte) bool {
	r, err := a.Has(key, nil)
	if err != nil {
		panic(err)
	}

	return r
}

func del(a accessor, key []byte) {
	err := a.Delete(key, nil)
	if err != nil {
		panic(err)
	}
}

func getLatestVersion(a accessor) uint64 {
	return valueToUint64(get(a, latestVersionKey, uint64toValue(configVersion)))
}

func getLatestSyncVersion(a accessor) uint64 {
	return valueToUint64(get(a, latestSyncVersionKey, uint64toValue(configVersion)))
}

func getDeltaSyncingVersion(a accessor) uint64 {
	return valueToUint64(get(a, deltaSyncingVersionKey, uint64toValue(configVersion)))
}

func (c *watchCollection) openTransaction() *leveldb.Transaction {
	tr, err := c.db.OpenTransaction()
	if err != nil {
		panic(err)
	}

	return tr
}

func (c *watchCollection) Save(ctx context.Context, i []x.WatchInfo, version uint64) (latestVersion uint64) {
	latestVersion = getLatestVersion(c.db)
	if latestVersion >= version {
		return latestVersion
	}
	if version >= reserved {
		return latestVersion
	}

	tr := c.openTransaction()
	defer tr.Discard()

	latestVersion = getLatestVersion(tr)
	if latestVersion >= version {
		return latestVersion
	}

	put(tr, uint64toKey(version), toJson(i))
	put(tr, latestVersionKey, uint64toValue(version))
	if err := tr.Commit(); err != nil {
		panic(err)
	}

	return
}

func (c *watchCollection) LatestVersion(ctx context.Context) uint64 {
	return getLatestVersion(c.db)
}

func (c *cache) all(ctx context.Context) []x.WatchInfo {
	r, err := c.db.Get([]byte("watchcoll"), nil)
	if err == leveldb.ErrNotFound {
		return []x.WatchInfo{}
	}

	if err != nil {
		panic(err)
	}

	return fromJson(r)
}

func (c *watchCollection) Get(ctx context.Context, version uint64) []x.WatchInfo {
	r := fromJson(get(c.db, uint64toKey(version), emptyWi))
	if len(r) != 0 || version != configVersion {
		return r
	}

	// 兼容旧的存储
	return c.oldWc.all(ctx)
}

func (c *watchCollection) DelLessThan(ctx context.Context, version uint64) {
	tr := c.openTransaction()
	defer tr.Discard()
	r := &util.Range{
		Start: nil,
		Limit: uint64toKey(version),
	}
	iter := tr.NewIterator(r, nil)
	cont := iter.First()
	for cont {
		del(tr, iter.Key())
		cont = iter.Next()
	}
	if err := tr.Commit(); err != nil {
		panic(err)
	}
}

// 不保证并发安全

func (c *watchCollection) NeedFullSyncing(ctx context.Context) bool {
	return has(c.db, fullSyncingKey)
}

func (c *watchCollection) MarkFullSyncing(ctx context.Context) {
	put(c.db, fullSyncingKey, uint64toValue(0))
}

func (c *watchCollection) NeedDeltaSyncing(ctx context.Context) (version uint64, need bool) {
	r, err := c.db.Get(deltaSyncingVersionKey, nil)
	if err == leveldb.ErrNotFound {
		return 0, false
	}

	if err != nil {
		panic(err)
	}

	return valueToUint64(r), true
}

func (c *watchCollection) DeltaSyncing(ctx context.Context, version uint64) {
	put(c.db, deltaSyncingVersionKey, uint64toValue(version))
}

func (c *watchCollection) ClearSyncingAndMarkSynced(ctx context.Context, version uint64) {
	del(c.db, deltaSyncingVersionKey)
	del(c.db, fullSyncingKey)
	put(c.db, latestSyncVersionKey, uint64toValue(version))
}

func (c *watchCollection) LatestSynced(ctx context.Context) (version uint64) {
	return valueToUint64(get(c.db, latestSyncVersionKey, uint64toValue(0)))
}
