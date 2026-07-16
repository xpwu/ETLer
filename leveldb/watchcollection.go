package leveldb

import (
	"context"
	"encoding/json"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"path"
)

type state struct {
	latestVersion       uint64 `json:"lver"`
	latestSyncVersion   uint64 `json:"lsyncver"`
	fullSyncing         bool   `json:"fsing"`
	deltaSyncingVersion uint64 `json:"dver"`
}

type watchCollection struct {
	db *leveldb.DB
}

func newWatchCollection(root string) *watchCollection {
	p := path.Join(root, "watchcollections")
	ldb, err := leveldb.OpenFile(p, nil)
	if err != nil {
		panic(err)
	}
	return &watchCollection{db: ldb}
}

const (
	latestVersion  = iota
	latestSyncVersion =
)

func toJson(d []config.WatchInfo) []byte {
	r, err := json.Marshal(&wi{D: d})
	if err != nil {
		panic(err)
	}

	return r
}

func fromJson(d []byte) []config.WatchInfo {
	r := &wi{}
	err := json.Unmarshal(d, r)
	if err != nil {
		panic(err)
	}

	return r.D
}

func (c *watchCollection) Save(ctx context.Context, i []config.WatchInfo, version uint64) (latestVersion uint64) {
	latestVersion = db.ConfigVersion
	err := c.db.Put([]byte(watchCollKey), toJson(i), nil)
	if err != nil {
		panic(err)
	}

	return
}

func (c *watchCollection) LatestVersion(ctx context.Context) uint64 {
	return db.ConfigVersion
}

func (c *watchCollection) Get(ctx context.Context, version uint64) []config.WatchInfo {

}

func (c *watchCollection) DelLessThan(ctx context.Context, version uint64) {

}

// 不保证并发安全

func (c *watchCollection) NeedFullSyncing(ctx context.Context) bool {

}

func (c *watchCollection) MarkFullSyncing(ctx context.Context) {

}

func (c *watchCollection) NeedDeltaSyncing(ctx context.Context) (version uint64, need bool) {

}

func (c *watchCollection) DeltaSyncing(ctx context.Context, version uint64) {

}

func (c *watchCollection) ClearSyncingAndMarkSynced(ctx context.Context, version uint64) {

}

func (c *watchCollection) LatestSynced(ctx context.Context) (version uint64) {

}
