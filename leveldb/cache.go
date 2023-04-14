package leveldb

import (
	"context"
	"encoding/json"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/ETLer/etl/db"
	"path"
)

type cache struct {
	db *leveldb.DB
}

const (
	resumeKey     = "resume"
	sentStreamKey = "sentstream"
	watchCollKey  = "watchcoll"
)

func (c *cache) ResumeToken(ctx context.Context) (token db.ResumeToken, ok bool) {
	r, err := c.db.Get([]byte(resumeKey), nil)
	if err == leveldb.ErrNotFound {
		return nil, false
	}

	if err != nil {
		panic(err)
	}

	return r, true
}

func (c *cache) SaveResumeToken(ctx context.Context, token db.ResumeToken) {
	err := c.db.Put([]byte(resumeKey), token, nil)
	if err != nil {
		panic(err)
	}
}

func (c *cache) SentStreamId(ctx context.Context) (id db.StreamId, ok bool) {
	r, err := c.db.Get([]byte(sentStreamKey), nil)
	if err == leveldb.ErrNotFound {
		return nil, false
	}

	if err != nil {
		panic(err)
	}

	return r, true
}

func (c *cache) DelSentStreamId(ctx context.Context) {
	err := c.db.Delete([]byte(sentStreamKey), nil)
	if err != nil {
		panic(err)
	}
}

func (c *cache) SaveSentStreamId(ctx context.Context, id db.StreamId) {
	err := c.db.Put([]byte(sentStreamKey), []byte(id), nil)
	if err != nil {
		panic(err)
	}
}

type wi struct {
	D []config.WatchInfo
}

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

func (c *cache) All(ctx context.Context) []config.WatchInfo {
	r, err := c.db.Get([]byte(watchCollKey), nil)
	if err == leveldb.ErrNotFound {
		return []config.WatchInfo{}
	}

	if err != nil {
		panic(err)
	}

	return fromJson(r)
}

func (c *cache) Save(ctx context.Context, i []config.WatchInfo) {
	err := c.db.Put([]byte(watchCollKey), toJson(i), nil)
	if err != nil {
		panic(err)
	}
}

func newCache(root string) *cache {
	p := path.Join(root, "cache")
	ldb, err := leveldb.OpenFile(p, nil)
	if err != nil {
		panic(err)
	}
	return &cache{db: ldb}
}
