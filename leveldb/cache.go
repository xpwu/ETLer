package leveldb

import (
	"context"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/xpwu/ETLer/x"
	"path"
)

type cache struct {
	db *leveldb.DB
}

const (
	resumeKey     = "resume"
	sentStreamKey = "sentstream"
)

func (c *cache) ResumeToken(ctx context.Context) (token x.ResumeToken, ok bool) {
	r, err := c.db.Get([]byte(resumeKey), nil)
	if err == leveldb.ErrNotFound {
		return nil, false
	}

	if err != nil {
		panic(err)
	}

	return r, true
}

func (c *cache) SaveResumeToken(ctx context.Context, token x.ResumeToken) {
	err := c.db.Put([]byte(resumeKey), token, nil)
	if err != nil {
		panic(err)
	}
}

func (c *cache) SentStreamId(ctx context.Context) (id x.StreamId, ok bool) {
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

func (c *cache) SaveSentStreamId(ctx context.Context, id x.StreamId) {
	err := c.db.Put([]byte(sentStreamKey), []byte(id), nil)
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
