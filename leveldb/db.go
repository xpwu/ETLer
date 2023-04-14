package leveldb

import (
	"github.com/xpwu/ETLer/etl/db"
	"path/filepath"
)

func Init(root string) {
	root = filepath.Join(root, "db")
	c := newCache(root)
	db.SetCache(c)
	db.SetStream(newStream(root))
	db.SetSyncTask(newSyncTask(root))
	db.SetWatchCollection(c)
}
