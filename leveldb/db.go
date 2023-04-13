package leveldb

import (
	"github.com/xpwu/ETLer/etl/db"
	"path/filepath"
)

func Init(root string) {
	root = filepath.Join(root, "db")
	db.SetCache(newCache(root))
	db.SetStream(newStream(root))
	db.SetSyncTask(newSyncTask(root))
	db.SetWatchCollection(newCache(root))
}
