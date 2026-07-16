package config

import (
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-config/configs"
	"github.com/xpwu/go-db-mongo/mongodb/mongocache"
)

type etl struct {
	Deployment       mongocache.Config
	FullDocument     bool          `conf:",https://www.mongodb.com/docs/v4.2/changeStreams/#lookup-full-document-for-update-operations"`
	WatchCollections []x.WatchInfo `conf:",init the WatchCollections"`
	SendToUrls       []string      `conf:",send in order until successful"`
}

var Etl = &etl{
	SendToUrls:       []string{"http://send/data/to"},
	FullDocument:     true,
	WatchCollections: []x.WatchInfo{{}},
	Deployment: mongocache.Config{
		MaxConn: 2,
	},
}

func init() {
	configs.Unmarshal(Etl)
}
