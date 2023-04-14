package config

import (
	"github.com/xpwu/go-config/configs"
	"github.com/xpwu/go-db-mongo/mongodb/mongocache"
)

type WatchInfo struct {
	DB         string
	Collection string
}

func (w WatchInfo) Id() string {
	return w.DB + "." + w.Collection
}

type etl struct {
	Deployment       mongocache.Config
	FullDocument     bool `conf:",https://www.mongodb.com/docs/v4.2/changeStreams/#lookup-full-document-for-update-operations"`
	WatchCollections []WatchInfo
	SendToUrl        string
}

var Etl = &etl{
	SendToUrl:        "http://send/data/to",
	FullDocument:     true,
	WatchCollections: []WatchInfo{{}},
	Deployment: mongocache.Config{
		MaxConn: 2,
	},
}

func init() {
	configs.Unmarshal(Etl)
}
