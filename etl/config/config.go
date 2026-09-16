package config

import (
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-config/configs"
	"github.com/xpwu/go-mongodb/client"
)

type watch struct {
	Deployment  client.Config `conf:", Watching DB"`
	Collections []x.WatchInfo `conf:",init the WatchCollections"`
	SendToUrls  []string      `conf:",send in order until successful"`
}

var Watch = &watch{
	SendToUrls:  []string{"http://send/data/to"},
	Collections: []x.WatchInfo{{}},
	Deployment: client.Config{
		MaxConn: 2,
	},
}

func init() {
	configs.Unmarshal(Watch)
}
