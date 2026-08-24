package config

import (
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-config/configs"
	"github.com/xpwu/go-mongodb/client"
)

type watch struct {
	Deployment   client.Config `conf:", Watching DB"`
	FullDocument bool          `conf:",https://www.mongodb.com/docs/v4.2/changeStreams/#lookup-full-document-for-update-operations"`
	Collections  []x.WatchInfo `conf:",init the WatchCollections"`
	SendToUrls   []string      `conf:",send in order until successful"`
}

var Watch = &watch{
	SendToUrls:   []string{"http://send/data/to"},
	FullDocument: true,
	Collections:  []x.WatchInfo{{}},
	Deployment: client.Config{
		MaxConn: 2,
	},
}

func init() {
	configs.Unmarshal(Watch)
}
