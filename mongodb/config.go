package mongodb

import "github.com/xpwu/go-mongodb/client"

type serverWorkingDB struct {
	Deployment client.Config `conf:",Server working db. MongoDB when URI is set; LevelDB otherwise"`
	DB         string
}
