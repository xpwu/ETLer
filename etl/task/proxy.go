package task

import (
	"context"
	"fmt"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/go-httpclient/httpc"
	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

type Type byte

func (t Type)String() string {
	switch t {
	case Sync:
		return "sync"
	case ChangeStream:
		return "change-stream"
	}

	return "<unknown>"
}

const (
	Sync = iota
	ChangeStream
)

type Proxy interface {
	Do(ctx context.Context, ty Type, data []bson.Raw) (ok bool)
}

var Sender Proxy = &http{}

type Request struct {
	T Type
	data []bson.Raw
}

type Response struct {
}

type http struct {

}

func (h *http) Do(ctx context.Context, ty Type, data []bson.Raw) (ok bool) {
	ctx, logger := log.WithCtx(ctx)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	logger.PushPrefix(fmt.Sprintf("send: %s, len(data): %d", ty, len(data)))
	r := &Request{
		T:    ty,
		data: data,
	}
	err := httpc.Send(ctx, config.Etl.SendToUrl, httpc.WithStructBodyToJson(r))
	if err != nil {
		logger.Error(err)
		return false
	}

	return true
}

