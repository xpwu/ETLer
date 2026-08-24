package task

import (
	"context"
	"errors"
	"fmt"
	"github.com/xpwu/ETLer/etl/config"
	"github.com/xpwu/go-httpclient/httpc"
	"github.com/xpwu/go-log/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"net/url"
	"time"
)

type Type byte

func (t Type) String() string {
	switch t {
	case Sync:
		return "sync"
	case ChangeStream:
		return "change-stream"
	}

	return "<unknown>"
}

const (
	Sync Type = iota
	ChangeStream
)

var (
	canceledErr   = context.Canceled
	sendFailedErr = errors.New("send failed")
)

type Proxy interface {
	Do(ctx context.Context, ty Type, db, coll string, data []bson.Raw) (err error)
}

var Sender Proxy = &http{}

type ns struct {
	DB   string
	Coll string
}

type Request struct {
	T Type
	// T == ChangeStream, Ns = {DB: "", Coll: ""}
	Ns   ns
	Data []bson.Raw
}

type Response struct {
}

type http struct {
}

func (h *http) doOne(ctx context.Context, r *Request, url string) (err error) {
	ctx, logger := log.WithCtx(ctx)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = httpc.Send(ctx, url, httpc.WithStructBodyToJson(r))
	if err != nil {
		logger.Warning(err)
		return
	}

	return
}

func (h *http) Do(ctx context.Context, ty Type, db, coll string, data []bson.Raw) (err error) {
	ctx, logger := log.WithCtx(ctx)
	r := &Request{
		T: ty,
		Ns: ns{
			DB:   db,
			Coll: coll,
		},
		Data: data,
	}
	logger.PushPrefix(fmt.Sprintf("send: %s", ty))

	for _, url_ := range config.Watch.SendToUrls {
		logger.PushPrefix(fmt.Sprintf("to: %s", url_))
		err = h.doOne(ctx, r, url_)
		if e, ok := err.(*url.Error); ok && e.Timeout() {
			time.Sleep(1 * time.Second)
			err = h.doOne(ctx, r, url_)
		}
		if ctx.Err() == context.Canceled {
			return canceledErr
		}
		if err == nil {
			return
		}
		logger.PopPrefix()
	}

	logger.Error("failed")

	return sendFailedErr
}
