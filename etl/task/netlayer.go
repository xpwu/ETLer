package task

import (
	"context"
	"go.mongodb.org/mongo-driver/v2/bson"
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

type NetLayer interface {
	Do(ctx context.Context, ty Type, db, coll string, data []bson.Raw) (err error)
}
