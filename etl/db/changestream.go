package db

import (
	"context"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// StreamId 有序Id，顺序就表示 change stream 的产生顺序，遍历时必须根据此 Id 有序遍历
type StreamId = []byte
type StreamValue = bson.Raw
type ResumeToken = bson.Raw

type ChangeStreamIterator interface {
	// First value 都不为nil
	//First(ctx context.Context) (id StreamId, value StreamValue, ok bool)
	//Last(ctx context.Context) (id x.StreamId, ok bool)

	// Next 必须按照StreamId顺序返回，values 都是有效值，len(values) <= limit
	Next(ctx context.Context, limit int) (values []StreamValue, lastId StreamId, ok bool)

	Release()
}

// ChangeStreamDBer Save 不会被并发调用，但是 Save 与其它方法相互之间会并发调用，其它方法本身也可能并发调用
type ChangeStreamDBer interface {
	// Save resumeToken 可作为唯一key使用，不会传入nil; value 是真正的值，如果没有需要处理的value，传 nil
	Save(ctx context.Context, resumeToken ResumeToken, value StreamValue) (id StreamId)
	//Get(ctx context.Context, id x.StreamId) (value x.StreamValue, ok bool)

	AllValues(ctx context.Context) ChangeStreamIterator
	ValuesStartWith(ctx context.Context, id StreamId) ChangeStreamIterator

	// LastResumeToken resumeToken = nil: 表示没有有效的 ResumeToken
	LastResumeToken(ctx context.Context) (resumeToken ResumeToken)

	MarkSentUpTo(ctx context.Context, id StreamId)

	// DeleteLessThan(ctx context.Context, id StreamId)

	DeleteAll(ctx context.Context)
}

var streamDBer ChangeStreamDBer

func SetChangeStream(s ChangeStreamDBer) {
	streamDBer = s
}

func ChangeStream() ChangeStreamDBer {
	return streamDBer
}
