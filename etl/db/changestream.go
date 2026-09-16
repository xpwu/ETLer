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
	// First value 可能为 nil
	First(ctx context.Context) (id StreamId, value StreamValue, ok bool)
	//Last(ctx context.Context) (id x.StreamId, ok bool)

	// Next 必须按照StreamId顺序返回
	Next(ctx context.Context, limit int) (values []StreamValue, lastId StreamId, ok bool)

	Release()
}

// ChangeStreamDBer Save 不会被并发调用，但是 Save 与其它方法会并发调用，其它接口可能并发
type ChangeStreamDBer interface {
	// Save resumeToken 可作为唯一key使用，不会传入nil; value 是真正的值，如果没有需要处理的value，传 nil
	Save(ctx context.Context, resumeToken ResumeToken, value StreamValue) (id StreamId)
	//Get(ctx context.Context, id x.StreamId) (value x.StreamValue, ok bool)

	All(ctx context.Context) ChangeStreamIterator
	StartWith(ctx context.Context, id StreamId) ChangeStreamIterator

	// LastResumeToken resumeToken = nil: 表示没有有效的 ResumeToken
	LastResumeToken(ctx context.Context) (resumeToken ResumeToken)
	DeleteUntil(ctx context.Context, id StreamId)
	DeleteAll(ctx context.Context)
}

var streamDBer ChangeStreamDBer

func SetChangeStream(s ChangeStreamDBer) {
	streamDBer = s
}

func ChangeStream() ChangeStreamDBer {
	return streamDBer
}
