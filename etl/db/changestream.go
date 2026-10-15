package db

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// StreamId 有序Id，顺序就表示 change stream 的产生顺序，遍历时必须根据此 Id 有序遍历
type StreamId = []byte
type StreamValue = bson.Raw
type ResumeToken = bson.Raw

var (
	ErrNotFoundSentPoint = errors.New("not found change stream sent point in DB")
)

type ChangeStreamIterator interface {

	// Next 第一次也必须调用 Next, Values() 才会有值。迭代完数据库中已有的值或者有错，返回 false
	// limit 表示本次最多返回多少个 value，实际返回的个数可以小于 limit
	//
	// 返回 false 表示没有更多可发送的数据（或者有错），此时不能再调用 Values()
	// 返回 true 表示 Values() 中至少有一个 value，即不会出现"成功但无数据"的情况，
	// 所以调用者只需要 for Next() 后直接使用 Values()，无需再判断 len(Values()) == 0
	//
	// 数据库中没有任何数据时（包括一条都没有、或者仅有已发送点且其后无新数据），
	// 返回 false 且 Err() == nil，这是正常的空状态，不是错误
	Next(ctx context.Context, limit int) (ok bool)

	// Values 必须按照 Save 顺序返回，value 都是有效的，不存在nil
	Values() []StreamValue
	// LastStreamId Values()中最后一个 value 的 StreamId
	LastStreamId() StreamId

	// Err 如果没有找到上一次发送的截止点，返回 ErrNotFoundSentPoint
	// 如果 context.Canceled, Err() 将返回此错误
	// nil if no error has occurred.
	// or unknown error
	Err() error
	Release(ctx context.Context)
}

// ChangeStreamDBer Save / DeleteAll 不会被并发调用，这两个方法相互之间也不会并发调用，
// 但是 Save / DeleteAll 与其它方法相互之间会并发调用，其它方法本身也可能并发调用
type ChangeStreamDBer interface {
	// Save resumeToken 可作为唯一key使用，不会传入nil; value 是真正的值，如果没有值，就传 nil
	// 必须按照 Save 的调用顺序有序的保存 resumeToken 与 value 的值，在迭代器获取 value 时，必须按照顺序返回 value
	// 返回的 id 必须是唯一的，可能作为 MarkSentUpTo 的一个参数，标记已发送点
	Save(ctx context.Context, resumeToken ResumeToken, value StreamValue) (id StreamId, err error)
	DeleteAll(ctx context.Context)

	// MarkSentUpTo 标记已发的截止 StreamId，表示在 id 之前的(也包括 id 本身这一条)都已发送
	// 如果 DB 中不存在 id 对应的数据，不做任何操作
	MarkSentUpTo(ctx context.Context, id StreamId)
	// AllNotSent 所有还没有发送的 change stream 数据
	AllNotSent(ctx context.Context) ChangeStreamIterator

	// LastStreamId 库中最后一条 Stream 的 id，无论其 value 是否为 nil 都可以
	LastStreamId() StreamId

	// ResumeToken resumeToken = nil: 表示没有有效的 ResumeToken
	// 应该返回 Save 存储的最后一条数据对应的 resumeToken
	ResumeToken(ctx context.Context) (resumeToken ResumeToken)
}

var streamDBer ChangeStreamDBer

func SetChangeStream(s ChangeStreamDBer) {
	streamDBer = s
}

func ChangeStream() ChangeStreamDBer {
	return streamDBer
}
