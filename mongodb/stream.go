package mongodb

import (
	"context"
	"encoding/binary"
	"github.com/xpwu/ETLer/etl/db"
	"github.com/xpwu/ETLer/x"
	"github.com/xpwu/go-mongodb/index"
	"github.com/xpwu/go-mongodb/projection"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

//go:generate go run github.com/xpwu/go-mongodb/cmd/gomongodbgen
type stream struct {
	Token bson.Binary `bson:"_id"`
	Value x.StreamValue
	Seq   uint64
}

type streamIter struct {
	cursor *mongo.Cursor
}

func (si *streamIter) First(ctx context.Context) (id x.StreamId, value x.StreamValue, ok bool) {
	ok = si.cursor.Next(ctx)
	if !ok {
		if si.cursor.Err() != nil {
			panic(si.cursor.Err())
		}
		return nil, nil, false
	}

	var ret stream
	err := si.cursor.Decode(&ret)
	if err != nil {
		panic(err)
	}

	return ret.Token.Data, ret.Value, true
}

//func (si *streamIter) Last(ctx context.Context) (id x.StreamId, ok bool) {
//
//}

// Next 必须按照StreamId顺序返回
func (si *streamIter) Next(ctx context.Context, limit int) (values []x.StreamValue, lastId x.StreamId, ok bool) {
	st := &stream{}
	var err error

	for i := 0; i < limit; i++ {
		ok = si.cursor.Next(ctx)
		if !ok {
			err = si.cursor.Err()
			break
		}

		err = si.cursor.Decode(st)
		if err != nil {
			break
		}
		values = append(values, st.Value)
		lastId = toStreamId(st.Seq)
	}

	if len(values) != 0 {
		return values, lastId, true
	}

	if err != nil {
		panic(err)
	}

	return nil, nil, false
}

func (si *streamIter) Release() {
	_ = si.cursor.Close(context.Background())
}

type StreamDBer struct {
	seq    uint64
	Client *mongo.Client
	DB     string
}

func toStreamId(id uint64) x.StreamId {
	ret := make([]byte, 8)
	binary.BigEndian.PutUint64(ret, id)
	return ret
}

func toUint64(streamId x.StreamId) uint64 {
	return binary.BigEndian.Uint64(streamId)
}

func (sd *StreamDBer) coll() *mongo.Collection {
	return sd.Client.Database(sd.DB).Collection(streamColl.DefaultName)
}

func (sd *StreamDBer) readPrimaryColl() *mongo.Collection {
	return sd.Client.Database(sd.DB).Collection(streamColl.DefaultName,
		options.Collection().SetReadPreference(readpref.Primary()))
}

func GetIndexKeys() []index.Key {
	return []index.Key{streamColl.SeqF().AscIndex(index.Unique())}
}

// Save token: StreamValue的一个唯一值，常用 resume token
// Save 不会被并发调用，但是 Save 与其它方法会并发调用
func (sd *StreamDBer) Save(ctx context.Context, token []byte, value x.StreamValue) (id x.StreamId) {
	seq := sd.seq + 1

	_, err := sd.coll().InsertOne(ctx, stream{
		Seq:   seq,
		Value: value,
		Token: bson.Binary{Data: token},
	})

	if err == nil {
		sd.seq = seq
		return toStreamId(seq)
	}

	// seq 每次+1  所以不考虑 seq 重复的情况
	if mongo.IsDuplicateKeyError(err) {
		res := sd.readPrimaryColl().FindOne(ctx, streamColl.TokenF().Eq(bson.Binary{Data: token}),
			options.FindOne().SetProjection(projection.Include(streamColl.SeqF()).Exclude_id()))
		ret := &stream{}
		err := res.Decode(&ret)
		if err != nil {
			panic(err)
		}

		return toStreamId(ret.Seq)
	}

	panic(err)
}

//func (sd *StreamDBer) Get(ctx context.Context, id x.StreamId) (value x.StreamValue, ok bool) {
//	res := sd.coll().FindOne(ctx, streamDoc.TokenF().Eq(bson.Binary{Data: id}),
//		options.FindOne().SetProjection(projection.Include(streamDoc.ValueF()).Exclude_id()))
//
//	var ret stream
//	err := res.Decode(&ret)
//
//	if err == mongo.ErrNoDocuments {
//		return nil, false
//	}
//
//	if err != nil {
//		panic(err)
//	}
//
//	return ret.Value, true
//}

func (sd *StreamDBer) All(ctx context.Context) db.StreamIterator {
	return sd.StartWith(ctx, toStreamId(0))
}

func (sd *StreamDBer) StartWith(ctx context.Context, id x.StreamId) db.StreamIterator {
	cursor, err := sd.coll().Find(ctx, streamColl.SeqF().Gte(toUint64(id)),
		options.Find().SetSort(bson.D{{streamColl.SeqF().FullName(), 1}}))

	if err != nil {
		panic(err)
	}

	return &streamIter{cursor: cursor}
}
