package x

import "go.mongodb.org/mongo-driver/bson"

type WatchInfo struct {
	DB         string
	Collection string
}

func (w WatchInfo) Id() string {
	return w.DB + "." + w.Collection
}

type StreamId = []byte

type StreamValue = bson.Raw
