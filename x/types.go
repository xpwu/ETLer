package x

type WatchInfo struct {
	DB         string
	Collection string
}

func (w WatchInfo) Id() string {
	return w.DB + "." + w.Collection
}
