package db

var stream StreamDBer

func SetStream(s StreamDBer)  {
  stream = s
}

func Stream() StreamDBer {
  return stream
}

var watchCollection WatchCollectionDBer

func SetWatchCollection(s WatchCollectionDBer)  {
  watchCollection = s
}

func WatchCollection() WatchCollectionDBer {
  return watchCollection
}

var syncTask SyncTaskDBer

func SetSyncTask(s SyncTaskDBer) {
  syncTask = s
}

func SyncTask() SyncTaskDBer {
  return syncTask
}

var cache CacheDBer

func SetCache(s CacheDBer) {
  cache = s
}

func Cache() CacheDBer {
  return cache
}
