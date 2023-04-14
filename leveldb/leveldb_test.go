package leveldb

import (
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"path"
	"testing"
)

func TestDel(t *testing.T) {
	a := assert.New(t)

	p := path.Join("./", "stream_test_db")
	ldb, err := leveldb.OpenFile(p, nil)
	a.Nil(err)

	err = ldb.Put([]byte{1}, []byte{1}, nil)
	a.Nil(err)

	d, err := ldb.Get([]byte{1}, nil)
	a.Nil(err)
	a.Equal([]byte{1}, d)

	err = ldb.Close()
	a.Nil(err)

	err = os.RemoveAll(p)
	a.Nil(err)

	p = path.Join("./", "stream_test_db")
	ldb, err = leveldb.OpenFile(p, nil)
	a.Nil(err)

	err = ldb.Put([]byte{2}, []byte{2}, nil)
	a.Nil(err)

	d, err = ldb.Get([]byte{1}, nil)
	a.Equal(leveldb.ErrNotFound, err)

	d, err = ldb.Get([]byte{2}, nil)
	a.Nil(err)
	a.Equal([]byte{2}, d)
}
