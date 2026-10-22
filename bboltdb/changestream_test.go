package bboltdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xpwu/ETLer/etl/db"
	"go.etcd.io/bbolt"
)

func newTestChangeStream(t *testing.T) *ChangeStream {
	t.Helper()
	cs, err := NewChangeStream(context.Background(), newTestDB(t))
	if err != nil {
		t.Fatalf("NewChangeStream failed: %v", err)
	}
	return cs
}

// mustSave value == "" 表示保存一条 nil value（不需要发送的数据）
func mustSave(t *testing.T, cs *ChangeStream, token, value string) db.StreamId {
	t.Helper()

	var rt db.ResumeToken
	if token != "" {
		rt = db.ResumeToken([]byte(token))
	}
	var v db.StreamValue
	if value != "" {
		v = db.StreamValue([]byte(value))
	}

	id, err := cs.Save(context.Background(), rt, v)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	return id
}

func readLastSent(t *testing.T, cs *ChangeStream) (uint64, bool) {
	t.Helper()

	var v uint64
	ok := false
	_ = cs.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		if b == nil {
			return nil
		}
		if raw := b.Get([]byte(csKeyLastSent)); raw != nil {
			v = csBytesToUint64(raw)
			ok = true
		}
		return nil
	})
	return v, ok
}

func countStreamKeys(t *testing.T, cs *ChangeStream) int {
	t.Helper()

	n := 0
	_ = cs.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(csBucketName))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if _, ok := csParseStreamKey(k); ok {
				n++
			}
		}
		return nil
	})
	return n
}

// ---- key 编解码 ----

// 这是 %016x 编解码是否配对的关键用例
func TestCsKeyStreamParseRoundTrip(t *testing.T) {
	for _, id := range []uint64{0, 1, 2, 15, 16, 255, 256, 65535, 1 << 32, ^uint64(0)} {
		key := csKeyStream(id)
		if len(key) != len(csKeyPrefixStream)+16 {
			t.Fatalf("id=%d: key len = %d, want %d (key=%q)", id, len(key), len(csKeyPrefixStream)+16, key)
		}
		got, ok := csParseStreamKey(key)
		if !ok {
			t.Fatalf("id=%d: csParseStreamKey failed, key=%q", id, key)
		}
		if got != id {
			t.Fatalf("id=%d: got %d", id, got)
		}
	}
}

// 字典序必须等于数值序，否则 cursor.Seek 定位会错
func TestCsKeyStreamOrder(t *testing.T) {
	prev := csKeyStream(0)
	for _, id := range []uint64{1, 2, 9, 10, 100, 1 << 20} {
		cur := csKeyStream(id)
		if string(cur) <= string(prev) {
			t.Fatalf("id=%d: key %q not after %q", id, cur, prev)
		}
		prev = cur
	}
}

func TestCsParseStreamKeyRejects(t *testing.T) {
	cases := [][]byte{
		[]byte(csKeyLastSent),              // 非 stream 键，长度不足
		[]byte(csKeyPrefixStream),          // 只有前缀
		[]byte("xtream:0000000000000001"),  // 前缀不符但长度相同
		[]byte("stream:00000000000000001"), // 17 位，长度超限
		append(csKeyStream(1), 'x'),        // 长度超限
	}
	for _, c := range cases {
		if _, ok := csParseStreamKey(c); ok {
			t.Errorf("csParseStreamKey(%q) should return false", c)
		}
	}
}

// 与 wcParseVersionKey 同源：Sscanf 对宽度内的非法字符宽容，
// "…000g" 会被解析为前 15 个 0（即 0）。见 TestWcParseVersionKeyToleratesTrailingGarbage。
func TestCsParseStreamKeyToleratesTrailingGarbage(t *testing.T) {
	id, ok := csParseStreamKey([]byte("stream:000000000000000g"))
	if !ok {
		t.Fatal("当前实现对宽度内的非法字符是宽容的，ok 应为 true")
	}
	if id != 0 {
		t.Fatalf("id = %d, want 0", id)
	}
}

func TestCsEntryEncodeDecode(t *testing.T) {
	cases := []*csEntry{
		{resumeToken: nil, value: nil},
		{resumeToken: []byte("t"), value: nil},
		{resumeToken: nil, value: []byte("v")},
		{resumeToken: []byte("token-abc"), value: []byte("value-xyz")},
	}
	for _, e := range cases {
		got := csDecodeEntry(csEncodeEntry(e))
		if got == nil {
			t.Fatalf("decode %+v: got nil", e)
		}
		if !bytes.Equal(got.resumeToken, e.resumeToken) {
			t.Errorf("resumeToken: got %q, want %q", got.resumeToken, e.resumeToken)
		}
		if !bytes.Equal(got.value, e.value) {
			t.Errorf("value: got %q, want %q", got.value, e.value)
		}
	}
}

func TestCsDecodeEntryRejectsCorrupted(t *testing.T) {
	if csDecodeEntry(nil) != nil {
		t.Error("nil data should return nil")
	}
	if csDecodeEntry([]byte{1, 2}) != nil {
		t.Error("len < 4 should return nil")
	}
	if csDecodeEntry([]byte{0, 0, 0, 0xff, 0}) != nil {
		t.Error("tokenLen overflow should return nil")
	}
}

func TestCsUint64BytesRoundTrip(t *testing.T) {
	for _, id := range []uint64{0, 1, 255, 1 << 32, ^uint64(0)} {
		if got := csBytesToUint64(csUint64ToBytes(id)); got != id {
			t.Fatalf("got %d, want %d", got, id)
		}
	}
	if csBytesToUint64([]byte{1, 2, 3}) != 0 {
		t.Error("len < 8 should return 0")
	}
}

// ---- Save / ResumeToken / LastStreamId ----

func TestChangeStreamSaveAndResumeToken(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	if rt := cs.ResumeToken(ctx); rt != nil {
		t.Fatalf("empty db: ResumeToken should be nil, got %v", []byte(rt))
	}
	if csBytesToUint64(cs.LastStreamId()) != 0 {
		t.Fatal("empty db: LastStreamId should be 0")
	}

	id1 := mustSave(t, cs, "t1", "v1")
	id2 := mustSave(t, cs, "t2", "") // nil value
	id3 := mustSave(t, cs, "t3", "v3")

	for want, id := range map[uint64]db.StreamId{1: id1, 2: id2, 3: id3} {
		if csBytesToUint64(id) != want {
			t.Fatalf("Save id = %d, want %d", csBytesToUint64(id), want)
		}
	}

	if csBytesToUint64(cs.LastStreamId()) != 3 {
		t.Fatalf("LastStreamId = %d, want 3", csBytesToUint64(cs.LastStreamId()))
	}
	// 最后一条保存的就是最新的 resumeToken，无论其 value 是否为 nil
	if rt := cs.ResumeToken(ctx); string(rt) != "t3" {
		t.Fatalf("ResumeToken = %q, want %q", rt, "t3")
	}
}

func TestChangeStreamDeleteAll(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	mustSave(t, cs, "t1", "a")
	cs.MarkSentUpTo(ctx, csUint64ToBytes(1))
	cs.DeleteAll(ctx)

	if csBytesToUint64(cs.LastStreamId()) != 0 {
		t.Fatal("DeleteAll 后 LastStreamId 应为 0")
	}
	if cs.ResumeToken(ctx) != nil {
		t.Fatal("DeleteAll 后 ResumeToken 应为 nil")
	}
	if n := countStreamKeys(t, cs); n != 0 {
		t.Fatalf("DeleteAll 后还剩 %d 条", n)
	}
	if _, ok := readLastSent(t, cs); ok {
		t.Fatal("DeleteAll 后不应有 lastsent")
	}

	// seq 必须重置，重新从 1 开始
	if id := mustSave(t, cs, "t2", "b"); csBytesToUint64(id) != 1 {
		t.Fatalf("DeleteAll 后首个 id = %d, want 1", csBytesToUint64(id))
	}
}

// ---- Next ----

func TestChangeStreamNextOnEmptyDB(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	it := cs.AllNotSent(ctx)
	defer it.Release(ctx)

	if it.Next(ctx, 10) {
		t.Fatal("empty db: Next should be false")
	}
	// 空库是正常状态，不是错误
	if err := it.Err(); err != nil {
		t.Fatalf("empty db 不是错误, got %v", err)
	}
}

func TestChangeStreamNextWithoutSentPoint(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	mustSave(t, cs, "t1", "v1")

	it := cs.AllNotSent(ctx)
	defer it.Release(ctx)

	if it.Next(ctx, 10) {
		t.Fatal("没有已发送点时 Next 应为 false")
	}
	if !errors.Is(it.Err(), db.ErrNotFoundSentPoint) {
		t.Fatalf("Err = %v, want ErrNotFoundSentPoint", it.Err())
	}
}

// nil value 不投递，LastStreamId 必须是 Values() 的最后一条
func TestChangeStreamNextSkipsNilValue(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	mustSave(t, cs, "t1", "")  // 1: nil
	mustSave(t, cs, "t2", "a") // 2
	mustSave(t, cs, "t3", "")  // 3: nil
	mustSave(t, cs, "t4", "b") // 4
	mustSave(t, cs, "t5", "")  // 5: nil

	cs.MarkSentUpTo(ctx, csUint64ToBytes(1))

	it := cs.AllNotSent(ctx)
	defer it.Release(ctx)

	if !it.Next(ctx, 10) {
		t.Fatalf("Next false, err = %v", it.Err())
	}

	values := it.Values()
	if len(values) != 2 {
		t.Fatalf("len(Values()) = %d, want 2", len(values))
	}
	if string(values[0]) != "a" || string(values[1]) != "b" {
		t.Fatalf("Values() = %q, want [a b]", values)
	}
	// 不能是尾部 nil 的 id=5
	if csBytesToUint64(it.LastStreamId()) != 4 {
		t.Fatalf("LastStreamId = %d, want 4", csBytesToUint64(it.LastStreamId()))
	}
}

// 尾部全是 nil：Next 返回 false 且不是错误（回归 lastsent 被写成 0 导致的死循环）
func TestChangeStreamNextTailNilReturnsFalse(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	mustSave(t, cs, "t1", "a")
	mustSave(t, cs, "t2", "")
	mustSave(t, cs, "t3", "")

	cs.MarkSentUpTo(ctx, csUint64ToBytes(1))

	it := cs.AllNotSent(ctx)
	defer it.Release(ctx)

	if it.Next(ctx, 10) {
		t.Fatal("尾部全是 nil, Next 应为 false")
	}
	if err := it.Err(); err != nil {
		t.Fatalf("尾部全是 nil 不是错误, got %v", err)
	}
}

func TestChangeStreamNextLimit(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	mustSave(t, cs, "t1", "")
	for i := 2; i <= 6; i++ {
		mustSave(t, cs, fmt.Sprintf("t%d", i), fmt.Sprintf("v%d", i))
	}
	cs.MarkSentUpTo(ctx, csUint64ToBytes(1))

	it := cs.AllNotSent(ctx)
	defer it.Release(ctx)

	if !it.Next(ctx, 2) {
		t.Fatal("first batch should be true")
	}
	if len(it.Values()) != 2 || csBytesToUint64(it.LastStreamId()) != 3 {
		t.Fatalf("batch1: len=%d last=%d", len(it.Values()), csBytesToUint64(it.LastStreamId()))
	}
	if !it.Next(ctx, 2) {
		t.Fatal("second batch should be true")
	}
	if csBytesToUint64(it.LastStreamId()) != 5 {
		t.Fatalf("batch2 last = %d, want 5", csBytesToUint64(it.LastStreamId()))
	}
	if !it.Next(ctx, 2) {
		t.Fatal("third batch should be true")
	}
	if len(it.Values()) != 1 || csBytesToUint64(it.LastStreamId()) != 6 {
		t.Fatalf("batch3: len=%d last=%d", len(it.Values()), csBytesToUint64(it.LastStreamId()))
	}
	if it.Next(ctx, 2) {
		t.Fatal("no more data")
	}
}

func TestChangeStreamIterBasics(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	it := cs.AllNotSent(ctx)
	if it.LastStreamId() != nil {
		t.Fatal("无数据时 LastStreamId 应为 nil")
	}

	it.Release(ctx)
	it.Release(ctx) // 重复 Release 不应 panic
	if it.Next(ctx, 10) {
		t.Fatal("Release 后 Next 应为 false")
	}
}

// ---- MarkSentUpTo ----

// 已发送点之后连续的 nil 等价于已处理完，可以直接跨过
func TestMarkSentUpToSkipsFollowingNil(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	mustSave(t, cs, "t1", "a")
	mustSave(t, cs, "t2", "")
	mustSave(t, cs, "t3", "")
	mustSave(t, cs, "t4", "b")

	cs.MarkSentUpTo(ctx, csUint64ToBytes(1))

	if v, ok := readLastSent(t, cs); !ok || v != 3 {
		t.Fatalf("lastsent = %d (ok=%v), want 3", v, ok)
	}

	// 有效数据 b 不能被跨过
	it := cs.AllNotSent(ctx)
	defer it.Release(ctx)
	if !it.Next(ctx, 10) {
		t.Fatalf("b 还未发送, 不应被跨过: err = %v", it.Err())
	}
	if len(it.Values()) != 1 || string(it.Values()[0]) != "b" {
		t.Fatalf("Values() = %q, want [b]", it.Values())
	}
}

// 回归：批次边界曾经把未发送的有效数据一起跨过，导致丢数据
func TestMarkSentUpToDoesNotSkipUnsentValue(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		mustSave(t, cs, fmt.Sprintf("t%d", i), fmt.Sprintf("v%d", i))
	}
	cs.MarkSentUpTo(ctx, csUint64ToBytes(1))

	it := cs.AllNotSent(ctx)
	defer it.Release(ctx)

	if !it.Next(ctx, 1) {
		t.Fatalf("Next false, err = %v", it.Err())
	}
	last := it.LastStreamId()
	if csBytesToUint64(last) != 2 {
		t.Fatalf("LastStreamId = %d, want 2", csBytesToUint64(last))
	}
	cs.MarkSentUpTo(ctx, last)

	it2 := cs.AllNotSent(ctx)
	defer it2.Release(ctx)
	if !it2.Next(ctx, 10) {
		t.Fatalf("id=3 被跨过（丢数据）: err = %v", it2.Err())
	}
	if string(it2.Values()[0]) != "v3" {
		t.Fatalf("Values()[0] = %q, want v3", it2.Values()[0])
	}
}

func TestMarkSentUpToNoRollback(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		mustSave(t, cs, fmt.Sprintf("t%d", i), fmt.Sprintf("v%d", i))
	}

	cs.MarkSentUpTo(ctx, csUint64ToBytes(3))
	cs.MarkSentUpTo(ctx, csUint64ToBytes(2))

	if v, _ := readLastSent(t, cs); v != 3 {
		t.Fatalf("lastsent = %d, want 3 (不能回退)", v)
	}
}

// 契约：DB 中不存在 id 对应的数据，不做任何操作
func TestMarkSentUpToNotExistIsNoOp(t *testing.T) {
	cs := newTestChangeStream(t)
	ctx := context.Background()

	mustSave(t, cs, "t1", "a")
	cs.MarkSentUpTo(ctx, csUint64ToBytes(999))

	if _, ok := readLastSent(t, cs); ok {
		t.Fatal("id 不存在时不应写入 lastsent")
	}
}

// ---- Compact ----

func TestChangeStreamCompact(t *testing.T) {
	if testing.Short() {
		t.Skip("慢用例：需写入 csCompactThreshold+1 条")
	}

	cs := newTestChangeStream(t)
	ctx := context.Background()

	n := int(csCompactThreshold) + 1
	last := db.StreamId(nil)
	for i := 1; i <= n; i++ {
		last = mustSave(t, cs, fmt.Sprintf("t%d", i), fmt.Sprintf("v%d", i))
	}
	cs.MarkSentUpTo(ctx, last)

	// 只保留最后一条已发送点
	if got := countStreamKeys(t, cs); got != 1 {
		t.Fatalf("Compact 后剩 %d 条, want 1", got)
	}
	// ResumeToken 必须仍然可用
	if rt := cs.ResumeToken(ctx); string(rt) != fmt.Sprintf("t%d", n) {
		t.Fatalf("ResumeToken = %q, want t%d", rt, n)
	}

	it := cs.AllNotSent(ctx)
	defer it.Release(ctx)
	if it.Next(ctx, 10) {
		t.Fatal("全部已发送, Next 应为 false")
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Err = %v, want nil", err)
	}
}

func TestChangeStreamCompactBelowThreshold(t *testing.T) {
	if testing.Short() {
		t.Skip("慢用例：需写入 csCompactThreshold-1 条")
	}

	cs := newTestChangeStream(t)
	ctx := context.Background()

	n := int(csCompactThreshold) - 1
	last := db.StreamId(nil)
	for i := 1; i <= n; i++ {
		last = mustSave(t, cs, fmt.Sprintf("t%d", i), fmt.Sprintf("v%d", i))
	}
	cs.MarkSentUpTo(ctx, last)

	if got := countStreamKeys(t, cs); got != n {
		t.Fatalf("未达阈值不应清理: got %d, want %d", got, n)
	}
}
