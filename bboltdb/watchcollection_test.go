package bboltdb

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/xpwu/ETLer/x"
)

func newTestWatchCollection(t *testing.T) *WatchCollection {
	t.Helper()
	wc, err := NewWatchCollection(context.Background(), newTestDB(t))
	if err != nil {
		t.Fatalf("NewWatchCollection failed: %v", err)
	}
	return wc
}

func TestWcVersionKeyParseRoundTrip(t *testing.T) {
	for _, v := range []uint64{0, 1, 255, 1 << 32, math.MaxUint64 - 1, math.MaxUint64} {
		key := wcVersionKey(v)
		if len(key) != len(wcKeyPrefixVersion)+16 {
			t.Fatalf("v=%d: key len = %d, want %d (key=%q)", v, len(key), len(wcKeyPrefixVersion)+16, key)
		}
		got, ok := wcParseVersionKey(key)
		if !ok {
			t.Fatalf("v=%d: parse failed, key=%q", v, key)
		}
		if got != v {
			t.Fatalf("v=%d: got %d", v, got)
		}
	}
}

func TestWcParseVersionKeyRejects(t *testing.T) {
	cases := [][]byte{
		[]byte(wcKeyMetaSyncing), // meta 键，长度不足
		[]byte(wcKeyMetaSynced),
		[]byte(wcKeyPrefixVersion),          // 只有前缀
		[]byte("xersion:0000000000000001"),  // 前缀不符但长度相同
		[]byte("version:00000000000000001"), // 17 位，长度超限
		append(wcVersionKey(1), 'x'),        // 长度超限
	}
	for _, c := range cases {
		if _, ok := wcParseVersionKey(c); ok {
			t.Errorf("wcParseVersionKey(%q) should return false", c)
		}
	}
}

// Sscanf 的宽度是"最多读 N 个字符"，不是"必须读满 N 个合法字符"，
// 所以 "…000g" 会被解析为前 15 个 0（即 0）且不报错。
// 这类 key 只可能来自数据损坏，wcVersionKey 不会生成，因此不影响正确性。
// 此处把该行为固化下来：若将来改为严格解析，本用例会失败并提醒同步更新。
func TestWcParseVersionKeyToleratesTrailingGarbage(t *testing.T) {
	id, ok := wcParseVersionKey([]byte("version:000000000000000g"))
	if !ok {
		t.Fatal("当前实现对宽度内的非法字符是宽容的，ok 应为 true")
	}
	if id != 0 {
		t.Fatalf("id = %d, want 0", id)
	}
}

func TestWcUint64BytesRoundTrip(t *testing.T) {
	for _, v := range []uint64{0, 1, 255, 1 << 32, math.MaxUint64} {
		if got := wcBytesToUint64(wcUint64ToBytes(v)); got != v {
			t.Fatalf("got %d, want %d", got, v)
		}
	}
	if wcBytesToUint64([]byte{1, 2, 3}) != 0 {
		t.Error("len < 8 should return 0")
	}
}

func TestWatchInfosEncodeDecode(t *testing.T) {
	cases := [][]x.WatchInfo{
		nil,
		{},
		{{DB: "", Collection: ""}},
		{{DB: "db", Collection: "c"}},
		{{DB: "db1", Collection: "c1"}, {DB: "db2", Collection: "c2"}},
	}
	for _, infos := range cases {
		got := decodeWatchInfos(encodeWatchInfos(infos))
		if len(got) != len(infos) {
			t.Fatalf("len = %d, want %d", len(got), len(infos))
		}
		if len(infos) > 0 && !reflect.DeepEqual(got, infos) {
			t.Fatalf("got %+v, want %+v", got, infos)
		}
	}
}

// 关键回归：ConfigVersion == 0，空库时 version=0 必须能写入
func TestWatchCollectionSaveVersionZeroOnEmptyDB(t *testing.T) {
	wc := newTestWatchCollection(t)
	ctx := context.Background()

	if wc.LatestVersion(ctx) != 0 {
		t.Fatal("empty db: LatestVersion should be 0")
	}
	if len(wc.Get(ctx, 0)) != 0 {
		t.Fatal("empty db: Get(0) should be empty")
	}

	infos := []x.WatchInfo{{DB: "db", Collection: "c"}}
	old, now := wc.Save(ctx, infos, 0)
	if old != 0 || now != 0 {
		t.Fatalf("old=%d now=%d, want 0 0", old, now)
	}
	if got := wc.Get(ctx, 0); !reflect.DeepEqual(got, infos) {
		t.Fatalf("Get(0) = %+v, want %+v", got, infos)
	}

	// 已有版本记录后，同样的 version=0 不能再修改
	other := []x.WatchInfo{{DB: "db", Collection: "other"}}
	if old, now := wc.Save(ctx, other, 0); old != 0 || now != 0 {
		t.Fatalf("old=%d now=%d, want 0 0", old, now)
	}
	if got := wc.Get(ctx, 0); !reflect.DeepEqual(got, infos) {
		t.Fatalf("不应被覆盖: got %+v", got)
	}
}

func TestWatchCollectionSaveVersionGuard(t *testing.T) {
	wc := newTestWatchCollection(t)
	ctx := context.Background()

	v1 := []x.WatchInfo{{DB: "db", Collection: "c1"}}
	v2 := []x.WatchInfo{{DB: "db", Collection: "c2"}}
	v3 := []x.WatchInfo{{DB: "db", Collection: "c3"}}

	if _, now := wc.Save(ctx, v1, 1); now != 1 {
		t.Fatalf("now = %d, want 1", now)
	}
	if old, now := wc.Save(ctx, v2, 1); old != 1 || now != 1 {
		t.Fatalf("同版本不应修改: old=%d now=%d", old, now)
	}
	if old, now := wc.Save(ctx, v3, 0); old != 1 || now != 1 {
		t.Fatalf("更小版本不应修改: old=%d now=%d", old, now)
	}
	if old, now := wc.Save(ctx, v2, 2); old != 1 || now != 2 {
		t.Fatalf("old=%d now=%d, want 1 2", old, now)
	}

	if wc.LatestVersion(ctx) != 2 {
		t.Fatalf("LatestVersion = %d, want 2", wc.LatestVersion(ctx))
	}
	if got := wc.Get(ctx, 1); !reflect.DeepEqual(got, v1) {
		t.Fatalf("Get(1) = %+v", got)
	}
	if got := wc.Get(ctx, 2); !reflect.DeepEqual(got, v2) {
		t.Fatalf("Get(2) = %+v", got)
	}
}

func TestWatchCollectionDelLessThan(t *testing.T) {
	wc := newTestWatchCollection(t)
	ctx := context.Background()

	for _, v := range []uint64{1, 2, 3} {
		wc.Save(ctx, []x.WatchInfo{{DB: "db", Collection: fmt.Sprintf("c%d", v)}}, v)
	}
	wc.MarkSyncing(ctx, 3)

	wc.DelLessThan(ctx, 3)

	if len(wc.Get(ctx, 1)) != 0 {
		t.Fatal("v1 应被删除")
	}
	if len(wc.Get(ctx, 2)) != 0 {
		t.Fatal("v2 应被删除")
	}
	if len(wc.Get(ctx, 3)) == 0 {
		t.Fatal("v3 应保留")
	}
	// meta 键不能被误删
	if _, need := wc.NeedSyncing(ctx); !need {
		t.Fatal("meta:syncing 不应被 DelLessThan 删除")
	}
}

func TestWatchCollectionSyncingMarks(t *testing.T) {
	wc := newTestWatchCollection(t)
	ctx := context.Background()

	if _, need := wc.NeedSyncing(ctx); need {
		t.Fatal("初始不应有 syncing 标记")
	}
	if wc.LatestSynced(ctx) != 0 {
		t.Fatal("初始 LatestSynced 应为 0")
	}

	wc.MarkSyncing(ctx, 5)
	if v, need := wc.NeedSyncing(ctx); !need || v != 5 {
		t.Fatalf("NeedSyncing = %d,%v want 5,true", v, need)
	}

	wc.MarkSyncing(ctx, 3) // 更小，不修改
	if v, _ := wc.NeedSyncing(ctx); v != 5 {
		t.Fatalf("delta 更小不应修改: got %d", v)
	}

	wc.MarkSyncing(ctx, 7)
	if v, _ := wc.NeedSyncing(ctx); v != 7 {
		t.Fatalf("got %d, want 7", v)
	}

	// full sync 优先级最高
	wc.MarkSyncing(ctx, math.MaxUint64)
	if v, need := wc.NeedSyncing(ctx); !need || v != math.MaxUint64 {
		t.Fatalf("NeedSyncing = %d,%v want MaxUint64,true", v, need)
	}
	wc.MarkSyncing(ctx, 2) // full sync 后 delta 不能覆盖
	if v, _ := wc.NeedSyncing(ctx); v != math.MaxUint64 {
		t.Fatalf("full sync 不应被 delta 覆盖: got %d", v)
	}

	wc.ClearSyncingAndMarkSynced(ctx, 7)
	if _, need := wc.NeedSyncing(ctx); need {
		t.Fatal("ClearSyncingAndMarkSynced 后不应有 syncing 标记")
	}
	if v := wc.LatestSynced(ctx); v != 7 {
		t.Fatalf("LatestSynced = %d, want 7", v)
	}
}

func TestWatchCollectionClear(t *testing.T) {
	wc := newTestWatchCollection(t)
	ctx := context.Background()

	wc.Save(ctx, []x.WatchInfo{{DB: "db", Collection: "c"}}, 1)
	wc.MarkSyncing(ctx, 1)

	wc.Clear(ctx)

	if wc.LatestVersion(ctx) != 0 {
		t.Fatalf("LatestVersion = %d, want 0", wc.LatestVersion(ctx))
	}
	if len(wc.Get(ctx, 1)) != 0 {
		t.Fatal("Clear 后 Get(1) 应为空")
	}
	if _, need := wc.NeedSyncing(ctx); need {
		t.Fatal("Clear 后不应有 syncing 标记")
	}
	if wc.LatestSynced(ctx) != 0 {
		t.Fatal("Clear 后 LatestSynced 应为 0")
	}

	// Clear 后仍可正常使用
	wc.Save(ctx, []x.WatchInfo{{DB: "db", Collection: "c2"}}, 2)
	if v := wc.LatestVersion(ctx); v != 2 {
		t.Fatalf("LatestVersion = %d, want 2", v)
	}
}
