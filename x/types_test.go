package x

import (
	"encoding/json"
	"testing"
)

// WatchInfo.Id() 是 SyncTask 的唯一标识、WatchCollection 的 map key、
// 以及 change stream 过滤用的 key，拼接规则本身是协议的一部分。
func TestWatchInfoId(t *testing.T) {
	cases := []struct {
		name string
		wi   WatchInfo
		want string
	}{
		{"常规", WatchInfo{DB: "db1", Collection: "c1"}, "db1.c1"},
		{"零值", WatchInfo{}, "."},
		{"DB 为空", WatchInfo{Collection: "c1"}, ".c1"},
		{"Collection 为空", WatchInfo{DB: "db1"}, "db1."},
		{"大小写敏感", WatchInfo{DB: "DB1", Collection: "C1"}, "DB1.C1"},
		{"含连字符下划线", WatchInfo{DB: "my-db", Collection: "my_coll"}, "my-db.my_coll"},
		{"多个点号", WatchInfo{DB: "a.b", Collection: "c.d"}, "a.b.c.d"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.wi.Id(); got != c.want {
				t.Fatalf("Id() = %q, want %q", got, c.want)
			}
		})
	}
}

// Id() 是简单的字符串拼接，不做转义，所以 DB/Collection 内含点号时会产生碰撞。
// 例如 {"a.b","c"} 与 {"a","b.c"} 都得到 "a.b.c"。
// 这不是 bug，而是当前约定的已知局限：Mongo 的 db/collection 命名规范里点号有特殊含义，
// 调用方依赖这个前提。此处把行为固化，若将来改成转义或结构化 key，本用例会失败并提醒。
func TestWatchInfoIdCollision(t *testing.T) {
	a := WatchInfo{DB: "a.b", Collection: "c"}
	b := WatchInfo{DB: "a", Collection: "b.c"}

	if a.Id() != b.Id() {
		t.Fatalf("预期两者 Id 相同（当前不做转义）: %q vs %q", a.Id(), b.Id())
	}
	if a.Id() != "a.b.c" {
		t.Fatalf("Id() = %q, want %q", a.Id(), "a.b.c")
	}
}

// WatchInfo 是值类型：拷贝后修改互不影响。
// 各包的实现里大量按值传递 / 存入 slice，依赖这个语义。
func TestWatchInfoIsValueType(t *testing.T) {
	orig := WatchInfo{DB: "db1", Collection: "c1"}
	copied := orig
	copied.DB = "db2"
	copied.Collection = "c2"

	if orig.DB != "db1" || orig.Collection != "c1" {
		t.Fatalf("原值被修改: %+v", orig)
	}
	if copied.DB != "db2" || copied.Collection != "c2" {
		t.Fatalf("拷贝值 = %+v", copied)
	}
	if orig.Id() != "db1.c1" || copied.Id() != "db2.c2" {
		t.Errorf("Id() 应基于各自字段: %q / %q", orig.Id(), copied.Id())
	}
}

// x.WatchInfo 没有 json tag，序列化后字段名是 "DB" / "Collection"。
// 这是 HTTP API 的请求/响应协议，若将来加 tag，用例会失败并提醒同步核对。
func TestWatchInfoJSON(t *testing.T) {
	bs, err := json.Marshal(WatchInfo{DB: "db1", Collection: "c1"})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if want := `{"DB":"db1","Collection":"c1"}`; string(bs) != want {
		t.Fatalf("JSON = %s, want %s", bs, want)
	}

	var got WatchInfo
	if err := json.Unmarshal([]byte(`{"DB":"db2","Collection":"c2"}`), &got); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if want := (WatchInfo{DB: "db2", Collection: "c2"}); got != want {
		t.Fatalf("Unmarshal = %+v, want %+v", got, want)
	}
}
