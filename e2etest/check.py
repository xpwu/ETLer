#!/usr/bin/env python3
"""端到端判定与等待辅助（零第三方依赖）。

用法：
  python3 check.py <sink.jsonl>                     主闭环完整判定（自动排除就绪探针）
  python3 check.py --check-ports 27017 8080 9000   检查端口未被占用
  python3 check.py --wait-sync <sink.jsonl>        等到出现一次成功的 Sync 请求
  python3 check.py --wait-events N <sink.jsonl>    等到 ChangeStream 事件总数 >= N
  python3 check.py --wait-event-id ID <sink.jsonl> 等到 insert 事件 documentKey._id == ID
  python3 check.py --count-probes <sink.jsonl>     打印已到达的就绪探针数量
  python3 check.py --assert-sync-once <sink.jsonl> 断言成功的 Sync 恰好 1 次
  python3 check.py --check-fail-seen <sink.jsonl>  断言注入故障期间出现过 500

子命令都遵循「满足返回 0，不满足返回 1」，供 e2etest.sh 轮询。
"""
import base64, json, struct, sys

PROBE_PREFIX = "__e2e_probe_"


# ---------------------------------------------------------------- BSON 解析
# Data 元素是 base64 编码的 BSON 文档（bson.Raw 是 []byte，json.Marshal 走 byte-slice 分支）。
# 只解析需要的字段，其余类型一律跳过，避免某个用不到的 BSON 类型让判定脚本崩掉。

def _cstr(raw, pos):
    end = raw.index(b"\x00", pos)
    return raw[pos:end].decode(), end + 1


def _skip(raw, pos, t):
    if t == 0x01: return pos + 8
    if t in (0x02, 0x0D, 0x0E):
        (n,) = struct.unpack_from("<i", raw, pos);
        return pos + 4 + n
    if t in (0x03, 0x04, 0x0F):
        (n,) = struct.unpack_from("<i", raw, pos);
        return pos + n
    if t == 0x05:
        (n,) = struct.unpack_from("<i", raw, pos);
        return pos + 4 + 1 + n
    if t == 0x06: return pos
    if t == 0x07: return pos + 12
    if t == 0x08: return pos + 1
    if t in (0x09, 0x11, 0x12): return pos + 8
    if t == 0x0A: return pos
    if t == 0x0B:
        _, p = _cstr(raw, pos);
        _, p = _cstr(raw, p);
        return p
    if t == 0x0C:
        (n,) = struct.unpack_from("<i", raw, pos);
        return pos + 4 + n + 12
    if t == 0x10: return pos + 4
    if t == 0x13: return pos + 16
    if t in (0x7F, 0xFF): return pos
    raise ValueError(f"unknown bson type 0x{t:02x}")


def _read(raw, pos, t):
    if t == 0x01: return struct.unpack_from("<d", raw, pos)[0], pos + 8
    if t == 0x02:
        (n,) = struct.unpack_from("<i", raw, pos)
        return raw[pos + 4:pos + 4 + n - 1].decode(), pos + 4 + n
    if t in (0x03, 0x04):
        (n,) = struct.unpack_from("<i", raw, pos)
        return _parse(raw[pos:pos + n]), pos + n
    if t == 0x10: return struct.unpack_from("<i", raw, pos)[0], pos + 4
    if t in (0x09, 0x12): return struct.unpack_from("<q", raw, pos)[0], pos + 8
    if t == 0x08: return bool(raw[pos]), pos + 1
    if t == 0x0A: return None, pos
    if t == 0x05:
        (n,) = struct.unpack_from("<i", raw, pos)
        return raw[pos + 5:pos + 5 + n].hex(), pos + 5 + n
    if t == 0x07: return raw[pos:pos + 12].hex(), pos + 12
    if t == 0x11: return struct.unpack_from("<q", raw, pos)[0], pos + 8
    return None, _skip(raw, pos, t)


def _parse(raw, want=None):
    (size,) = struct.unpack_from("<i", raw, 0)
    if size != len(raw):
        raise ValueError(f"bad bson size {size} != {len(raw)}")
    pos, out = 4, {}
    while pos < size - 1:
        t = raw[pos];
        pos += 1
        key, pos = _cstr(raw, pos)
        if want is not None and key not in want:
            pos = _skip(raw, pos, t);
            continue
        val, pos = _read(raw, pos, t)
        out[key] = val
    return out


WANT = {"_id", "operationType", "documentKey"}


def decode(item):
    return _parse(base64.b64decode(item), want=WANT)


# ---------------------------------------------------------------- 读取日志
# sink.py 可能正在写半行，解析失败的行直接跳过。

def load(path):
    rows = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    return rows


def successful(rows):
    return [r for r in rows if r.get("status") == 200 and r.get("path") == "/etl"]


def events_of(rows):
    out = []
    for r in successful(rows):
        body = r.get("body") or {}
        if body.get("T") != 1:
            continue
        for x in (body.get("Data") or []):
            try:
                out.append(decode(x))
            except Exception:
                continue
    return out


def is_probe(e):
    doc_id = (e.get("documentKey") or {}).get("_id") or e.get("_id")
    return isinstance(doc_id, str) and doc_id.startswith(PROBE_PREFIX)


def business_events(rows):
    return [e for e in events_of(rows) if not is_probe(e)]


# ---------------------------------------------------------------- 子命令

def cmd_check_ports(args):
    import socket
    if not args:
        print("用法: --check-ports <port> [port ...]")
        return 1
    for a in args:
        try:
            p = int(a)
        except ValueError:
            print(f"非法端口: {a}")
            return 1
        s = socket.socket()
        try:
            s.bind(("127.0.0.1", p))
        except OSError:
            print(f"端口 {p} 被占用")
            return 1
        finally:
            s.close()
    return 0


def cmd_wait_sync(path):
    return 0 if any((r.get("body") or {}).get("T") == 0 for r in successful(load(path))) else 1


def cmd_wait_events(n, path):
    total = sum(len((r.get("body") or {}).get("Data") or [])
                for r in successful(load(path))
                if (r.get("body") or {}).get("T") == 1)
    return 0 if total >= n else 1


def cmd_wait_event_id(eid, path):
    for e in events_of(load(path)):
        if e.get("operationType") == "insert" and (e.get("documentKey") or {}).get("_id") == eid:
            return 0
    return 1


def cmd_count_probes(path):
    print(sum(1 for e in events_of(load(path)) if is_probe(e)))
    return 0


def cmd_assert_sync_once(path):
    n = sum(1 for r in successful(load(path)) if (r.get("body") or {}).get("T") == 0)
    if n != 1:
        print(f"Sync 次数 = {n}, 期望 1（重启不应重新全量同步）")
        return 1
    return 0


def cmd_check_fail_seen(path):
    return 0 if any(r.get("status") == 500 for r in load(path)) else 1


# ---------------------------------------------------------------- 主判定

def fail(msg):
    print("FAIL:", msg)
    sys.exit(1)


def verify(path):
    rows = load(path)
    ok = successful(rows)
    if not ok:
        fail("没有任何成功的 /etl 请求")

    if any(r.get("concurrent") != 1 for r in ok):
        fail("出现并发请求，违反『同一时间只会有一个接口请求』")
    if any(not all(k in (r.get("body") or {}) for k in ("T", "Ns", "Data")) for r in ok):
        fail("Request 字段不是 {T, Ns, Data}")

    sync = [r for r in ok if r["body"]["T"] == 0]
    cs = [r for r in ok if r["body"]["T"] == 1]
    other = [r for r in ok if r["body"]["T"] not in (0, 1)]
    if other:
        fail(f"出现未知 T: {[r['body']['T'] for r in other]}")

    # 全量同步：启动前的 3 条种子，按 _id 升序，Ns 必须是监听集合
    if len(sync) != 1:
        fail(f"期望 1 次 Sync 全量同步，实际 {len(sync)} 次")
    b = sync[0]["body"]
    if b["Ns"] != {"DB": "demo", "Coll": "items"}:
        fail(f"Sync Ns = {b['Ns']}")
    try:
        ids = [decode(x)["_id"] for x in b["Data"]]
    except Exception as e:
        fail(f"Sync Data 解析失败: {e}")
    if ids != ["a", "b", "c"]:
        fail(f"Sync Data 顺序/内容 = {ids}, want ['a','b','c']")

    # 增量：insert(d)/update(b)/delete(c)；Ns 为空；other 集合应被过滤。
    # 就绪探针是脚本为保证 watcher 已建流而写入的辅助数据，主判定里排除。
    if not cs:
        fail("没有收到任何 ChangeStream 请求")
    events = []
    probes = 0
    for r in cs:
        if r["body"]["Ns"] != {"DB": "", "Coll": ""}:
            fail(f"ChangeStream Ns 应为空: {r['body']['Ns']}")
        for x in r["body"]["Data"]:
            try:
                e = decode(x)
            except Exception as ex:
                fail(f"ChangeStream Data 解析失败: {ex}")
            if is_probe(e):
                probes += 1
            else:
                events.append(e)

    got = sorted((e.get("operationType"), (e.get("documentKey") or {}).get("_id"))
                 for e in events)
    want = sorted([("insert", "d"), ("update", "b"), ("delete", "c")])
    if got != want:
        fail(f"事件 = {got}, want {want}（非监听集合的写入是否被误发？）")

    replay = [(e.get("operationType"), (e.get("documentKey") or {}).get("_id"))
              for e in events
              if (e.get("documentKey") or {}).get("_id") in ("a", "b", "c")
              and e.get("operationType") == "insert"]
    if replay:
        fail(f"启动前的种子事件被重放了: {replay}")

    if min(r["seq"] for r in cs) < max(r["seq"] for r in sync):
        fail("ChangeStream 出现在 Sync 之前（initWatching 握手顺序错误）")

    print(f"PASS  requests={len(ok)}  sync={len(sync)}(3 docs)  "
          f"change-stream={len(cs)} batches/{len(events)} events(探针 {probes} 条已排除)  "
          f"concurrent=1  顺序正确  种子未重放")
    print("  请求序列 (seq, T, n):",
          [(r["seq"], r["body"]["T"], len(r["body"]["Data"] or [])) for r in ok])
    return 0


# ---------------------------------------------------------------- CLI

def main():
    argv = sys.argv[1:]
    if not argv:
        print(__doc__)
        return 2
    cmd = argv[0]
    if cmd == "--check-ports":
        return cmd_check_ports(argv[1:])
    if cmd == "--wait-sync":
        return cmd_wait_sync(argv[1])
    if cmd == "--wait-events":
        return cmd_wait_events(int(argv[1]), argv[2])
    if cmd == "--wait-event-id":
        return cmd_wait_event_id(argv[1], argv[2])
    if cmd == "--count-probes":
        return cmd_count_probes(argv[1])
    if cmd == "--assert-sync-once":
        return cmd_assert_sync_once(argv[1])
    if cmd == "--check-fail-seen":
        return cmd_check_fail_seen(argv[1])
    if cmd.startswith("--"):
        print(f"未知子命令: {cmd}")
        print(__doc__)
        return 2
    return verify(cmd)


if __name__ == "__main__":
    sys.exit(main())
