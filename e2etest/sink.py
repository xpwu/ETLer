#!/usr/bin/env python3
"""下游处理服务器替身：把每次 POST 记到 JSONL，可用文件注入 500 故障。

由 e2etest.sh 启动，不直接手动运行。
环境变量：SINK_LOG / SINK_FAIL_FILE / SINK_PORT
"""
import json, os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG = os.environ.get("SINK_LOG", "sink.jsonl")
FAIL = os.environ.get("SINK_FAIL_FILE", "sink.fail")
lock = threading.Lock()
active = 0
seq = 0


class H(BaseHTTPRequestHandler):
    def do_POST(self):
        global active, seq
        n = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(n)
        t0 = time.time()
        with lock:
            active += 1
            cur, seq = active, seq + 1
            my = seq
        fail = os.path.exists(FAIL)
        try:
            body = json.loads(raw.decode())
        except Exception:
            body = {"__raw__": raw.decode(errors="replace")}

        self.send_response(500 if fail else 200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

        with lock:
            with open(LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "seq": my, "t0": t0, "t1": time.time(),
                    "concurrent": cur, "status": 500 if fail else 200,
                    "path": self.path, "body": body,
                }, ensure_ascii=False) + "\n")
            active -= 1
        print(f"#{my} status={500 if fail else 200} concurrent={cur} "
              f"T={body.get('T')} Ns={body.get('Ns')} "
              f"n={len(body.get('Data') or [])}", flush=True)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    port = int(os.environ.get("SINK_PORT", "9000"))
    open(LOG, "w").close()
    print(f"sink on 127.0.0.1:{port} -> {LOG}; touch {FAIL} to inject 500", flush=True)
    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
