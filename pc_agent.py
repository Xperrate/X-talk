"""PC Agent guardian process - provides a command execution channel for LLM agents resident in GroupChat. Run one on each represented computer (the machine where the GroupChat service is located is connected locally and does not need it; run it on the remote computer and open the firewall port): python pc_agent.py # default listening 0.0.0.0:8931 environment variables: PC_AGENT_PORT = port, PC_AGENT_BIND = bind address (default 0.0.0.0) interfaces: GET /status; POST /exec {"cmd":"...","timeout":seconds} -> {ok,exit_code,elapsed_ms,output}. Each command execution is appended to pc_agent.log in the same directory as this script."""
import json
import os
import platform
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PC_AGENT_PORT", "8931"))
BIND = os.environ.get("PC_AGENT_BIND", "0.0.0.0")
OUT_LIMIT = 6000
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pc_agent.log")


def log(line: str):
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(time.strftime("[%Y-%m-%d %H:%M:%S] ") + line + "\n")


class Handler(BaseHTTPRequestHandler):
    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/status":
            self._json({"ok": True, "hostname": platform.node(),
                        "system": f"{platform.system()} {platform.release()}"})
        else:
            self._json({"ok": False, "error": "not found"}, 404)

    def do_POST(self):
        if self.path != "/exec":
            return self._json({"ok": False, "error": "not found"}, 404)
        try:
            n = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(n).decode("utf-8") or "{}")
            cmd = str(data.get("cmd", ""))[:4000]
            timeout = max(1, min(int(data.get("timeout", 120)), 300))
        except Exception as e:
            return self._json({"ok": False, "error": f"bad request: {e}"}, 400)
        if not cmd.strip():
            return self._json({"ok": False, "error": "cmd is empty"}, 400)

        argv = (["powershell", "-NoProfile", "-Command", cmd] if os.name == "nt" else ["/bin/sh", "-c", cmd])
        t0 = time.time()
        try:
            p = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            rc, out_b = p.returncode, (p.stdout or b"")
        except Exception as e:
            log(f"FAIL cmd={cmd!r} err={e}")
            return self._json({"ok": False, "error": f"failed to execute command: {e}"}, 500)

        ms = int((time.time() - t0) * 1000)
        text = out_b.decode("utf-8", "replace")[:OUT_LIMIT]
        log(f"cmd={cmd!r} exit_code={rc} elapsed_ms={ms}")
        self._json({"ok": True, "exit_code": rc, "elapsed_ms": ms, "output": text})

    def log_message(self, *args):  # Block default access log
        pass


def main():
    print(f"pc_agent listening on {BIND}:{PORT} (host={platform.node()}, system={platform.system()})", flush=True)
    if BIND == "0.0.0.0":
        print("prompt: Open to LAN, Windows requires adminexecute firewall release:", flush=True)
        print(f'  New-NetFirewallRule -DisplayName "PC-Agent" -Direction Inbound -Protocol TCP -LocalPort {PORT} -Action Allow', flush=True)
    log(f"pc_agent started bind={BIND} port={PORT}")
    ThreadingHTTPServer((BIND, PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
