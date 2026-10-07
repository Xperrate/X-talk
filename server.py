"""LLM GroupChatservice entrance. run: python server.py (default http://127.0.0.1:8321) v2 Architecture Description: - Support multiple sessions (context switching): each session is an independent Conversation instance, history stores data/<session_id>.jsonl; - config.json is the only persistence source for members/global arguments, and changes on run sync back to all livenesssessions."""
import asyncio
import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Optional

import httpx
from fastapi import Body, FastAPI, File, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse

import llms  # noqa: F401 import i.e. registration All built-in adapters
from core.conversation import (Conversation, DEFAULT_TURN_ETIQUETTE, safe_upload_name, PRIVATE_PREFIX,   # Private_prefix = Feature # 10g private chat prefix
                              presence_clear, presence_rename, presence_status,   # Feature#10:membersonline/offlinestatus
                              FOCUS as _FOCUS_REG,   # Feature # 10d: Focus registration form (same object as core.conversation.FOCUS)
                              focus_is_on, focus_manual_set, focus_task_link, focus_task_unlink,
                               focus_rebuild, focus_clear_member, focus_rename)    # Feature # 10d/10f: Focus schema (taskauto on/off + manual free switching) + role division of this round
from core.conversation import presence_mark   # Feature # host: After switching, put the onlinestatus in the ash first, and the next callsuccess will be auto green again
from core.executor import make_executor       # Feature # host: Rebuild the executor when replacing the host

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.environ.get("GROUPCHAT_CONFIG", str(ROOT / "config.json")))
DATA_DIR = Path(os.environ.get("GROUPCHAT_DATA", str(ROOT / "data")))
UPLOADS_DIR = DATA_DIR / "uploads"
SESSIONS_META = DATA_DIR / "sessions.json"
PORT = int(os.environ.get("PORT", 8321))


def _ensure_etiquette_file(path: Path):
    """when the [GroupChat etiquette] prompt file injected at run is missing, it is seeded with the built-in default; if it already exists (changed by user), it is never overwritten."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text(DEFAULT_TURN_ETIQUETTE + "\n", encoding="utf-8")
    except OSError:  # noqa: BLE001 - Write disk failed does not affect start, rewind built-in default text on run
        pass


ETIQUETTE_PATH = Path(os.environ.get("GROUPCHAT_ETYQUETTE", str(ROOT / "prompts" / "turn_etiquette.txt")))
_ensure_etiquette_file(ETIQUETTE_PATH)


def _lan_ip() -> str:
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:  # noqa: BLE001 - Return to native loop when no network
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip


# Feature # 6 (file channel): The public/local network base address of the group shared file, which can be used by the remote agent via the command channel pullattachment; and can be overwritten by groupchat_public_host.
PUBLIC_BASE = os.environ.get("GROUPCHAT_PUBLIC_HOST", f"http://{_lan_ip()}:{PORT}")

FILE_LOG = DATA_DIR / "file_log.jsonl"   # Feature # 6: attachment registration log [{ts,session,sender,orig,stored,size}], new line at the end
TASKS_PATH = DATA_DIR / "tasks.json"     # Feature # 7 (task): {sid: [task,...]} in progress tasklist
TASK_LOG = DATA_DIR / "task_log.jsonl"   # Feature # 8 (Daily Record): [{ts, day, action: create | complete, sid, task: {... }}] incrementalevent

TEXT_EXTS = {".txt", ".md", ".csv", ".json", ".py", ".js", ".ts", ".html", ".htm",
             ".xml", ".yml", ".yaml", ".log", ".ini", ".cfg", ".toml", ".sql"}
INLINE_MAX_BYTES = 200 * 1024            # Pure textattachment smaller than this body product will be inlined into the LLM context (remote agent can be read without network)


def _tail_jsonl(path: Path, limit: int = 500) -> list[dict]:
    """Read the end of the jsonl log by line, return→ old and new sort; bad lines are skipped."""
    if not path.exists():
        return []
    out = []
    for line in reversed(path.read_text(encoding="utf-8").splitlines()[-limit * 3 - 100:]):
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:  # noqa: BLE001
            pass
        if len(out) >= limit:
            break
    return out


def _append_jsonl(path: Path, entry: dict):
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:  # noqa: BLE001 - logfailed does not affect the primary workflow (upload/task itself has succeeded)
        pass


def _inline_text_file(p: Path):
    """Feature # 6: The small body product pure textattachment is read as a content string for inline; the condition return None is not met."""
    try:
        if p.suffix.lower() not in TEXT_EXTS or p.stat().st_size > INLINE_MAX_BYTES:
            return None
        body = p.read_text(encoding="utf-8", errors="replace")[:6000]
        note = "\n (content too long truncated, see path/address above for full file)" if len(body) >= 6000 else ""
        return body + note
    except Exception:  # noqa: BLE001 - Only give the path if you can't read it, do not block the message
        return None


def _load_tasks_store() -> dict[str, list]:
    if TASKS_PATH.exists():
        try:
            d = json.loads(TASKS_PATH.read_text(encoding="utf-8"))
            return {k: [t for t in v if isinstance(t, dict) and t.get("id")]
                    for k, v in d.items() if isinstance(v, list)}
        except Exception:  # noqa: BLE001 - tasks.json Rebuild on Damage (Daily record unaffected in task_log)
            pass
    return {}


def _eod(day_s: str) -> float:
    """The local time stamp at 24:00 on a certain day."""
    return time.mktime(time.strptime(day_s, "%Y-%m-%d")) + 86400


def _days_summary(events: list[dict]) -> list[dict]:
    """Feature # 8: eventstream grouped by day (→old and new). done = done on the day; undone = not done as of the end of Japan."""
    today = time.strftime("%Y-%m-%d")
    day_set = {str(e.get("day") or "") for e in events if e.get("day")} | {today}
    created: dict[str, tuple] = {}     # id - > (create ts, tasksnapshot)
    completed_at: dict[str, float] = {}  # id - > donets (take the earliest)
    done_by_day: dict[str, list] = {}
    for e in sorted(events, key=lambda x: float(x.get("ts", 0))):
        tsk = str((e.get("task") or {}).get("id") or "")
        if not tsk:
            continue
        ts = float(e.get("ts", 0.0))
        day = str(e.get("day") or time.strftime("%Y-%m-%d", time.localtime(ts)))
        task = e.get("task") or {}
        act = e.get("action")
        if act == "create" and tsk not in created:
            created[tsk] = (ts, {"content": str(task.get("content") or ""), "assignee": str(task.get("assignee") or "")})
        elif act == "complete":
            completed_at.setdefault(tsk, ts)
            done_by_day.setdefault(day, []).append({**task, "completed_ts": ts})
    out = []
    for day in sorted((d for d in day_set if d), reverse=True)[:30]:  # Last 30 days,→ old and new
        cut = _eod(day)
        undone = [{"content": info["content"], "assignee": info["assignee"]}
                  for tsk, (cts, info) in created.items()
                  if cts <= cut and completed_at.get(tsk, 1e30) > cut]
        out.append({"day": day, "done": done_by_day.get(day, []), "undone": undone})
    return out
# v1 Single sessionhistory, automigration at first start is the first session (environmentvariable is available to point elsewhere/testisolation)
LEGACY_HISTORY = Path(os.environ.get("GROUPCHAT_LEGACY", str(ROOT / "history.jsonl")))

DATA_DIR.mkdir(parents=True, exist_ok=True)
UPLOADS_DIR.mkdir(exist_ok=True)

cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


# ---------- config/sessions persistence (atomic writing) ----------

def _atomic_write(path: Path, text: str):
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def save_cfg():
    try:
        _atomic_write(CONFIG_PATH, json.dumps(cfg, ensure_ascii=False, indent=2))
    except Exception as e:  # noqa: BLE001 - return prompt to caller
        raise RuntimeError(f"failed to write config: {e}") from e


def try_save():
    """soft package of save_cfg: successreturn None, failedreturnerrorstring (endpoint goes to JSON response)."""
    try:
        save_cfg()
        return None
    except RuntimeError as e:
        return str(e)


# -------- Feature # 10b (avatar): Built-in imageresource + replant from seed directory ----------
HEADPIC_DIR = ROOT / "static" / "headpic"   # Statically mounted to root relative URL (/headpic/) access → project whole bodymigration/switching does not fail
DEFAULT_AVATAR_SEED_DIR = ""


def _ensure_headpics():
    """Copy missing .png files from config.json `avatar_seed_dir` (default desktop headpic) into static/headpic/; never overwrite existing files, and repair legacy avatar values in config. Default avatars (Me.png for Me, Ai.png for agents) and each member's custom image follow the project tree: moving directories or restoring backups does not affect display; if static/headpic is deleted, the next start automatically recovers it from the seed directory. Repair targets written back to config.json: native absolute avatar paths inside headpic/ are rewritten to bare filenames; leftover dirty values such as `"C:\\...\\Ar.png"` wrapped in JSON quotes are first unwrapped and then repaired by the same rules (including legal URLs that only need quote stripping)."""
    try:
        HEADPIC_DIR.mkdir(parents=True, exist_ok=True)
        copied = []
        seed_value = str(cfg.get("avatar_seed_dir") or DEFAULT_AVATAR_SEED_DIR).strip()
        seed_dir = Path(seed_value) if seed_value else None
        if seed_dir is not None and seed_dir.is_dir():   # Skip replanting when seed directory does not exist/is null (does not affect legacy repair and start below)
            for src in sorted(seed_dir.glob("*.png")):
                dst = HEADPIC_DIR / src.name
                try:
                    if not dst.exists():
                        import shutil as _shutil
                        _shutil.copyfile(src, dst)
                        copied.append(src.name)
                except OSError:  # noqa: BLE001 - single failed does not affect the rest vs. start
                    pass
        fixed = []   # Legacy Fix: "Native absolute path/quoted dirty value" in legacyconfig cannot be → rewritten to naked file name (or legal URL) by HTTP reference and written back
        for e in cfg.get("members", []) or []:
            raw = str(e.get("avatar") or "").strip()
            if not raw or raw.lower().startswith(("http://", "https://", "data:image/")):
                continue
            inner = raw[1:-1] if len(raw) >= 2 and raw.startswith('"') and raw.endswith('"') else raw   # The dirty value of the whole body wrapped in JSON quotation mark package is peeled off → before judging
            low_in = inner.lower()
            if raw != inner and low_in.startswith(("http://", "https://", "data:image/")):   # Legal image URL with quotation marks → Directly unquoted Repair
                e["avatar"] = inner
                fixed.append(f"{e.get('name')}: {raw!r} -> unquoted url")
            elif re.search(r"[/:\\\\]", inner) and not low_in.startswith(("http://", "https://")):   # Native path (may be quoted) The→ basename must already be repaired within headpic/, otherwise it will not move
                base = Path(inner).name
                if (HEADPIC_DIR / base).exists():
                    e["avatar"] = base
                    fixed.append(f"{e.get('name')}: {raw} -> {base}")
        return copied, fixed
    except Exception:  # noqa: BLE001 - avatar replanting/repair never blocks servicestart
        return [], []


_copied, _fixed = _ensure_headpics()
if _copied or _fixed:
    if _copied:
        print(f"[headpic] seeded from seed dir -> static/headpic/: {', '.join(_copied)}")
    for f in _fixed:
        print(f"[headpic] repaired legacy avatar path: {f}")
if _fixed:   # config is written back by the repaired → atom to avoid repeated changes every time it is restarted
    try_save()


def load_sessions_meta() -> list[dict]:
    if SESSIONS_META.exists():
        try:
            data = json.loads(SESSIONS_META.read_text(encoding="utf-8"))
            return [s for s in data if isinstance(s, dict) and s.get("id")]
        except Exception:  # noqa: BLE001 - rebuild when meta is damaged
            pass
    return []


def save_sessions_meta(meta: list[dict]):
    _atomic_write(SESSIONS_META, json.dumps(meta, ensure_ascii=False, indent=2))


# ---------- v1 → v2 migration: old history.json l becomes first session ----------

sessions_meta = load_sessions_meta()
if not sessions_meta and LEGACY_HISTORY.exists():
    sid = uuid.uuid4().hex[:8]
    if LEGACY_HISTORY.stat().st_size > 0:
        _atomic_write(DATA_DIR / f"{sid}.jsonl", LEGACY_HISTORY.read_text(encoding="utf-8"))
        LEGACY_HISTORY.write_text("", encoding="utf-8")
    else:
        (DATA_DIR / f"{sid}.jsonl").touch()
    sessions_meta = [{"id": sid, "name": "session 1", "created": time.time()}]
    save_sessions_meta(sessions_meta)

if not sessions_meta:
    sid = uuid.uuid4().hex[:8]
    (DATA_DIR / f"{sid}.jsonl").touch()
    sessions_meta = [{"id": sid, "name": "session 1", "created": time.time()}]
    save_sessions_meta(sessions_meta)

# ---------- One Conversation instance per session (non-interfering turn drive) ----------

convs: dict[str, Conversation] = {}
TASKS_STORE = _load_tasks_store()   # Feature # 7: {sid:[task,...]} memory state; changes are written back to tasks.json


def _save_tasks():
    try:
        _atomic_write(TASKS_PATH, json.dumps(TASKS_STORE, ensure_ascii=False, indent=2))
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"failed to write task list: {e}") from e


async def presence_fanout(event: dict):   # Feature # 10: Global broadcast of presence migration (parse fanout_all → compatible module loading order within functionbody)
    await fanout_all(event)


def _create_task(sid: str, content_core: str, assignee: str, assigned_by: str = "") -> tuple[dict, Optional[tuple[str, dict]]]:
    """Feature # role/# 7/# 10d: Only build task ledger and focus link, do not send message. return (task, focus_event)."""
    task = {"id": uuid.uuid4().hex[:8], "content": content_core, "assignee": assignee,
            "assigned_by": assigned_by, "created": time.time()}
    TASKS_STORE.setdefault(sid, []).append(task)
    _save_tasks()
    _append_jsonl(TASK_LOG, {"ts": time.time(), "day": time.strftime("%Y-%m-%d"),
                             "action": "create", "sid": sid,
                             "task": {k: task[k] for k in ("id", "content", "assignee", "assigned_by")}})
    focus_ev = None
    if assignee:
        ch_f, fv = focus_task_link(assignee, sid, task["id"])
        if ch_f:
            focus_ev = (assignee, fv)
    return task, focus_ev


def make_conv(sid: str) -> Conversation:
    c = Conversation(cfg, history_path=DATA_DIR / f"{sid}.jsonl", session_id=sid,
                     file_log_path=FILE_LOG, uploads_dir=UPLOADS_DIR, public_base=PUBLIC_BASE,
                     etiquette_path=ETIQUETTE_PATH)   # [GroupChat etiquette] prompt injected at run: read the file live before each speak
    c.load_history()

    async def broadcast(event: dict):  # Only send to WS clients belonging to that session
        await send_to_session(sid, event)

    async def leader_task_dispatch(content_core: str, assignee: str, assigned_by: str) -> str:
        task, focus_ev = _create_task(sid, content_core, assignee, assigned_by)
        if focus_ev is not None:
            await _broadcast_focus(focus_ev[0], focus_ev[1])
        await send_to_session(sid, {"type": "task", "op": "create", "sid": sid, "tasks": TASKS_STORE.get(sid, [])})
        return task["id"]

    c.broadcast = broadcast
    c.global_broadcast = presence_fanout   # Feature # 10: online/offlinestatusmigration → notification All clients for all sessions
    c.task_dispatch_fn = leader_task_dispatch   # Feature # role: leaderreply dispatch ledger channel, no extra dispatch classmessage
    return c


for s in sessions_meta:
    convs[s["id"]] = make_conv(s["id"])

focus_rebuild(TASKS_STORE)   # Feature # 10d: Focus status of tasks.json ledger restore "task derived" when servicestart (manual switch is run mode, not persistent)

ACTIVE_FILE = DATA_DIR / "last_active.txt"
LAST_ACTIVE = {"sid": sessions_meta[-1]["id"]}  # defaulttarget when no explicit targetsession (takes precedence over the last persisted active session)


def set_active(sid):
    LAST_ACTIVE["sid"] = sid
    try:
        ACTIVE_FILE.write_text(sid, encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass


_saved_sid = ACTIVE_FILE.read_text(encoding="utf-8").strip() if ACTIVE_FILE.exists() else ""
if _saved_sid in convs:
    LAST_ACTIVE["sid"] = _saved_sid

app = FastAPI(title="X-Talk")
clients: dict[WebSocket, str] = {}  # ws -> session_id


async def send_to_session(sid: str, event: dict):
    dead = []
    for ws in list(clients):
        if clients.get(ws) != sid:
            continue
        try:
            await ws.send_json(event)
        except Exception:  # noqa: BLE001
            dead.append(ws)
    for ws in dead:
        clients.pop(ws, None)


async def fanout_all(event: dict):  # Global event (configclass change), sent to all clients of all sessions
    dead = []
    for ws in list(clients):
        try:
            await ws.send_json(event)
        except Exception:  # noqa: BLE001
            dead.append(ws)
    for ws in dead:
        clients.pop(ws, None)


def member_views() -> list[dict]:
    """All sessions share the same members definition (from cfg), just take the view of any liveness instance."""
    c = next(iter(convs.values()))
    return [_member_view(m) for m in c.members]


def _member_view(m) -> dict:
    llm = m.llm if m.is_llm else None
    st, info = presence_status(m.name) if m.is_llm else ("online", None)   # Feature # 10: LLM members with online/offlinestatus (default online)
    v = {
        "name": m.name,
        "kind": "llm" if m.is_llm else "human",
        "enabled": m.enabled,
        "system_prompt": llm.system_prompt if llm else None,
        "model": getattr(llm, "model", "") or "",
        "executor": m.executor.describe() if (m.executor is not None and hasattr(m.executor, "describe")) else None,
        "avatar": getattr(m, "avatar", "") or "",   # Feature # 10b: customavatar (null = default tile)
        "role": getattr(m, "role", "boss" if not m.is_llm else "member"),   # Feature#role:boss/leader/member
    }
    if m.is_llm:
        v["presence_status"] = st
        v["presence_info"] = info   # {"reason","ts"} when offline, None when online
        e = _FOCUS_REG.get(m.name) or {"on": False, "tasks": []}   # Feature # 10d/10f: Focus schemastatus (last event first single valid state + number of undonetasks)
        v["focus"] = {"on": focus_is_on(m.name), "task_count": len(set(e["tasks"]))}
        _ent = next((x for x in cfg["members"] if str(x.get("name") or "") == m.name and x.get("type") != "human"), None)   # Feature # host: The current host (taken from base_url and written back after the switch)
        _hm = re.match(r"https?://([^/]+)", str((_ent or {}).get("base_url") or ""))
        v["host"] = _hm.group(1).lower() if _hm else ""   # Feature # host: Includes port, for front-end pre-filled full address
    return v


async def _broadcast_focus(name: str, view: dict):   # Feature # 10d: Focus on statusmigration → global event (Focus is not related to session)
    await fanout_all({"type": "focus", **view})


# ---------- Feature #10+: active presence probing ----------
PRESENCE_PROBE_INTERVAL = float(os.environ.get("PRESENCE_PROBE_INTERVAL", "20"))
PRESENCE_PROBE_TIMEOUT = float(os.environ.get("PRESENCE_PROBE_TIMEOUT", "5"))
PRESENCE_FAIL_THRESHOLD = int(os.environ.get("PRESENCE_FAIL_THRESHOLD", "2"))
_probe_state: dict[str, dict] = {}
_probe_task: Optional[asyncio.Task] = None


def _probe_targets(member) -> list[tuple[str, dict]]:
    llm = member.llm
    targets: list[tuple[str, dict]] = []
    base_url = str(getattr(llm, "base_url", "") or "").rstrip("/")
    if not base_url:
        return targets
    headers = {}
    api_key = str(getattr(llm, "api_key", "") or "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if getattr(llm, "type", "") == "ollama":
        targets.append((f"{base_url}/api/tags", headers))
    else:
        targets.append((f"{base_url}/models", headers))
    return targets


async def _probe_member(member) -> tuple[bool, str]:
    targets = _probe_targets(member)
    if not targets:
        return True, ""
    errors = []
    for url, headers in targets:
        try:
            async with httpx.AsyncClient(timeout=PRESENCE_PROBE_TIMEOUT, trust_env=False) as client:
                r = await client.get(url, headers=headers)
            if r.status_code >= 500:
                errors.append(f"{url} HTTP {r.status_code}")
        except Exception as e:  # noqa: BLE001
            errors.append(f"{url}: {type(e).__name__}: {e}")
    if errors:
        return False, "; ".join(errors)[:200]
    return True, ""


async def _presence_probe_once() -> None:
    c = convs.get(LAST_ACTIVE["sid"]) or next(iter(convs.values()), None)
    if c is None:
        return
    names = set()
    for m in c.members:
        if not m.is_llm or not m.enabled:
            continue
        names.add(m.name)
        st = _probe_state.setdefault(m.name, {"fails": 0, "oks": 0, "last_ok": None, "last_fail": None})
        ok, reason = await _probe_member(m)
        now = time.time()
        if ok:
            st["fails"] = 0
            st["oks"] += 1
            st["last_ok"] = now
            if presence_mark(m.name, "online", ""):
                await presence_fanout({"type": "presence", "name": m.name, "status": "online"})
        else:
            st["oks"] = 0
            st["fails"] += 1
            st["last_fail"] = now
            if st["fails"] >= PRESENCE_FAIL_THRESHOLD and presence_status(m.name)[0] != "offline":
                presence_mark(m.name, "offline", reason)
                await presence_fanout({"type": "presence", "name": m.name, "status": "offline", "reason": reason})
    for name in list(_probe_state):
        if name not in names:
            _probe_state.pop(name, None)


async def _presence_probe_loop() -> None:
    while True:
        try:
            await _presence_probe_once()
        except Exception as e:  # noqa: BLE001
            print(f"[warn] presence probe loop failed: {e}")
        await asyncio.sleep(PRESENCE_PROBE_INTERVAL)


@app.on_event("startup")
async def _start_presence_probe():
    global _probe_task
    if _probe_task is None:
        _probe_task = asyncio.create_task(_presence_probe_loop())


@app.on_event("shutdown")
async def _stop_presence_probe():
    global _probe_task
    if _probe_task is not None:
        _probe_task.cancel()
        try:
            await _probe_task
        except asyncio.CancelledError:
            pass
        _probe_task = None


@app.post("/api/presence/probe")
async def force_presence_probe():
    await _presence_probe_once()
    return {"ok": True, "members": member_views()}


def _sessions_view() -> list[dict]:
    return [{"id": s["id"], "name": s.get("name", s["id"]), "created": s.get("created")} for s in sessions_meta]


async def broadcast_sessions():
    await fanout_all({"type": "sessions", "sessions": _sessions_view(), "active": LAST_ACTIVE["sid"]})


def init_payload(sid: str) -> dict:
    c = convs[sid]
    return {
        "type": "init",
        "group_name": c.group_name,
        "max_llm_streak": c.max_llm_streak,
        "max_tool_steps": c.max_tool_steps,
        "you": c.human_name(),
        "members": [_member_view(m) for m in c.members],
        "sessions": _sessions_view(),
        "session_id": sid,
        "history": [m.to_dict() for m in c.history],
    }


# ---------- rest: session management (Function 4) ------------

@app.get("/api/sessions")
async def list_sessions():
    return {"sessions": _sessions_view(), "current": LAST_ACTIVE["sid"]}


@app.post("/api/session")
async def session_action(payload: dict = Body(default={})):
    action = str(payload.get("action", ""))
    if action == "new":
        sid = uuid.uuid4().hex[:8]
        name = str(payload.get("name") or "").strip()[:32] or f"session {len(sessions_meta) + 1}"
        (DATA_DIR / f"{sid}.jsonl").touch()
        sessions_meta.append({"id": sid, "name": name, "created": time.time()})
        save_sessions_meta(sessions_meta)
        convs[sid] = make_conv(sid)
        set_active(sid)
        await broadcast_sessions()
        return {"ok": True, "id": sid}
    if action == "use":
        sid = str(payload.get("id", ""))
        if sid not in convs:
            return JSONResponse({"ok": False, "error": f"session {sid!r} does not exist"}, status_code=404)
        set_active(sid)
        await broadcast_sessions()
        return {"ok": True, "id": sid}
    if action == "rename":
        sid = str(payload.get("id", ""))
        name = str(payload.get("name") or "").strip()[:32]
        s = next((x for x in sessions_meta if x["id"] == sid), None)
        if s is None or not name:
            return JSONResponse({"ok": False, "error": "session does not exist or name is null"}, status_code=400)
        s["name"] = name
        save_sessions_meta(sessions_meta)
        await broadcast_sessions()
        return {"ok": True}
    if action == "delete":
        sid = str(payload.get("id", ""))
        if len(convs) <= 1:
            return JSONResponse({"ok": False, "error": "Keep at least one session"}, status_code=400)
        s = next((x for x in sessions_meta if x["id"] == sid), None)
        if s is None:
            return JSONResponse({"ok": False, "error": f"session {sid!r} does not exist"}, status_code=404)
        convs.pop(sid, None)
        p = DATA_DIR / f"{sid}.jsonl"
        if p.exists():
            p.unlink()
        sessions_meta.remove(s)
        save_sessions_meta(sessions_meta)
        # Feature # 10d: → sessiondelete its un-donetasked focus link is also voided (incidentally clear the account of the session in tasks.json and fix the legacy leak)
        focus_evts = []   # [(name, view), ...]
        for t2 in TASKS_STORE.get(sid, []) or []:
            a2 = str(t2.get("assignee") or "")
            if a2 and t2.get("id"):
                ch2, fv2 = focus_task_unlink(a2, sid, str(t2["id"]))
                if ch2:
                    focus_evts.append((a2, fv2))
        TASKS_STORE.pop(sid, None)
        try:
            _save_tasks()
        except Exception:   # noqa: BLE001 - Ledger cleanupfailed does not block sessiondelete itself
            pass
        if LAST_ACTIVE["sid"] == sid:
            set_active(sessions_meta[-1]["id"])
        await broadcast_sessions()
        for nm, fv in focus_evts:   # Focus on statusmigration → global event (same level as presence/configclass)
            await _broadcast_focus(nm, fv)
        return {"ok": True}
    return JSONResponse({"ok": False, "error": f"unknown action {action!r} (supports new/use/rename/delete)"}, status_code=400)


# ---------- rest: members/global argument (functions 1, 2; valid for all livenesssessions and written back to config.json) ----------

@app.get("/api/members")
async def members():
    return {"group_name": convs[LAST_ACTIVE["sid"]].group_name, "members": member_views()}


def _all_convs_members(name: str):
    out = []
    for c in convs.values():
        m = next((x for x in c.members if x.name == name), None)
        if m is not None:
            out.append(m)
    return out


@app.post("/api/members")
async def add_member(payload: dict):
    """Function 2: Add an LLM assistant directly in the UI. field is the same as the config.json members [] element (optional base_url/api_key_env/system_prompt/executor/host_hint/temperature/timeout)."""
    name = str(payload.get("name") or "").strip()[:64]
    mtype = str(payload.get("type") or "")
    model = str(payload.get("model") or "").strip()
    if not name:
        return JSONResponse({"ok": False, "error": "First name cannot be null"}, status_code=400)
    if any(c["name"] == name for c in cfg["members"]):
        return JSONResponse({"ok": False, "error": f"a member with the same name {name!r} already exists in the group"}, status_code=400)
    m_cfg = {"name": name, "type": mtype, "model": model}
    for k in ("base_url", "api_key_env", "system_prompt", "executor", "host_hint", "temperature", "timeout", "avatar"):   # avatar = Feature # 10b customavatar, null do not write (default color block)
        if payload.get(k) not in (None, ""):
            m_cfg[k] = payload[k]
    role = str(payload.get("role") or "member").strip().lower()
    m_cfg["role"] = "leader" if role == "leader" else "member"
    if m_cfg["role"] == "leader":   # Feature # role: Unique leader - When new members become leaders, all old LLMs are downgraded to members
        for e in cfg["members"]:
            if e.get("type") != "human":
                e["role"] = "member"
        for c in convs.values():
            for x in c.members:
                if x.is_llm:
                    x.role = "member"
    try:
        first = convs[LAST_ACTIVE["sid"]]
        member = first.add_llm_member(m_cfg)  # type/model validate thrown wrong here
    except ValueError as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    for c in list(convs.values()):
        if c is not first and all(x.name != name for x in c.members):
            c.add_llm_member(m_cfg)
    cfg["members"].append({k: v for k, v in m_cfg.items()})
    err = try_save()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)
    await fanout_all({"type": "members", "members": member_views(), "added": name})
    return {"ok": True, "name": name}


@app.post("/api/member/delete")
async def delete_member(payload: dict):
    """Feature 2 (companion): Remove an LLM assistant and write back to config.json."""
    name = str(payload.get("name") or "").strip()
    if any(c["type"] == "human" and c["name"] == name for c in cfg["members"]):
        return JSONResponse({"ok": False, "error": "People class seats cannot be deleted"}, status_code=400)
    removed = False
    for c in convs.values():
        if any(x.name == name and x.is_llm for x in c.members):
            try:
                c.remove_member(name)
                removed = True
            except ValueError as e:
                return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    if not removed:
        return JSONResponse({"ok": False, "error": f"LLM member {name!r} does not exist"}, status_code=404)
    cfg["members"] = [c for c in cfg["members"] if not (c.get("type") != "human" and c.get("name") == name)]
    presence_clear(name)   # Feature # 10: cleanup its online/offline records to avoid ghost status
    focus_clear_member(name)   # Feature # 10d: Abolish its focus schema (including tasklink) to avoid the residual status of retired members
    err = try_save()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)
    await fanout_all({"type": "members", "members": member_views(), "removed": name})
    return {"ok": True, "name": name}


@app.post("/api/member/update")
async def update_member(payload: dict):
    """Feature #10b: change a member's name and/or custom avatar (written back to config.json). LLM members can change both; human seats can only change avatar (renaming would break the "Me" identity, so it is rejected). Body: `{"name": current_name}` plus optional `"rename"` and `"avatar"`. Avatar value: image URL (`http(s)`/`data:image`) is referenced directly; bare filename (such as `Ar.png`, must exist in static/headpic/) is referenced as /headpic/ and remains valid after project migration; other non-empty text is displayed as text (compatible with old values); empty string clears back to built-in default avatar, omitting the field means unchanged. Renaming also updates the sender of that member in all session JSONL files and in-memory history, and migrates presence records."""
    old = str(payload.get("name") or "").strip()
    m_cfg0 = next((c for c in cfg["members"] if c.get("name") == old), None)
    if m_cfg0 is None:
        return JSONResponse({"ok": False, "error": f"member {old!r} does not exist"}, status_code=404)
    is_human = m_cfg0.get("type") == "human"

    new_name = str(payload.get("rename") or "").strip()[:64] if payload.get("rename") is not None else old
    renaming = bool(new_name and new_name != old)
    if is_human and renaming:
        return JSONResponse({"ok": False, "error": "The person class seat does not support renaming (it will disrupt the 'Me' of each client), only modifiedavatar is allowed"}, status_code=400)
    if renaming:
        if any(c["name"] == new_name for c in cfg["members"]):
            return JSONResponse({"ok": False, "error": f"a member with the same name {new_name!r} already exists in the group"}, status_code=400)

    avatar = str(payload.get("avatar"))[:128].strip() if payload.get("avatar") is not None else None   # null string = clear; None = unchanged
    if avatar and len(avatar) >= 2 and avatar.startswith('"') and avatar.endswith('"'):   # Defense: The dirty value of the whole body wrapped in JSON quotation mark package is → peeled off the outer layer and then validated/stored
        avatar = avatar[1:-1].strip()[:128]
    if avatar:   # "" is a valid action (clear), skipping validate
        low = avatar.lower()
        is_url = low.startswith(("http://", "https://")) or low.startswith("data:image/")
        bare_file = bool((not is_url) and re.fullmatch(r"[\w\u4e00-\u9fff\-. ]{1,64}\.(png|jpe?g|gif|webp)", avatar))
        if not is_url and bare_file:   # Naked file name → must already be in static/headpic/(with project walk, root relative to URL reference)
            if not (HEADPIC_DIR / avatar).exists():
                return JSONResponse({"ok": False, "error": f"image {avatar!r} is not in static/headpic/ — put the png in that directory (or the desktop headpic seed directory, auto-seeded on restart) and try again"}, status_code=400)
        elif not is_url and re.search(r"[/:\\\\]", avatar):   # Includes path delimiter but is not URL → native absolute path, etc., and cannot be referenced by HTTP
            return JSONResponse({"ok": False, "error": "avatar Please use the file name in image URL (http/https/data), static/headpic/(such as Ar.png), or short text; native absolute path is not supported"}, status_code=400)
        # is_url → release (browser direct reference); remaining plain text → color block display (compatible with old value), release

    for c in convs.values():
        m0 = next((x for x in c.members if x.name == old and (is_human or x.is_llm)), None)   # People class seats should also be positioned to (for avatar)
        if m0 is None:
            continue
        changed_here = False
        if renaming:
            n_hist = c.rename_member(old, new_name)   # memory:members + llm.name + history sender
            p = DATA_DIR / f"{c.session_id}.jsonl"    # Platters: Rewrite only the sender field (the old name in the body is historytext, reserved)
            if n_hist and p.exists():
                try:
                    lines = [ln for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
                    out_lines, dirty = [], False
                    for ln in lines:
                        try:
                            d2 = json.loads(ln)
                        except Exception:  # noqa: BLE001 - Bad rows are kept as is to avoid data loss
                            out_lines.append(ln); continue
                        if str(d2.get("sender", "")) == old:
                            d2["sender"] = new_name; dirty = True
                            out_lines.append(json.dumps(d2, ensure_ascii=False))
                        else:
                            out_lines.append(ln)   # Maintain the original text without changing the line (do not rearrange details such as floating point representation)
                    if dirty:
                        tmp = p.with_suffix(".jsonl.tmp")
                        tmp.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
                        os.replace(tmp, p)   # Atomic substitution to prevent half-write damage to the chathistory
                except OSError as e:   # noqa: BLE001 - file override failed not blocking the rename itself (memory already in effect)
                    print(f"[warn] rename history rewrite failed for {p.name}: {e}")
            changed_here = True
        if avatar is not None:
            c.set_avatar(new_name, avatar)   # Note: You may have just changed your name → to target it with a new name
            changed_here = True
    if not any(c.get("name") == old for c in cfg["members"]):   # Second confirmation (theoretically will not go)
        return JSONResponse({"ok": False, "error": f"member {old!r} does not exist"}, status_code=404)

    ev: dict = {"type": "members", "members": member_views()}   # broadcast new view (including avatar/presence), the front end is based on the sync bubble
    for e in cfg["members"]:   # Write back config.json (first rename and then set avatar, in the same order as memory; people class seats may only be changed to avatar)
        if e.get("name") == old:
            if renaming:
                e["name"] = new_name
            if avatar is not None:
                e["avatar"] = avatar
    if renaming:
        presence_rename(old, new_name)   # Feature # 10: Offline Records Follow New Name
        focus_rename(old, new_name)      # Feature # 10d: Focus registration table key with new name migration (internal tasklink is sid: tid, unrelated to members renaming)
        ev["renamed"] = {"from": old, "to": new_name}
    elif avatar is not None:
        ev["avatar_changed"] = new_name
    err = try_save()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)
    await fanout_all(ev)
    return {"ok": True, "name": new_name}


@app.post("/api/stop")
async def stop(payload: dict = Body(default={})):
    sid = str(payload.get("session") or LAST_ACTIVE["sid"])
    convs.get(sid, next(iter(convs.values()))).stop()
    return {"ok": True}


@app.post("/api/reset")
async def reset(payload: dict = Body(default={})):
    """clear the history of a session (default currently active session)."""
    sid = str(payload.get("session") or LAST_ACTIVE["sid"])
    c = convs.get(sid, next(iter(convs.values())))
    c.reset()
    await send_to_session(sid, {"type": "reset"})
    return {"ok": True}


@app.post("/api/streak")
async def set_streak(payload: dict):
    """settings LLM Continuous speak limit (1-8), effective immediately and written back to config.json."""
    try:
        n = int(payload.get("max_llm_streak", next(iter(convs.values())).max_llm_streak))
    except (TypeError, ValueError):
        return {"ok": False, "error": "max_llm_streak must be an integer"}
    n = max(1, min(n, 8))
    for c in convs.values():
        c.max_llm_streak = n
    cfg["max_llm_streak"] = n
    err = try_save()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)
    await fanout_all({"type": "streak", "value": n})
    return {"ok": True, "max_llm_streak": n}


@app.post("/api/toolmax")
async def set_toolmax(payload: dict):
    """Function 1: settings Upper limit (1-20) of toolcall in a single round, effective immediately and written back to config.json."""
    try:
        n = int(payload.get("max_tool_steps", next(iter(convs.values())).max_tool_steps))
    except (TypeError, ValueError):
        return {"ok": False, "error": "max_tool_steps must be an integer"}
    n = max(1, min(n, 20))
    for c in convs.values():
        c.max_tool_steps = n
    cfg["max_tool_steps"] = n
    err = try_save()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)
    await fanout_all({"type": "toolmax", "value": n})
    return {"ok": True, "max_tool_steps": n}


@app.post("/api/focus")
async def set_focus(payload: dict):   # Feature # 10d: manual Switching the focus schema of an LLM member {name, on} (task-derived links are managed by system auto)
    name = str((payload or {}).get("name") or "").strip()[:64]
    if not name:
        return JSONResponse({"ok": False, "error": "Missing member name"}, status_code=400)
    found = [m for m in next(iter(convs.values())).members if m.name == name and m.is_llm]
    if not found:
        return JSONResponse({"ok": False, "error": f"LLM member {name!r} does not exist"}, status_code=404)
    changed, view = focus_manual_set(name, bool((payload or {}).get("on", True)))
    await _broadcast_focus(name, view)   # manual switch is always effective immediately; give latestview global sync to all windows
    return {"ok": True, **view}


@app.post("/api/private")
async def private_chat(payload: dict = Body(default={})):
    """Feature # 10g/10j: One-on-one taskprogress with members who are focusing.{name, content, session?}: Send a [private-<name>] … (mention =<name>, private_to =<name>) with a human classrole —— * * true Invisible * *: The message and the reply from targetmembers The bodyfilter will be dropped from the other members' LLM context (Feature # 10j), and they will neither see the content nor be interrupted; the UI on the human class side will be visible as usual."""
    sid, err = _resolve_sid(payload)
    if err:
        return err
    c = convs[sid]
    name = str((payload or {}).get("name") or "").strip()[:64]
    text = str((payload or {}).get("content") or "").strip()[:2000]
    found = [m for m in c.members if m.name == name and m.is_llm]
    if not found:
        return JSONResponse({"ok": False, "error": f"LLM member {name!r} does not exist"}, status_code=404)
    if not text:
        return JSONResponse({"ok": False, "error": "private chatcontent cannot be null"}, status_code=400)
    await c.handle_incoming(c.human_name(), f"{PRIVATE_PREFIX}{name}] {text}", mention=name, private_to=name)   # Feature # 10j: channel tag → other memberscontext is not visible
    return {"ok": True}


_IPV4_RE = re.compile(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$")


def _valid_ipv4(s):
    m = _IPV4_RE.match(str(s or "").strip().lower())
    return bool(m and all(0 <= int(g) <= 255 for g in m.groups()))


def _swap_host(url, newhost, newport=None):   # Feature # host: Change the host part of http (s)://< old host > [: port] [/path]; if newport is given, even port will be changed, otherwise port/path will remain as it is
    m = re.match(r"^(https?://)([^:/]+)(:\d+)?(/.*)?$", str(url or ""), re.I)
    return f"{m.group(1)}{newhost}{(':' + str(newport)) if newport else (m.group(3) or '')}{m.group(4) or ''}" if m else url


@app.post("/api/member/host")
async def member_host(payload: dict = Body(default={})):
    """Feature #host (v1.5.1): manually change the host where an agent resides — {name, host(IPv4 or IPv4:port)}. Rewrites the host + port in the member's base_url (original port retained if omitted) and the host in executor.remote.url (remote agent port is independent and not linked to the model port); rebuilding llm + executor in all session memories takes effect immediately, writes back to config.json and broadcasts members; online status is gray first, then automatically green after a successful call. Old addresses mentioned in host_hint/persona settings must be updated manually."""
    name = str((payload or {}).get("name") or "").strip()[:64]
    raw = re.sub(r"^https?://", "", str((payload or {}).get("host") or ""), flags=re.I).strip().rstrip("/").lower()
    hm = re.match(r"^([^:/]+)(?::(\d{1,5}))?$", raw)
    newhost = hm.group(1) if hm else ""
    newport = hm.group(2) if (hm and hm.group(2)) else None
    if not _valid_ipv4(newhost):
        return JSONResponse({"ok": False, "error": f"invalid IPv4 address: {newhost!r}"}, status_code=400)
    if newport is not None and not 1 <= int(newport) <= 65535:
        return JSONResponse({"ok": False, "error": f"port out of range: {newport}"}, status_code=400)
    host_out = f"{newhost}:{newport}" if newport else newhost
    entry = next((m for m in cfg["members"] if str(m.get("name") or "") == name and m.get("type") != "human"), None)
    if entry is None:
        return JSONResponse({"ok": False, "error": f"LLM member {name!r} does not exist"}, status_code=404)
    changed = []
    if isinstance(entry.get("base_url"), str):
        nu = _swap_host(entry["base_url"], newhost, newport)
        if nu != entry["base_url"]:
            entry["base_url"] = nu; changed.append("base_url")
    ex = entry.get("executor")
    if isinstance(ex, dict) and str(ex.get("type")) == "remote" and ex.get("url"):
        nu = _swap_host(ex["url"], newhost)   # the remote agent port switches hosts independently → of the modelport
        if nu != ex["url"]:
            ex["url"] = nu; changed.append("executor.url")
    if not changed:
        return {"ok": True, "name": name, "host": host_out, "changed": False}   # Same address, → idempotent, no action
    try:   # First verify the new config can be built, failed does not change any status
        llms.build(dict(entry)); make_executor(entry.get("executor"))
    except Exception as e:   # noqa: BLE001 - validate but reject directly
        return JSONResponse({"ok": False, "error": f"new address failed validation and was not applied: {e}"}, status_code=400)
    n_live = 0
    for c in list(convs.values()):   # Each session holds a separate instance of llm/executor rebuilt → one by one
        for x in c.members:
            if x.name == name and x.is_llm:
                nl = llms.build(dict(entry)); nl.name = name; x.llm = nl
                x.executor = make_executor(entry.get("executor"))
                n_live += 1
    presence_mark(name, "offline", f"host switched to {host_out}, waiting for first call")   # The old online state has expired. Put the gray → first, and the next successauto will be green again.
    err = try_save()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)
    await fanout_all({"type": "members", "members": member_views(), "host_changed": {"name": name, "host": host_out}})
    return {"ok": True, "name": name, "host": host_out, "changed": True, "live_instances": n_live}


@app.post("/api/member/role")
async def member_role(payload: dict = Body(default={})):
    """Feature # role: modified LLM membersrole —— {name, role(member|leader)}. Person class seat fixed boss; maximum one leader in LLM."""
    name = str((payload or {}).get("name") or "").strip()[:64]
    role = str((payload or {}).get("role") or "").strip().lower()
    if not name:
        return JSONResponse({"ok": False, "error": "Missing member name"}, status_code=400)
    if role not in ("member", "leader"):
        return JSONResponse({"ok": False, "error": "role can only be member or leader"}, status_code=400)
    entry = next((m for m in cfg["members"] if str(m.get("name") or "") == name and m.get("type") != "human"), None)
    if entry is None:
        return JSONResponse({"ok": False, "error": f"LLM member {name!r} does not exist"}, status_code=404)
    if str(entry.get("role") or "member") == role:
        return {"ok": True, "name": name, "role": role, "changed": False}
    if role == "leader":
        for e in cfg["members"]:
            if e.get("type") != "human":
                e["role"] = "member"
        for c in convs.values():
            for x in c.members:
                if x.is_llm:
                    x.role = "member"
    entry["role"] = role
    for c in convs.values():
        for x in c.members:
            if x.name == name and x.is_llm:
                x.role = role
    err = try_save()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)
    await fanout_all({"type": "members", "members": member_views(), "role_changed": {"name": name, "role": role}})
    return {"ok": True, "name": name, "role": role, "changed": True}


@app.post("/api/prompt")
async def set_prompt(payload: dict):
    """the role of one of the LLM members is modified (system prompt), which takes effect immediately and writes back to config.json."""
    name = str(payload.get("name", ""))
    text = payload.get("system_prompt")
    found = _all_convs_members(name)
    if not found or not isinstance(text, str):
        return JSONResponse({"ok": False, "error": f"member {name!r} does not exist or is not an LLM"}, status_code=400)
    for m in found:
        if m.is_llm:
            m.llm.system_prompt = text.strip()[:4000]
    for c in cfg["members"]:
        if c.get("type") != "human" and c.get("name") == name:
            c["system_prompt"] = found[0].llm.system_prompt
            break
    err = try_save()
    if err:
        return JSONResponse({"ok": False, "error": err}, status_code=500)
    await fanout_all({"type": "members", "members": member_views(), "changed_prompt": name})
    return {"ok": True, "name": name}


# ---------- rest: attachmentupload (Feature 5) ------------

MAX_UPLOAD_BYTES = 20 * 1024 * 1024  # Maximum 20MB per file (must match core.conversation.MAX_AGENT_FILE_BYTES)


@app.post("/api/upload")
async def upload(files: list[UploadFile] = File(...), session: str = "", sender: str = ""):
    """Function 5+6: upload attachments to data/uploads/ (the shared file channel directory). Optional query arguments session/sender register "who uploaded in which session" for /api/files listing and in-group preview; returns each file's absolute path (readable in shell by local LLM) + url (/uploads/, accessible by browser/remote agent)."""
    out = []
    for f in files:
        dest = UPLOADS_DIR / safe_upload_name(f.filename)   # Same name as agent [sendfile] enrollment (see core.conversation)
        size = 0
        with open(dest, "wb") as fh:
            while chunk := await f.read(1 << 20):
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    fh.close()
                    dest.unlink(missing_ok=True)
                    return JSONResponse({"ok": False, "error": f"file {f.filename} exceeds the 20MB limit"}, status_code=413)
                fh.write(chunk)
        _append_jsonl(FILE_LOG, {"ts": time.time(), "session": session.strip()[:64],
                                 "sender": sender.strip()[:64], "orig": f.filename or dest.name,
                                 "stored": dest.name, "size": size})
        out.append({"name": f.filename or dest.name, "size": size, "path": str(dest),
                    "url": f"/uploads/{dest.name}", "stored": dest.name})
        # file channelautorefresh: notificationclient has a new registration (the front end receives {type:"files"}, that is, the heavy pull list); only the session with the session is sent, no sid = the whole group is visible → broadcast The whole body
        if session.strip() and session.strip() in convs:
            await send_to_session(session.strip(), {"type": "files", "op": "add", "session": session.strip()})
        else:
            await fanout_all({"type": "files", "op": "add"})
        # the remote agent curl upload → service end reissues an "attachment card" with the membersrole (the front end renders the imagepreview/download chip under the bubble);
        # The person class's own attachment is carried with its WS message, not repeated here; if turn is already in progress,_kickoff auto is merged into the current round
        c0 = convs.get(session.strip()) if session.strip() else None
        sender_n = sender.strip()[:64]
        if c0 is not None and any(mm.is_llm and mm.name == sender_n for mm in c0.members):
            kb = int(size / 1024 + 0.5) or "<1"
            await c0.handle_incoming(sender_n, f"[attachment{len(out)}] {f.filename or dest.name} ({kb} KB)",
                                     files=[{"name": f.filename or dest.name, "stored": dest.name, "size": size}])
    return {"ok": True, "files": out}


# ---------- rest: file channellist (function 6) ----------

@app.get("/api/files")
async def list_files(session: str = ""):
    """The registration record of the group shared file,→ old and new; only the of the session is listed when the session is not null (history entries without sid are considered visible to the whole group)."""
    items = _tail_jsonl(FILE_LOG)
    if session.strip():
        items = [e for e in items if not str(e.get("session") or "") or str(e["session"]) == session.strip()]
    return {"ok": True, "files": items}


# ---------- rest: task + daily record (function 7, 8) ----------

def _resolve_sid(payload: dict) -> tuple[str | None, JSONResponse]:
    sid = str((payload or {}).get("session") or LAST_ACTIVE["sid"])
    if sid not in convs:
        return None, JSONResponse({"ok": False, "error": f"session {sid!r} does not exist"}, status_code=404)
    return sid, None


@app.get("/api/tasks")
async def list_tasks(session: str = ""):
    """tasklist in progress + daily records (grouped by day; each session only statistics its own taskevent, not each other)."""
    sid = session.strip() or LAST_ACTIVE["sid"]
    if sid not in convs:
        return JSONResponse({"ok": False, "error": f"session {sid!r} does not exist"}, status_code=404)
    # tasks/days by sessionisolation: daily ledger statistics only this session self-created/done taskevent (each log with sid)
    events = [e for e in _tail_jsonl(TASK_LOG, 3000) if str(e.get("sid") or "") == sid]
    return {"ok": True, "tasks": TASKS_STORE.get(sid, []), "days": _days_summary(events)}


def parse_chat_dispatch(content: str, member_names: set[str]) -> tuple[bool, str, str]:
    """Feature # 10d Uniform entrance —— chatinput box hand "[task dispatch]..." ≡ taskpanel officially issued. return (is_dispatch, assignee, core): - is_dispatch = False: not [task dispatch] prefix (or with attachment downgrade by caller), processed as normal message; - "[task dispatch] @ member name<body>" → ("",. . . ) assignee = the name, core =<body>; only if @ object is an existing LLM/classmembers and core is not null, otherwise the whole body is a normal message (without accidentally injuring the hand and prefixing); - "[task dispatch]" → assignee = "" (same as panel "not assigned": build order, join group, but no one auto focuses)."""
    if not content.startswith("[task dispatch]"):
        return False, "", ""
    rest = content[len("[task dispatch]"):].lstrip()
    mm = re.match(r"@([^\s@，。！？!?：:；;,]+)", rest)
    if mm:
        cand = mm.group(1)[:64]
        core = rest[len(mm.group(0)):].strip()
        if cand in member_names and core:
            return True, cand, core
        return False, "", ""     # @ is not a member/no body after roll call is sent → as a normal message more securely
    if rest.strip():
        return True, "", rest.strip()[:2000]
    return False, "", ""


async def _task_dispatch(c, sid: str, content_core: str, assignee: str) -> str:
    """[task dispatch] shared kernel (Feature #7/#10d): create the task in the ledger -> (when there is an assignee) attach the focus link before triggering the turn -> broadcast focus/task events. Auto creation and manual [task] creation share this kernel to keep both entry points semantically consistent: - auto ON: assigned members immediately enter focus (_run wave gating is effective for this round, and @ roll call also has opening permission); - auto OFF: after the order is completed, check -> focus_task_unlink, and exit when the last one is not manually opened. _save_tasks() failure raises RuntimeError (REST -> 500 / chat -> downgrade to normal message)."""
    task, focus_ev = _create_task(sid, content_core, assignee)
    text = f"[task dispatch] @{assignee} {content_core}" if assignee else f"[task dispatch] {content_core}"
    await c.handle_incoming(c.human_name(), text, mention=assignee or None)   # @ roll call: @ prioritized by [roll call instruction]
    if focus_ev is not None:           # Focus on → statusmigration global event (issued before task event, UI sync badge/panel/address book switch)
        await _broadcast_focus(focus_ev[0], focus_ev[1])
    await send_to_session(sid, {"type": "task", "op": "create", "sid": sid, "tasks": TASKS_STORE.get(sid, [])})
    return task["id"]


@app.post("/api/task")
async def task_action(payload: dict = Body(default={})):
    """task action. action: - create {content, assignee?, session?}: added and send it immediately (send a [task] @ assignee content with the person classrole, the person named will speak first); - complete {id, session?}: check confirmdone -- remove it from in progresslist (top auto below) and record it in the account of the day; - reorder {ids:[...], session?}: New order after dragsort (ids must correspond to the current task one by one)."""
    action = str(payload.get("action", ""))
    sid, err = _resolve_sid(payload)
    if err:
        return err
    c = convs[sid]

    if action == "create":
        content = str((payload or {}).get("content") or "").strip()[:2000]
        if not content:
            return JSONResponse({"ok": False, "error": "taskcontent cannot be null"}, status_code=400)
        assignee = str(payload.get("assignee") or "").strip()[:64]
        llm_names = {m.name for m in c.members if m.is_llm}
        if assignee and assignee not in llm_names:
            return JSONResponse({"ok": False, "error": f"member {assignee!r} does not exist (or is not an LLM assistant)"}, status_code=400)
        try:
            tid = await _task_dispatch(c, sid, content, assignee)   # Feature #10d unified entry: same kernel as manual [task dispatch]
        except RuntimeError as e:
            return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
        return {"ok": True, "id": tid}

    if action == "complete":
        tid = str(payload.get("id") or "")
        lst = TASKS_STORE.get(sid) or []
        t = next((x for x in lst if x.get("id") == tid), None)
        if t is None:
            return JSONResponse({"ok": False, "error": f"task {tid!r} does not exist (it may already be complete)"}, status_code=404)
        lst.remove(t)   # Directly remove all taskauto tops → below
        _save_tasks()
        _append_jsonl(TASK_LOG, {"ts": time.time(), "day": time.strftime("%Y-%m-%d"),
                                 "action": "complete", "sid": sid,
                                 "task": {k: t.get(k) for k in ("id", "content", "assignee")}})
        # Feature # 10d: task Check done to → remove its focus link; autoexit focus when the last member is assigned to taskdone and is not turned on by manual
        focus_ev = None
        assignee_c = str(t.get("assignee") or "")
        if assignee_c and t.get("id"):
            ch_f, fv = focus_task_unlink(assignee_c, sid, str(t["id"]))
            if ch_f:
                focus_ev = (assignee_c, fv)
        if focus_ev is not None:
            await _broadcast_focus(focus_ev[0], focus_ev[1])
        await send_to_session(sid, {"type": "task", "op": "complete", "sid": sid, "id": tid,
                                    "tasks": TASKS_STORE.get(sid, [])})
        return {"ok": True, "id": tid}

    if action == "reorder":
        ids = payload.get("ids") or []
        lst = TASKS_STORE.get(sid) or []
        by_id = {t["id"]: t for t in lst}
        ordered = [by_id[i] for i in ids if str(i) in by_id]
        if len(ordered) != len(lst):
            return JSONResponse({"ok": False, "error": "the tasklist has changed, please refresh and retry"}, status_code=409)
        TASKS_STORE[sid] = ordered
        _save_tasks()
        await send_to_session(sid, {"type": "task", "op": "reorder", "sid": sid, "tasks": ordered})
        return {"ok": True}

    return JSONResponse({"ok": False, "error": f"unknown action {action!r} (supports create/complete/reorder)"}, status_code=400)


# -------- WebSocket: Bind one session per connection (Function 4) ----------

async def _ws_init(ws: WebSocket):
    await ws.send_json(init_payload(clients[ws]))


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    qsid = (ws.query_params.get("session") or "").strip()
    sid = qsid if qsid in convs else LAST_ACTIVE["sid"]
    await ws.accept()
    clients[ws] = sid
    try:
        await _ws_init(ws)
        while True:
            # Stay away: When the session bound by this connection is deleted by other clients, fall back to the active session and resend init
            if clients.get(ws) not in convs:
                clients[ws] = LAST_ACTIVE["sid"]
                await _ws_init(ws)
            data = await ws.receive_json()
            t = data.get("type")
            if t == "session":  # Toggle context (feature 4): replay init of targetsession
                nsid = str(data.get("id", ""))
                if nsid not in convs:
                    continue
                clients[ws] = nsid
                # Note: The WS sidebar toggle only applies to this connection - do not override global last_active,
                # Otherwise, the exploratory click of either window will cause all new label pages/bat start to fall to another session.
                await _ws_init(ws)
            elif t == "message":
                content = str(data.get("content", "")).strip()[:4000]
                # Function 5 +6: attachment (get the path through /api/upload first), validate and then put it into body + persistent metadata (preview/file channel in the group)
                extra_lines, files_meta, paths = [], [], []
                for i, finfo in enumerate(data.get("files") or [], 1):
                    p = Path(str(finfo.get("path", ""))) if isinstance(finfo, dict) else None
                    try:
                        rp = p.resolve()
                        ok = p.is_file() and UPLOADS_DIR.resolve() in rp.parents
                    except Exception:  # noqa: BLE001
                        ok = False
                    if not (p and ok):
                        continue
                    fname = str(finfo.get("name") or p.name)
                    fsize = int(rp.stat().st_size / 1024 + 0.5)
                    extra_lines.append(f"[attachment{i}] {fname} ({fsize if fsize else '<1'} KB)")
                    files_meta.append({"name": fname, "stored": rp.name, "size": rp.stat().st_size})
                    paths.append(rp)
                for meta, fp in zip(files_meta, paths):  # Feature 6: Small textattachment inline content——remote agent can read without accessing native disk
                    body = _inline_text_file(fp)
                    if body is not None:
                        extra_lines.append(f"[attachment content:{meta['name']}]\n{body}")
                text = content + ("\n\n" if content and extra_lines else "") + "\n".join(extra_lines)
                if not text.strip():
                    continue
                # Feature # 10d unified entrance: chatinput box hand hit "[task dispatch]..." ≡ taskpanel issue - the same set up bill + assignee auto in/out focus;
                # If you do not go to the kernel, there is no "auto on" (no link) or "auto off" (no checkable order) for this path. downgrade with attachment is normal message.
                cws = convs[clients[ws]]
                is_disp, disp_a, disp_core = parse_chat_dispatch(content, {m.name for m in cws.members})
                if is_disp and not files_meta:
                    try:
                        await _task_dispatch(cws, clients[ws], disp_core or content.strip(), disp_a)
                        continue   # Kernel bubbled/triggered turn/broadcast focus + task event
                    except RuntimeError as e:
                        print(f"[task dispatch] failed to write ledger, falling back to a normal message: {e}")     # message is not lost, just no pending orders/auto focus
                # Function 6: @ roll call —— message When opening the header with "@ member name", target the first speak of the LLM members in this round
                mention = None
                mm = re.match(r"^@([^\s@，。！？!?：:；;,]+)", content)
                if mm:
                    cand = mm.group(1)[:64]
                    conv0 = cws
                    if any(m.name == cand for m in conv0.members):
                        mention = cand
                await cws.handle_incoming(cws.human_name(), text, files=files_meta or None, mention=mention)
    except WebSocketDisconnect:
        pass
    finally:
        clients.pop(ws, None)


app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")
app.mount("/", StaticFiles(directory=ROOT / "static", html=True), name="static")

if __name__ == "__main__":
    import uvicorn

    # the default is open to the local area network (file channel: remote agent/other computers need to go through HTTP/uploads/pullattachment); inbound TCP needs to be released by the local firewall<PORT>.
    # Only set host = 127.0.0.1 for this unit. The location is a local area network single usertool, no authentication, please do not expose the public network.
    uvicorn.run(app, host=os.environ.get("HOST", "0.0.0.0"), port=PORT, log_level="info")
