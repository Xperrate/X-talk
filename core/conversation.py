"""GroupChat core: maintenancehistory + drive LLM turn, completely decoupled from the transport layer (WS/HTTP)."""
import asyncio
import base64
import json
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable, Optional

from core.executor import LocalExec, RemoteExec, make_executor
from llms.base import BaseLLM, build as build_llm

DEFAULT_MAX_TOOL_STEPS = 5  # config.json Default value when not config max_tool_steps
MAX_AGENT_FILE_BYTES = 20 * 1024 * 1024   # Feature # 9: Upper limit of single file sent by agent (same as server.MAX_UPLOAD_BYTES /api/upload)

# ---------- Feature # 10c (vision): → imageattachment prompt carries true real pixels ----------
# when membersconfig `"vision": true`, its message with imageattachment is sent as OpenAI multimodal content (list [part]):
# [{"type":"text",...}, {"type":"image_url","image_url":{"url":"data:<mime>;base64,..."}}]。
# the base64 data uri is embedded in the prompt → model on another machine (remote executor) and does not need to pass an additional file to "see" the graph;
# The message of the unopened vision/unreadable image remains the same as the plain text content (the [attachmentN] name line is still in the body, and the behavior is consistent with the legacy).
_VISION_MIME = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp"}


def _msg_vision_parts(m: "Msg", uploads_dir) -> Optional[list]:   # No readable imageattachment → None (call party fallback pure text content)
    imgs = []
    for f in m.files or []:
        name = str(f.get("name") or f.get("stored") or "attachment")
        ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
        mime = _VISION_MIME.get(ext)
        stored = str(f.get("stored") or "")
        if not (mime and uploads_dir and stored):
            continue
        try:
            b64 = base64.b64encode((Path(uploads_dir) / stored).read_bytes()).decode("ascii")   # Read failed/deleted → Skip this graph, do not block speak
        except OSError:
            continue
        imgs.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}})
    if not imgs:
        return None
    return [{"type": "text", "text": m.content or ""}, *imgs]


_VISION_NOTE = ("\n [Visual ability description] You have graph image comprehension: Messages with imageattachment attach graph pixels directly to the messagecontent with a data uri."
                "You can directly describe/analyze the actual content in the graph, don't make yourself a pure textmodel, and don't use 'can't see the graph' to extrapolate.")


def safe_upload_name(orig: str) -> str:
    """Feature # 5/9 Shared by generation uploads/(timestamp + random suffix); also used by server.py /api/upload."""
    base = re.sub(r"[\\/:*?\"<>|\r\n\t]+", "_", Path(str(orig)).name or "file")[:80] or "file"
    return f"{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}-{base}"


_FILE_SEND_RE = re.compile(r"\[sendfile\]\s*(.+?)\s*$")   # Feature # 9: Issue file tag in membersreply (the most stable in the exclusive line, you can also hold it in the sentence)
_TASK_DISPATCH_RE = re.compile(r"^\[task dispatch\]\s*@([^\s@,.!?;:]+)\s+(.+?)\s*$")   # Feature # role: dispatch line in leader reply
_IDENTITY_LINE_RE = re.compile(r"^\s*\[role[:：][^\]]+\]\s*$")   # Feature # role: legacy first-line role tag, stripped from replies
_PASS_RE = re.compile(r"^\s*\[(pass|skip)\]\s*$", re.IGNORECASE)   # Feature # turn: explicit forfeit token
DEFAULT_TURN_TIMEOUT = 90.0   # Feature # turn: seconds to wait for a member to use the speaking right
DEFAULT_MAX_CONTEXT_MESSAGES = 80   # Feature # context: recent messages fed to each LLM turn; 0 disables trimming

# ---------- Feature#10:LLM membersonline/offlinestatus ----------
# Global registration table across all sessions (the reachability of the LLM endpoint is a machine-level property, independent of which session it belongs to).
# Default "online"; active probes and real call results drive transitions: probe/call success -> online,
# repeated probe/call network failures -> offline. Only run-time status, do not write config.json.
PRESENCE: dict[str, dict] = {}   # name -> {"status": "online"/"offline", "reason": str, "ts": float}


def presence_mark(name: str, status: str, reason: str = "") -> bool:
    """updatemembersstatus; return True only if status * * migration * * occurs (the call party decides whether to broadcast accordingly)."""
    cur = PRESENCE.get(name)
    if cur is not None and cur["status"] == status:
        return False
    PRESENCE[name] = {"status": status, "reason": str(reason)[:200], "ts": time.time()}
    return True


def presence_clear(name: str):
    """cleanup its status record when members are moved out of GroupChat."""
    PRESENCE.pop(name, None)


def presence_rename(old: str, new: str):
    """Renamed migrationstatus record (offline reason/timestamp followed by new name)."""
    if old in PRESENCE and new not in PRESENCE:
        PRESENCE[new] = PRESENCE.pop(old)


def presence_status(name: str) -> tuple[str, Optional[dict]]:
    """return (current status, offlineinformation); no record = default online."""
    p = PRESENCE.get(name)
    if not p or p["status"] != "offline":
        return "online", None
    return "offline", {"reason": p["reason"], "ts": p["ts"]}


# ---------- Feature # 10d: members Focus on schema (deep work) ----------
# Global registration table across all sessions ("at work" is independent of which session it belongs to); status only when run, do not write config.json.
# * * Last event first * * (single valid state `on`, no longer OR): Whoever happens after the three writers determines--
# - [task dispatch] @ members → focus_task_link: auto Focus (new work starts to bury the header stem; it will re-enter even if it was previously turned off by manual);
# - The last task assigned is checked done/sessiondelete → focus_task_unlink: * * autoexit * * when there is no remaining (that is, "donetask only acts as an auto switch");
# If there are others that are not donetasked, the status quo remains (usually still focused).
# - usermanual switch (phone book dialing /api/focus)→ * * Effective immediately at any time, free and controllable * *: can be turned off with task, can be turned on without task;
# Keep covering it until the next event (again switch/new dispatch auto in/out doneauto out).
# Focused members * * default do not join ordinary chat waves * * (no longer reply chat); but this round triggers message @ to name it ([task is issued] also with @, @ in the sentence also counts)→
# The opening of the current round was approved, and [focus schema] description was added to the system (priority was given to handling roll call items, and blocked active report was encountered). Progress can also be communicated individually via “private chat”.
FOCUS: dict[str, dict] = {}   # name -> {"on": bool, "tasks": ["<sid>:<tid>", ...]}


def _focus_entry(name: str) -> dict:
    e = FOCUS.get(name)
    if e is None:
        e = FOCUS[name] = {"on": False, "tasks": []}
    return e


def focus_is_on(name: str) -> bool:
    """Whether the members are currently in the focus schema (single valid state)."""
    e = FOCUS.get(name)
    return bool(e and e["on"])


def _focus_payload(name: str) -> dict:   # status shape shared by init/members/focus event (for UI sync)
    e = FOCUS.get(name) or {"on": False, "tasks": []}
    return {"name": name, "on": focus_is_on(name), "task_count": len(set(e["tasks"]))}


def _focus_link_unlink(name: str, sid: str, tid: str, link: bool) -> tuple[bool, dict]:
    """taskevent = auto switch (last writer preferred): dispatch→ entry; last done with no remaining→ exit."""
    if not name or not tid:
        return False, _focus_payload(name)
    e = _focus_entry(name)
    key = f"{sid}:{tid}"
    before = focus_is_on(name)
    if link and key not in e["tasks"]:
        e["tasks"].append(key)
        e["on"] = True                       # New work starts → auto focus (overwrites previous manualclose)
    elif not link:
        try:
            e["tasks"].remove(key)
        except ValueError:
            pass
        if not e["tasks"]:
            e["on"] = False                  # Closing done, no remaining task → autoexit focus
    return (before != focus_is_on(name)), _focus_payload(name)


def focus_manual_set(name: str, on: bool) -> tuple[bool, dict]:   # manual switching: * * Anytime free to control * *, effective immediately and duration to the next event override
    e = _focus_entry(name)
    before = focus_is_on(name)
    e["on"] = bool(on)
    return (before != focus_is_on(name)), _focus_payload(name)


def focus_task_link(name: str, sid: str, tid: str) -> tuple[bool, dict]:   # [task dispatch] → auto enters focus schema
    return _focus_link_unlink(name, sid, tid, True)


def focus_task_unlink(name: str, sid: str, tid: str) -> tuple[bool, dict]:   # taskdone/→sessiondelete remove link; autoexit when no remaining
    return _focus_link_unlink(name, sid, tid, False)


PRIVATE_PREFIX = "[private-"   # Feature # 10g: Message prefix (server /api/private assembled) to communicate progress one-on-one with members who are focusing; other members of this round are constrained by [private chat silence]


def focus_rebuild(tasks_by_sid: "dict[str, list]"):
    """when servicestart, press tasks.json account restore "task derived" focus status (first clear; manual switch is run mode, not persistent)."""
    FOCUS.clear()
    for sid, lst in (tasks_by_sid or {}).items():
        for t in lst or []:
            a = str(t.get("assignee") or "")
            if a and t.get("id"):
                focus_task_link(a, str(sid), str(t["id"]))


def focus_clear_member(name: str):   # members were removed from → GroupChat cleanup for their focus record (voided with tasklink)
    FOCUS.pop(name, None)


def focus_rename(old: str, new: str):   # Rename migrationregistration table key (internal task key is sid: tid, irrelevant to name, unaffected)
    if old in FOCUS and new not in FOCUS:
        FOCUS[new] = FOCUS.pop(old)


def _recent_file_entries(file_log_path: Optional[Path], session_id: str, limit: int = 10) -> list[dict]:
    """Read file_log.json l (new line at the end) and take the latest limit entry belonging to this session; history entries without sid are considered to be shared by the whole group."""
    if not (file_log_path and Path(file_log_path).exists()):
        return []
    try:
        lines = Path(file_log_path).read_text(encoding="utf-8").splitlines()[-300:]
        out = [json.loads(x) for x in reversed(lines) if x.strip()]
    except Exception:  # noqa: BLE001 - log corruption does not affect chat main workflow
        return []
    res = []
    for e in out:
        sid_e = str(e.get("session") or "")
        if sid_e and session_id and sid_e != session_id:
            continue
        res.append(e)
        if len(res) >= limit:
            break
    return res


def _file_channel_block(entries: list[dict], member, uploads_dir: Optional[Path], public_base: str = "") -> str:
    """Format the most recent entry of the file channel into a description text injected into the LLM context; give different pickup methods according to the executor type."""
    if not entries:
        return ""
    lines = ["[Group file channel] Files shared in the group (visible to the whole group after any membersupload, which can be used to pass data to each other):"]
    for e in reversed(entries):
        stored = str(e.get("stored") or "")
        orig = str(e.get("orig") or stored)
        size_kb = int(int(e.get("size", 0)) / 1024 + 0.5)
        sender = str(e.get("sender") or "Unknown members")
        head = f"· {sender} → {orig} ({size_kb if size_kb else '<1'} KB)"
        if isinstance(member.executor, LocalExec):
            p = uploads_dir / stored if (uploads_dir and stored) else None
            loc = str(p) if p else "(file does not exist)"
            lines.append(head + f" | local path: {loc}")
        elif isinstance(member.executor, RemoteExec) and public_base and stored:
            lines.append(head + f" | download URL: {public_base}/uploads/{stored} (use curl.exe -o target_name 'URL' to fetch)")
        else:
            lines.append(head)
    tail = ("When transferring the file to other members, you can specify the file name in the message; localmembers directly use the shell tool to read the path, and remotemembers download and then process it."
            if member.executor is not None else "Quote the name of the instrument bodyfile to discuss these materials with your groupmates.")
    return "\n".join(lines) + "\n" + tail

_TOOL_DOC = """[Host operation ability] You have a command channel to operate this computer where you are, or you can access networkresource through it. Rules: - When you need to view/modifiedfile, system status or crawl web pages, you can directly call shell toolexecute, no need to ask for instructions from the mouth header; but only move things related to the current task; - Only call the tool once per round; after executone, its output will be sent back to you as a new message, and then dispose of the next step according to the output; - Too long output will be truncated, please disassemble small steps for operation; summarize the completed task briefly to the group (do not paste the original log)."""

# [GroupChat Etiquette] Built-in default text. Actually read prompts/turn_etiquette.txt preferentially when running (see Conversation._turn_etiquette (),
# Each time the file is read and changed on site before speak, it takes effect); the file is missing or null before it is returned here.
DEFAULT_TURN_ETIQUETTE = """There may be multiple members in the [X-Talk etiquette] group who are generreply at the same time, and the messages will appear staggered.Please listen/read the other members' recent opinions before commenting: - Speak only when there is a supplement, amendment, or disagreement with someone else's point of view; avoid rushing to repeat verbatim what others have already said; - If there is no new information, a short endorsement (or select not to speak)."""


def new_id() -> str:
    return uuid.uuid4().hex[:16]


@dataclass
class Msg:
    id: str
    sender: str
    content: str
    ts: float = field(default_factory=time.time)
    files: list[dict] = field(default_factory=list)   # CP-0
    mention: Optional[str] = None                     # @ named member name; named member's agent will be woken up first
    private_to: Optional[str] = None                  # Feature # 10j: Non-null = private chatchannel tag (pointing to targetmembers) - not visible at all in the LLM context of other members

    def to_dict(self) -> dict:
        d = {"id": self.id, "sender": self.sender, "content": self.content, "ts": self.ts}
        if self.files:
            d["files"] = self.files
        if self.mention:
            d["mention"] = self.mention
        if self.private_to:   # Feature # 10j: private chatchannel marks the order (filter still works when historyrestore is restarted)
            d["private_to"] = self.private_to
        return d


@dataclass
class Member:
    name: str
    llm: Optional[BaseLLM] = None
    enabled: bool = True
    executor: object = None  # LocalExec/RemoteExec/None; only points to the member's own computer
    avatar: str = ""         # Feature # 10b: customavatar (emoji/short text or image URL); null = default (initials block)
    role: str = "member"     # Feature # role: human = boss; LLM = leader/member (sole leader)

    @property
    def is_llm(self) -> bool:
        return self.llm is not None


BroadcastFn = Callable[[dict], Awaitable[None]]


class Conversation:
    """GroupChatstatus machine. - history: fullmessage (person + LLM), in chronological order - turn drive (concurrency wave model): After the triggered message arrives, all enable LLM members * * at the same time * * wave speak, No more serial wheel streams; replies of different members are staggered according to their respective done.Each member has their own max_llm_streak ("single round limit", that is, a maximum of several per AI), the system prompt word suggests that they wait for others to finish speaking before expressing their opinions; When the entire quota is exhausted, or when the previous wave of less than two memberstrue is opening (one party has finished speaking), the current round ends, and the waiting person class opens again."""

    def __init__(self, cfg: dict, history_path: Optional[Path] = None, session_id: str = "",
                 file_log_path: Optional[Path] = None, uploads_dir: Optional[Path] = None,
                 public_base: str = "", etiquette_path: Optional[Path] = None):
        self.group_name = cfg.get("group_name", "GroupChat")
        # Semantic (concurrency wave model): each LLM member * * the maximum number of speaks * * in a single round (no longer the total number of global consecutive); each wave reads the current value in real time
        self.max_llm_streak = max(1, int(cfg.get("max_llm_streak", 2)))
        # Feature # 1: Maximum number of consecutive callcommandtools for one LLM in a single round (can be adjusted in real time in run time)
        self.max_tool_steps = max(1, int(cfg.get("max_tool_steps", DEFAULT_MAX_TOOL_STEPS)))
        # Feature # context: recent visible messages fed to each LLM turn; 0 disables trimming
        self.max_context_messages = max(0, int(cfg.get("max_context_messages", DEFAULT_MAX_CONTEXT_MESSAGES)))
        self.history_path = Path(history_path) if history_path else None
        # Feature # 6: file channel/@ roll call required contextsource (server.py injection)
        self.session_id = session_id or (Path(history_path).stem if history_path else "")
        self.file_log_path = file_log_path
        self.uploads_dir = uploads_dir
        self.public_base = public_base.rstrip("/")
        # [GroupChat etiquette] prompt file injected at run (default prompts/turn_etiquette.txt, incoming from server.py); read live before each speak
        self.etiquette_path = etiquette_path
        self.broadcast: Optional[BroadcastFn] = None
        # Feature # 10: presence is global status (endpoint reachability is independent of session), server.py inject fanout_all migrationnotification all clients → at once
        self.global_broadcast: Optional[BroadcastFn] = None
        # Feature # role: [task dispatch] in leaderreply Create a task order channel injected by the server (no extra person classmessage)
        self.task_dispatch_fn: Optional[Callable[..., Awaitable[str]]] = None

        self.members: list[Member] = []
        for m in cfg.get("members", []):
            name = m["name"]
            if m["type"] == "human":
                self.members.append(Member(name, None, True, avatar=str(m.get("avatar") or "")[:128], role="boss"))   # Feature # 10b: Person class seats can also be customavatar (default Me.png from front end); Feature # role: Person class fixed boss
            else:
                llm = build_llm(m)
                llm.name = name
                executor = make_executor(m.get("executor"))
                avatar = str(m.get("avatar") or "")[:128]   # Feature#10b
                role = "leader" if str(m.get("role") or "member") == "leader" else "member"
                self.members.append(Member(name, llm, bool(m.get("enabled", True)), executor, avatar, role))

        if not any(not m.is_llm for m in self.members):
            raise ValueError("at least one member of type = human is required in config")

        self.history: list[Msg] = []
        self._loop_task: Optional[asyncio.Task] = None
        self._gen_tasks: set[asyncio.Task] = set()   # All LLM tasks (concurrency waves, can be multiple) currently being generated
        self._followup_pending = False               # Feature # 10h: The wave in progress has been triggered again. After the end of → this round, run a round to prevent the message from being silently discarded.
        self.stop_event = asyncio.Event()

    # ---------- members / history ----------

    def human_name(self) -> str:
        return next(m.name for m in self.members if not m.is_llm)

    def append(self, sender: str, content: str, files=None, mention: Optional[str] = None,
               private_to: Optional[str] = None) -> Msg:   # Feature # 10j: private_to = private chatchannel tag (only visible to targetmemberscontext)
        msg = Msg(new_id(), sender, content.strip(), files=list(files or []), mention=mention, private_to=private_to)
        self.history.append(msg)
        if self.history_path is not None:
            with open(self.history_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(msg.to_dict(), ensure_ascii=False) + "\n")
        return msg

    def load_history(self):
        """Continue the previous GroupChat from jsonl restorehistory (service restart)."""
        if self.history_path is None or not self.history_path.exists():
            return
        for line in self.history_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
                files = [f for f in (d.get("files") or []) if isinstance(f, dict)]
                self.history.append(Msg(d["id"], d["sender"], d["content"], float(d.get("ts", 0.0)),
                                        files=files, mention=d.get("mention"), private_to=d.get("private_to")))   # Feature # 10j: restoreprivate chatchannel flag
            except Exception:
                pass

    def reset(self):
        self.history.clear()
        if self.history_path is not None:
            self.history_path.write_text("", encoding="utf-8")

    # ---------- Dynamic management of members (Function 2: Add and delete assistants directly in the UI) ----------

    def add_llm_member(self, m_cfg: dict) -> Member:
        """when running, add an LLM member; m_cfg as an element in config.json members []."""
        name = str(m_cfg.get("name") or "").strip()[:64]
        if not name:
            raise ValueError("the member name character cannot be null")
        if any(x.name == name for x in self.members):
            raise ValueError(f"a member with the same name {name!r} already exists in the group")
        llm = build_llm(dict(m_cfg))          # Unknown type/missing model throws exception, switching from upper layer to errorresponse
        llm.name = name
        executor = make_executor(m_cfg.get("executor"))   # Etiquette prompt read on speak (see_turn_etiquette), new membersauto applies
        avatar = str(m_cfg.get("avatar") or "")[:128]     # Feature # 10b: customavatar, null = default
        role = "leader" if str(m_cfg.get("role") or "member") == "leader" else "member"
        member = Member(name, llm, bool(m_cfg.get("enabled", True)), executor, avatar, role)
        self.members.append(member)
        return member

    def remove_member(self, name: str):
        """remove an LLM member on run (people class seats cannot be deleted)."""
        for i, x in enumerate(self.members):
            if x.name == name and x.is_llm:
                del self.members[i]
                return
        raise ValueError(f"LLM member {name!r} does not exist")

    def rename_member(self, old: str, new: str) -> int:
        """Feature # 10b (memory section): Rename —— members list + llm.name + fullhistory Msg.sender; return the number of history entries changed."""
        for x in self.members:
            if x.name == old and x.is_llm:
                x.name = new
                if x.llm is not None:
                    x.llm.name = new   # to_api_messages by name map assistant/user, must sync
        n = 0
        for m in self.history:
            if m.sender == old:
                m.sender = new
                n += 1
        return n

    def set_avatar(self, name: str, avatar: str):
        """Feature # 10b: customavatar (LLM or person class seat) for updatemembers; "" = Clear back the built-in default graph (front end pocket Ai.png/Me.png)."""
        for x in self.members:
            if x.name == name:
                x.avatar = (avatar or "")[:128]

    def leader_name(self) -> str:
        """Feature # role: Current unique leader name; null string if none."""
        return next((m.name for m in self.members if m.is_llm and m.role == "leader"), "")

    def set_role(self, name: str, role: str):
        """Feature # role: modified LLM role; Person class seat fixed boss."""
        role = "leader" if str(role or "").strip().lower() == "leader" else "member"
        for x in self.members:
            if x.name == name:
                if not x.is_llm:
                    raise ValueError("Person class seat role is fixed as boss")
                x.role = role
                return
        raise ValueError(f"member {name!r} does not exist")

    # ---------- turn drive ----------

    async def handle_incoming(self, sender: str, content: str, files=None, mention: Optional[str] = None,
                              private_to: Optional[str] = None) -> Msg:   # Feature # 10j: private_to Transparent to append (private chatchannel tag)
        """New message entry (human class or LLM): Record + broadcast + trigger subsequent turn. Feature # 6: files = [{name,stored,size}] attachment metadata persisted/replayed with message; mention = by @ member name, Its context will be injected with [roll call instruction] priority processing (all enablemembers in this round speak at the same time under the concurrency wave model, @ no longer determines the order)."""
        self.stop_event.clear()
        msg = self.append(sender, content, files=files, mention=mention, private_to=private_to)
        await self._emit({"type": "message", **msg.to_dict()})
        self._kickoff()
        return msg

    def _context_extras(self, member: Member) -> str:
        """Feature # 6: @ Call instruction + group file channel recent entries (only injected into tool-supported turncontext)."""
        parts = []
        recent = self._visible_history(member.name)[-4:]   # Feature # 10j: Take according to the visible view of the members (private chatcontent must not leak into any system injection)
        if any(getattr(m, "mention", None) == member.name for m in recent):
            named_src = next((m.content[:300] for m in reversed(recent) if getattr(m, "mention", None) == member.name), "")
            parts.append("[Call instruction] user just specially @ you, target gave you the following work request (please prioritize): \n" + named_src)
        entries = _recent_file_entries(self.file_log_path, self.session_id)
        block = _file_channel_block(entries, member, self.uploads_dir, self.public_base)
        if block:
            parts.append(block)
        return "\n\n".join(parts)

    def _turn_etiquette(self) -> str:
        """[GroupChat etiquette] prompt: read from etiquette_path (prompts/turn_etiquette.txt) before each speak, After saving, it will take effect for the next LLM message without restarting; when file is missing or null, it will fall back to the built-in default_turn_ETIQUETTE."""
        p = self.etiquette_path
        if p is not None:
            try:
                text = Path(p).read_text(encoding="utf-8-sig").strip()   # utf-8-sig tolerance notepad BOM
                if text:
                    return text
            except OSError:
                pass
        return DEFAULT_TURN_ETIQUETTE

    def _identity_line(self, member: Member) -> str:
        """Feature # role: The current role that must be explicitly declared before the LLM speak."""
        return f"[role:{'leader' if member.role == 'leader' else 'member'}]"

    def _strip_identity(self, text: str) -> str:
        """Feature # role: strip legacy first-line role tags instead of forcing them."""
        lines = (text or "").splitlines()
        while lines and _IDENTITY_LINE_RE.match(lines[0]):
            lines.pop(0)
        return "\n".join(lines).strip()

    def _hierarchy_note(self, member: Member) -> str:
        """Feature # role: Inject organization structure and task/upload/private message rules."""
        boss = self.human_name()
        leader = self.leader_name()
        common = (
            f"\n[org structure] User \"{boss}\" is the boss, not automatically the leader; at most one AI member can be leader."
            "Do not output `[role:leader]` or `[role:member]` tags in replies."
            "When uploading or delivering a file, if the leader or boss target has a file folder, place it in the target file folder;"
            "if no path is specified, default to the download directory on your computer (Windows is usually %USERPROFILE%\\Downloads)."
            "After upload/copy is done, reply with the file name and full path in the group."
        )
        if member.role == "leader":
            return common + (
                "\n[your role] You are the current leader."
                "You may assign work to regular members; the format must be a separate line:"
                "[task dispatch] @member_name task_content; you cannot assign work to yourself, the boss, or another leader."
                "Prioritize tasks from the boss; if a boss task duplicates or conflicts with your previous plan, do not guess—have members ask the boss via [private message]."
            )
        if leader:
            return common + (
                f"\n[your role] You are a regular member; the current leader is \"{leader}\"."
                "You accept assignments from the boss and the leader;"
                "if tasks from the boss and leader duplicate or conflict, do not execute yet; send a separate line [private message] to ask the boss:"
                "[private message] task conflict:...; wait for the boss's explicit answer before executing or not. Regular members must not use [task dispatch] to assign work to others."
            )
        return common + (
            "[your role] You are a regular member and do not currently have a leader."
            "You only accept assignments from the boss;"
            "you may not use [task dispatch] to assign work to others. If you encounter a conflict that requires instructions, use [private message] to ask the boss."
        )

    def _file_send_doc(self, member: Member) -> str:
        """Feature # 9: Send the file to the method in the group - injected by the machine where the members are located (only members with a shell make sense): localmembers mark the line with [sendfile], service side verification + registration; remotemembers disk cannot be read natively, use curl to push /api/upload instead."""
        if not (member.executor and getattr(member.llm, "supports_tools", False)):
            return ""
        if isinstance(member.executor, LocalExec):
            return ("[sendfile] When you need to share a file generated or found on your computer to the group, write a separate line in the final reply:"
                    "[sendfile] <absolute path>")
        if isinstance(member.executor, RemoteExec) and self.public_base:
            return ("[sendfile] Your computer is not on the same machine as the group service. To send a local file to the group, use shell once:"
                    f'curl.exe -s -F "files=@<absolute path of the file>" "{self.public_base}/api/upload?session={self.session_id}&sender=your name"\n'
                    'After it returns ok: true, GroupChat will automatically show the attachment notification you sent (no need to repeat the declaration); if needed, add one sentence about what the file is and how to use it; single file ≤ 20MB.')
        return ""

    def _prepare_vision(self, member):   # Feature # 10c (vision): The imageattachment pre-encoding in the → history of vision members is the data uri (Msg transient attribute: by messagecache, not by order); non-vision members are not affected at all
        if self.uploads_dir is None or not getattr(getattr(member, "llm", None), "cfg", {}).get("vision"):
            return
        for m in self.history:   # filelist no longer changes after messages.append skip → shaped, avoid repeating base64 encoding large graph
            if getattr(m, "_vshaped", False):
                continue
            setattr(m, "_llm_content", _msg_vision_parts(m, self.uploads_dir))
            setattr(m, "_vshaped", True)

    def _hist_entry(self, m: "Msg") -> str | list:   # Feature # 10c (vision): multimodal content with graphmessage for pre-encoding, otherwise plain text (shared by two prompt paths)
        vc = getattr(m, "_llm_content", None)
        return vc if vc is not None else m.content

    def _visible_history(self, agent_name: str) -> list["Msg"]:   # Feature # 10j: The message of view--private_to = X by membersfilter is only visible to X (on the human class side); other members → do not exist in the LLM context at all and will not be interrupted/disturbed. Old message does not have this field = public, backwards compatible
        return [m for m in self.history if not (m.private_to and m.private_to != agent_name and m.sender != agent_name)]

    def _context_history(self, agent_name: str) -> list["Msg"]:
        """Feature # context: bounded LLM context. Full history stays on disk, but each turn feeds only the recent visible window."""
        hist = self._visible_history(agent_name)
        limit = max(0, int(os.environ.get("MAX_CONTEXT_MESSAGES", self.max_context_messages)))
        if limit <= 0 or len(hist) <= limit:
            return hist
        return hist[-limit:]

    async def _agent_turn(self, member: Member) -> str:
        """The full speak of an LLM member: the package may contain multiple toolcalls (to operate their own computer), and eventually return a GroupChattext."""
        llm = member.llm
        etiq = self._turn_etiquette()   # On-site reading file → user changes this round will take effect
        vnote = _VISION_NOTE if (llm.cfg or {}).get("vision") else ""   # Feature # 10c (vision): Visual ability description (correct "Me is pure textmodel" self Me setting limit)
        hnote = self._hierarchy_note(member)   # Feature # role: boss/leader/members, task issuance, uploaddefaultdownload, conflict private message
        tnote = ("\n[turn-taking] Members speak one at a time in order. When it is your turn, either reply normally or, if you have nothing useful to add, output exactly [pass] on a single line. "
                 "Do not output [role:...] tags.")   # Feature # turn: sequential speaking right and explicit forfeit
        visible = self._visible_history(member.name)
        ctx = self._context_history(member.name)
        cnote = ("\n[context window] Older messages were omitted from this turn; only the most recent visible messages are included."
                 if len(ctx) < len(visible) else "")   # Feature # context: bounded prompt window
        fnote = ""                      # Feature # 10d: Members in the Focus schema only come here if they are named by @ - explain the rules and encourage proactive report questions
        if focus_is_on(member.name):
            _n_tasks = len(set((FOCUS.get(member.name) or {}).get("tasks") or []))
            fnote = ("[focus mode] You are currently in focus mode (you are heads-down working and usually do not participate in small talk; stay silent)."
                     f"Someone @mentioned you this round ({('also ' + str(_n_tasks) + ' assigned task(s) uncompleted, ') if _n_tasks else ''}please prioritize the mentioned task);"
                     "if you get blocked or run into problems, proactively report to the group; the user can also contact you individually through private chat for progress.")
        rnote = ""                      # Feature # 10f/10g: This round of role division —— @ roll call (only objections/suggestions can be made); [private-X] round of other people's silence throughout the whole process
        rt = getattr(self, "_round_trig", None)
        _rcontent = str(getattr(rt, "content", "") or "") if rt is not None else ""
        if rt is not None and _rcontent.startswith(PRIVATE_PREFIX):   # private chat rounds: only targetmembers participate in the dialogue
            _ptarget = str(getattr(rt, "mention") or "").strip()
            if member.name != _ptarget:
                rnote = (f"\n[private silence] This is one-to-one communication between the user and {_ptarget} (task progress/details)—you are not in that conversation,"
                         "do not post anything (no need to agree, interject, or add), unless someone directly @mentions you.")
        elif rt is not None:
            _mset = [n for n in (getattr(self, "_round_mentioned", set()) or ())]   # Focused members with @ release in sentences
            _rtarget = str(rt.mention) if getattr(rt, "mention", None) else (_mset[0] if len(_mset) == 1 else "")
            if _rtarget and member.name != _rtarget:
                rnote = (f"\n[not your task] This message/task is assigned to {_rtarget}, not you: **do not execute, do, or answer on their behalf**;"
                         "only if you find an obvious problem with their approach or have an important improvement suggestion may you briefly interject once (objection/suggestion); otherwise stay silent—no need to say \"received\" or \"it's up to them\".")
        self._prepare_vision(member)    # vision members: → imageattachment data uri pre-encoding (transient attribute for two prompt path reads)
        if not (member.executor and getattr(llm, "supports_tools", False)):
            llm.extra_system = etiq + vnote + hnote + tnote + cnote + fnote + rnote   # Non-toolpath: spelled from to_api_messages () to the end of the system; bring with you vision/role/turn/context/focus/division of labor instructions
            return await llm.reply(ctx)   # Feature # 10j: Only feed the members visible view; Feature # context: bounded recent window

        tools = llm.get_tool_specs(str(llm.cfg.get("host_hint", "")))
        # toolpath manually constructs messages and does not go to_api_messages, so etiquette/visual/focus text is appended to this manual (non-toolmembers are brought by extra_system auto)
        sys_content = llm.system_prompt + "\n" + _TOOL_DOC + "\n\n" + etiq + vnote + hnote + tnote + cnote + fnote + rnote
        fdoc = self._file_send_doc(member)   # Feature # 9: [filesend] usage by members (local = tagline/remote = curl), toolpath injection only
        if fdoc:
            sys_content += "\n\n" + fdoc
        extras = self._context_extras(member)
        if extras:
            sys_content += "\n\n" + extras
        messages: list[dict] = [{"role": "system", "content": sys_content}]
        for m in ctx:   # Feature # 10j: feed only the members visible view (private chatmessage is not visible to them); Feature # context: bounded recent window; Feature # 10c (vision): with graphmessage → multimodal content (text + image data uri), the rest pure text
            role = "assistant" if m.sender == member.name else "user"
            messages.append({"role": role, "content": self._hist_entry(m)})

        # Feature # 10i: Soft budget - top to max_tool_steps No longer a phrase to give up: first reminder in the same context and put a small amount of extra credits;
        # The hardtop still does not converge. → The last time, do a "call without tool", let it summarize progress by itself in a complete context (the middle shell output is all in messages, not lost)
        TOOL_SOFT_EXTRA = 5   # After the top to the regular limit: reminder once and the number of additional allowed bars
        step = 0
        soft_reminded = False
        while True:
            res = await llm.chat_with_tools(messages, tools)
            tcs = res.get("tool_calls") or []
            content = (res.get("content") or "").strip()
            if not tcs:
                return content or "(No reply generated)"

            messages.append({
                "role": "assistant", "content": "",
                "tool_calls": [{"id": t["id"], "type": "function",
                                "function": {"name": t["name"], "arguments": str(t["arguments"])}} for t in tcs],
            })
            for t in tcs:
                cmd = ""
                try:
                    args = json.loads(t["arguments"]) if isinstance(t["arguments"], str) else (t["arguments"] or {})
                    cmd = str(args.get("cmd", ""))
                except Exception:
                    pass
                await self._emit({"type": "tool", "sender": member.name, "name": t["name"], "cmd": cmd})
                try:
                    out = await member.executor.run(cmd) if t["name"] == "shell" \
                        else f"(unsupported tool {t['name']}; only shell is available)"
                except Exception as e:
                    out = f"[execution failed] {e}"
                messages.append({"role": "tool", "tool_call_id": t["id"], "content": str(out)[:8000]})
            step += 1
            cap = self.max_tool_steps   # Feature # 1: At each step of reading the current value, the UI adjustment will take effect this round (the increase in the quota in the middle will also be relaxed)
            if soft_reminded and step >= cap + TOOL_SOFT_EXTRA:
                break                                  # Mandatory summary of hard top → walks below
            if not soft_reminded and step >= cap:      # Reminder to the regular quota for the → first time, open additional quota (context is fully retained, do not restart turn)
                messages.append({"role": "user",
                                 "content": (f"[system notice] This round has used up the {cap} tool-call quota. If the task is unfinished you may continue calling tools to finish"
                                             f"(at most +{TOOL_SOFT_EXTRA} more); if you are close to done, prioritize finishing and output a result summary directly.")})
                soft_reminded = True

        messages.append({"role": "user",
                         "content": ("[system] The toolcall quota for this round has been exhausted, and you can no longer exececommand. Do not issue any more toolcall——"
                                     "Summarize in 2 ~ 4 sentences directly: what has been done, where is the card (including the middle result of the key), and the next step of the proposal.")})
        try:
            res2 = await llm.chat_with_tools(messages, [])   # tools = [] → Adapter without tools field → can only be plain textoutput
            summary = str(res2.get("content") or "").strip()
        except Exception:                                    # noqa: BLE001 - Return guaranteed copy when all calls fail (better than nothing)
            summary = ""
        return summary or f"({step} commands were executed this round without convergence and no progress summary could be generated; please issue a more specific instruction.)"

    def _kickoff(self):
        active = [m for m in self.members if m.is_llm and m.enabled]
        if not active:
            return
        if self._loop_task is None or self._loop_task.done():
            self._followup_pending = False   # The new round of start digests the old marker (if the previous round is interrupted by stop, the residual marker does not produce redundant makeup)
            self._loop_task = asyncio.get_running_loop().create_task(self._run())
        else:   # Feature # 10h: There is already a turn in transit (wave not completed)→ This trigger will not be processed this round, marking will be completed after_run
            self._followup_pending = True

    async def _run(self):
        """Sequential turn-taking: after a trigger, enabled LLM members take turns in order. max_llm_streak is the maximum number of speaking rounds per member; pass/timeout forfeits that turn and does not count as a spoken round. A round with fewer than two speakers ends the discussion. Focused members are skipped unless mentioned."""
        active = [m for m in self.members if m.is_llm and m.enabled]
        # Feature # 10d: Focus gating - only if * * this round triggers message * * (the latest one at the start of turn) @ points a focus member, → it is approved to open in this round;
        # Do not sweep the earlier history, avoid the old @ leakage to the subsequent ordinary epoch (so that "no more reply chat" is named). [task dispatch] comes with mention = assignee.
        trig = self.history[-1] if self.history else None
        mentioned: set[str] = {str(trig.mention)} if (trig is not None and getattr(trig, "mention", None)) else set()
        # Relaxation (focus members only): Official mention only recognizes "message opens header @ name" (WS parse), but user is used to writing @ in sentences--
        # "@ point to focus agent must reply": trigger "@ < the focus member name >" in the body is also released; non-focused members are not affected by this gating, and there is no change in behavior.
        if trig is not None:
            _tcontent = str(getattr(trig, "content", "") or "")
            for _nm in [x.name for x in active if focus_is_on(x.name)]:
                if f"@{_nm}" in _tcontent:
                    mentioned.add(_nm)
        active_eff = [a for a in active if (not focus_is_on(a.name)) or (a.name in mentioned)]
        # Feature # 10j: Pure private chat round ([private-X] with private_to)→ Only targetmembers are awakened generation; other members will not occur even generation (invisible + does not interrupt dual insurance, and does not waste tokens). If target is disabled, it is returned to the original logic, and turn is not swallowed.
        if trig is not None and getattr(trig, "private_to", None):
            _pt = str(trig.private_to)
            if _pt == self.human_name():   # Feature # role: agent → Only keeps the agent who initiated the private message to the boss, and does not wake up other members
                active_eff = [a for a in active_eff if a.name == str(getattr(trig, "sender", "") or "")] or active_eff
            else:
                active_eff = [a for a in active_eff if a.name == _pt] or active_eff
        # Feature # 10f/10g: This round triggers the same "role division" as the roll call objectsnapshot - the turn of each member in the same round reads from here (@ roll call does not do/private chat round silence)
        self._round_trig = trig
        self._round_mentioned = mentioned
        used: dict[str, int] = {a.name: 0 for a in active_eff}
        timeout = float(os.environ.get("TURN_TIMEOUT", DEFAULT_TURN_TIMEOUT))
        try:
            while not self.stop_event.is_set() and active_eff:
                cap = self.max_llm_streak   # Current value per round read
                if len(active_eff) <= 1:
                    cap = min(cap, 1)       # Single active agent: avoid self-talk beyond one reply
                round_spoke = 0
                for a in active_eff:
                    if self.stop_event.is_set() or used[a.name] >= cap:
                        continue
                    _, status = await self._take_turn(a, timeout)
                    if status == "spoke":
                        used[a.name] += 1
                        round_spoke += 1
                    elif status == "error":
                        used[a.name] = cap   # Error members are out of the current round, no longer occupying subsequent turns
                if self.stop_event.is_set() or round_spoke < 2:
                    break
        finally:
            self._loop_task = None
            # Feature # 10h: This round in progress If there is a new trigger that has not been processed, → immediately release the running wheel with latestmessage as the trigger (manual stop is not made up, respect stop)
            if self._followup_pending and not self.stop_event.is_set():
                self._followup_pending = False
                active2 = [m for m in self.members if m.is_llm and m.enabled]
                if active2:
                    self._loop_task = asyncio.get_running_loop().create_task(self._run())

    async def _take_turn(self, member: Member, timeout: float) -> tuple[str, str]:
        """Give one member the speaking right. Returns (text, status); status is spoke/pass/timeout/cancelled/error."""
        await self._emit({"type": "typing", "sender": member.name})
        task = asyncio.create_task(self._speak(member))
        self._gen_tasks.add(task)
        try:
            text, status = await asyncio.wait_for(task, timeout)
            if status in ("spoke", "pass"):
                await self._presence_notify(member.name, "online")
            return text, status
        except asyncio.TimeoutError:
            task.cancel()
            await self._emit({"type": "pass", "sender": member.name, "reason": "timeout"})
            return "", "timeout"
        except asyncio.CancelledError:
            await self._emit({"type": "stopped", "sender": member.name})
            return "", "cancelled"
        except Exception as e:   # noqa: BLE001
            await self._emit({"type": "error", "sender": member.name, "content": str(e)})
            await self._presence_notify(member.name, "offline", e)
            return "", "error"
        finally:
            self._gen_tasks.discard(task)

    async def _presence_notify(self, name: str, status: str, reason=None):
        """Feature # 10: Only when statusmigration broadcast presence event; take the global channel (server injects fanout_all), if none, return session broadcast."""
        if not presence_mark(name, "online" if status == "online" else "offline", "" if reason is None else str(reason)):
            return
        ev: dict = {"type": "presence", "name": name, "status": status}
        if status == "offline":
            info = PRESENCE.get(name) or {}
            ev["reason"] = (info.get("reason") or "")[:200]
        fn = self.global_broadcast or self.broadcast
        if fn is not None:
            await fn(ev)

    def _materialize_agent_files(self, member: Member, text: str) -> tuple[str, list[dict]]:
        """Feature # 9: Handles the [sendfile] tag line in the LLM reply. - localmembers (disk local): validate exists/→size cached in uploads/(referenced in-place) + registration file_log.json l (same shape as /api/upload), replace the marker line with the `[attachmentn] (<KB>)` description consistent with the human attachment; - remotemembers: This service cannot read the other party's disk, rewritten as curl.exe uploadprompt (guide it to correct itself). return (new body, attachment meta datalist); return as is when there is no tag."""
        if "[sendfile]" not in text:
            return text, []
        local = isinstance(member.executor, LocalExec) and self.uploads_dir is not None \
            and self.file_log_path is not None
        meta: list[dict] = []
        out_lines: list[str] = []
        for line in text.splitlines():
            m2 = _FILE_SEND_RE.search(line)
            if not m2:
                out_lines.append(line)
                continue
            pre = line[:m2.start()].rstrip()
            prefix = (pre + " ") if pre else ""   # In the sentence, marker retains the previous sentence; if it is exclusive, it does not leave null cells
            raw = m2.group(1).strip().strip('"')
            if local:
                p, rp = Path(raw), None
                try:
                    rp = p.resolve()
                    size = rp.stat().st_size if rp.is_file() else 0
                except OSError:
                    rp, size = None, 0
                if rp is None or not rp.is_file():
                    out_lines.append(prefix + f"[file send failed] {raw} does not exist (check the path)")
                    continue
                if size > MAX_AGENT_FILE_BYTES:
                    out_lines.append(prefix + f"[file send failed] {rp.name} exceeds the 20MB limit")
                    continue
                try:
                    up = Path(self.uploads_dir)
                    up.mkdir(parents=True, exist_ok=True)
                    stored_name = rp.name if rp.is_relative_to(up.resolve()) else safe_upload_name(rp.name)
                    if stored_name != rp.name:
                        shutil.copyfile(rp, up / stored_name)   # Keep the original in place after registration (the agent may continue to use it)
                except OSError as e:   # noqa: BLE001 - Registration failed does not block the message itself
                    out_lines.append(prefix + f"[file send failed] {rp.name} failed to write to shared directory: {e}")
                    continue
                with open(self.file_log_path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps({"ts": time.time(), "session": self.session_id[:64], "sender": member.name[:64],
                                         "orig": rp.name, "stored": stored_name, "size": size}, ensure_ascii=False) + "\n")
                meta.append({"name": rp.name, "stored": stored_name, "size": size})
                out_lines.append(prefix + f"[attachment{len(meta)}] {rp.name} ({int(size / 1024 + 0.5) or '<1'} KB)")
            else:   # remotemembers (or lack of uploads/file_log config): This service can't do it for you, give the correct uploadcommand
                out_lines.append(prefix + f"[file send failed] You are a remote member; the group service cannot read your local disk—use shell instead:"
                                     f' curl.exe -s -F "files=@{raw}" "{self.public_base}/api/upload?session={self.session_id}&sender=your name"')
        return "\n".join(out_lines), meta

    async def _materialize_task_dispatch(self, member: Member, text: str) -> tuple[str, Optional[str]]:
        """Feature # role: Handles [task issuance] @ members content in leaderreply. return (new body, first valid assignee).Invalid target/no channel will be rewritten as failedprompt without blocking leaderspeak."""
        if member.role != "leader" or "[task dispatch]" not in text:
            return text, None
        out_lines: list[str] = []
        first_assignee: Optional[str] = None
        for line in text.splitlines():
            m2 = _TASK_DISPATCH_RE.match(line)
            if not m2:
                out_lines.append(line)
                continue
            target = m2.group(1)[:64]
            core = m2.group(2)[:2000]
            target_member = next((x for x in self.members if x.name == target and x.is_llm), None)
            if target_member is None:
                out_lines.append(f"[task dispatch failed] @{target} is not an existing LLM member")
                continue
            if target_member.role != "member":
                out_lines.append(f"[task dispatch failed] @{target} is not a regular member")
                continue
            if self.task_dispatch_fn is None:
                out_lines.append("[task dispatch failed] the server did not inject the task channel")
                continue
            try:
                tid = await self.task_dispatch_fn(core, target, member.name)
            except Exception as e:   # noqa: BLE001 - dispatch failure only rewrites the line and prevents the leader's whole reply from disappearing
                out_lines.append(f"[task dispatch failed] @{target}: {e}")
                continue
            if first_assignee is None:
                first_assignee = target
            self._followup_pending = True   # Feature # role: after the leader dispatches, run a round so the assigned member can see and respond
            out_lines.append(f"[task dispatch] @{target} {core} (task #{tid})")
        return "\n".join(out_lines), first_assignee

    async def _speak(self, member: Member) -> tuple[str, str]:
        """One member's turn. Returns (text, status); "" + pass means forfeit without posting."""
        reply = await self._agent_turn(member)   # CancelledError/exception thrown as is, handled by _take_turn
        r = (reply or "").strip()
        if not r:
            await self._emit({"type": "pass", "sender": member.name, "reason": "empty"})
            return "", "pass"
        text = self._strip_identity(r)   # Feature # role: legacy role tags are stripped, not displayed
        if _PASS_RE.match(text):
            await self._emit({"type": "pass", "sender": member.name, "reason": "explicit"})
            return "", "pass"
        text, files_meta = self._materialize_agent_files(member, text)   # Feature # 9: [sendfile] Tag → group file channelattachment
        if files_meta:
            await self._emit({"type": "files", "op": "add", "session": self.session_id})   # Frontend "file channel" autorefresh (same event as /api/upload)
        mention_tag = None
        if member.role == "leader" and "[task dispatch]" in text:   # Feature # role: leader dispatch order -> /hang focus, but do not repeat a message with a human role
            text, mention_tag = await self._materialize_task_dispatch(member, text)
        pt_tag = ""
        rt = getattr(self, "_round_trig", None)
        if (rt is not None and getattr(rt, "private_to", None) and str(getattr(rt, "mention") or "") == member.name):   # Feature # 10j: This round is a reply to Me's private chat → Me belongs to the same one-to-one channel (other memberscontext is not visible)
            pt_tag = str(rt.private_to)
        elif "[private message]" in text:   # Feature # role: member proactively private-messaging the boss
            pt_tag = self.human_name()
        msg = self.append(member.name, text, files=files_meta or None, mention=mention_tag, private_to=pt_tag or None)
        await self._emit({"type": "message", **msg.to_dict()})
        return text, "spoke"

    async def _emit(self, event: dict):
        if self.broadcast is not None:
            await self.broadcast(event)

    def stop(self):
        """Global stop: Interrupts all LLM replies currently being generated (concurrency waves may have more than one in transit) and ends the round."""
        self.stop_event.set()
        for t in list(self._gen_tasks):
            if not t.done():
                t.cancel()
