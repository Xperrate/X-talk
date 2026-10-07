"""LLM adapter base interface - the only extension point for adding a new LLM. Conventions: - an adapter implements BaseLLM.reply() and is registered with @register("type_name"); - referenced in config.json as {"name": ..., "type": "type_name", ...}; - zero changes needed on the server and frontend to enable the new LLM."""
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.conversation import Msg


_REGISTRY: dict[str, type["BaseLLM"]] = {}


def register(type_name: str):
    """classdecorator: registration adapter by name for reference in the `type` field of config.json."""

    def deco(cls):
        _REGISTRY[type_name] = cls
        return cls

    return deco


class BaseLLM(ABC):
    type: str = "base"
    system_prompt: str = (
        "You are an X-Talk member; there are humans and other AI assistants in the group, and you can see all chat records."
        "Please reply in English, as natural, short, and targeted as chatting with a friend; do not introduce yourself."
    )

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.name: str = cfg.get("name") or self.type
        self.system_prompt = cfg.get("system_prompt", self.system_prompt)
        # the system injected during the run is supplemented with text (such as concurrencyspeak etiquette), which is set by the turn drive; it is not set by the person and is not written back to config.json
        self.extra_system: str = ""

    # ---- Optional toolcall capability (config members of the executor take this path) ----
    supports_tools: bool = False

    def get_tool_specs(self, host_note: str = "") -> list[dict]:
        """return OpenAI function-calling schema; does not support implementationreturn [] of tool."""
        return []

    async def chat_with_tools(self, messages: list[dict], tools: list[dict]) -> dict:
        """OpenAI style single round dialogue (with tools). Input messages: [{role,content,...}] with system/user/assistant/tool; the assistant's tool_calls element is {id, type, function: {name, arguments (str) }}. return {"content": str | None, "tool_calls": [{id,name,str arguments}...] | None}."""
        raise NotImplementedError(f"{type(self).__name__} does not support tool calls")

    def to_api_messages(self, history: list["Msg"]) -> list[dict]:
        """Convert the GroupChathistory to OpenAI -style messages; the map of what you said is assistant."""
        sys_c = self.system_prompt + ("\n\n" + self.extra_system if self.extra_system else "")
        msgs = [{"role": "system", "content": sys_c}]
        for m in history:
            role = "assistant" if m.sender == self.name else "user"
            vc = getattr(m, "_llm_content", None)   # Feature # 10c (vision): With graphmessage pre-encoding from Conversation to multimodal content (text + image data uri); otherwise, pure text remains unchanged
            msgs.append({"role": role, "content": vc if vc is not None else m.content})
        return msgs

    @abstractmethod
    async def reply(self, history: list["Msg"]) -> str:
        """Generate the next sentence speak according to the full GroupChathistory (list [Msg], in chronological order)."""


def shell_tool_spec(host_note: str = "") -> list[dict]:
    """Standard OpenAI function-calling schema: execute a command on the host server."""
    desc = "Execute a command on your host and return its output (exit code + time consuming + text)."
    if host_note:
        desc += f" host environment: {host_note}. Write native commands for that environment."
    return [{
        "type": "function",
        "function": {
            "name": "shell",
            "description": desc,
            "parameters": {"type": "object", "properties": {"cmd": {"type": "string"}}, "required": ["cmd"]},
        },
    }]


def build(cfg: dict) -> BaseLLM:
    t = cfg.get("type")
    cls = _REGISTRY.get(t)
    if cls is None:
        raise ValueError(f"unknown llm type: {t!r}; currently available: {sorted(_REGISTRY)}")
    return cls(cfg)


def available_types() -> list[str]:
    return sorted(_REGISTRY)
