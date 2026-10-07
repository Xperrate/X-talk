"""Local Ollama adapter (http://localhost:11434)."""
import httpx

from .base import BaseLLM, register


@register("ollama")
class Ollama(BaseLLM):
    type = "ollama"

    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self.model = cfg["model"]
        self.base_url = str(cfg.get("base_url", "http://localhost:11434")).rstrip("/")
        self.temperature = float(cfg.get("temperature", 0.7))
        self.timeout = float(cfg.get("timeout", 600))
        self.extra_body = dict(cfg.get("extra_body") or {})

    async def reply(self, history) -> str:
        payload = {
            "model": self.model,
            "messages": self.to_api_messages(history),
            "stream": False,
            "options": {"temperature": self.temperature},
        }
        payload.update(self.extra_body)  # Transparent Ollama specific argument (e.g. Qwen3 thought: {"think": false})
        # the endpoint is local/local area network Ollama, never take the system agent (VPN switch does not affect)
        async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
            r = await client.post(f"{self.base_url}/api/chat", json=payload)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
            data = r.json()
        return data["message"]["content"]

    supports_tools = True

    def get_tool_specs(self, host_note: str = "") -> list[dict]:
        from .base import shell_tool_spec

        return shell_tool_spec(host_note)

    async def chat_with_tools(self, messages: list[dict], tools: list[dict]) -> dict:
        # toolcall takes Ollama 's OpenAI compatible endpoint (/v1), and the messageformat is consistent with the standard protocol;
        # Normal speak still uses native /api/chat (keep think: false and other arguments).
        url = f"{self.base_url}/v1/chat/completions"
        payload = {"model": self.model, "messages": [dict(m) for m in messages]}
        if tools:
            payload["tools"] = tools
        # Same as above: directly connected to the local area network Ollama, without passing through any system agent
        async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
            r = await client.post(url, json=payload)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
            data = r.json()
        m = (data["choices"][0].get("message") or {})
        tcs = []
        for t in m.get("tool_calls") or []:
            fn = t.get("function", {})
            tcs.append({"id": t.get("id", ""), "name": fn.get("name", ""), "arguments": fn.get("arguments", "{}")})
        return {"content": m.get("content"), "tool_calls": tcs or None}
