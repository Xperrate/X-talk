"""Any OpenAI is compatible with the/chat/completions endpoint (OpenAI, each classgateway, local vLLM/LM Studio, etc.)."""
import os

import httpx

from .base import BaseLLM, register


@register("openai")
class OpenAICompat(BaseLLM):
    type = "openai"

    def __init__(self, cfg: dict):
        super().__init__(cfg)
        self.model = cfg["model"]
        self.base_url = str(cfg.get("base_url", "https://api.openai.com/v1")).rstrip("/")
        key_env = cfg.get("api_key_env")
        if key_env:
            api_key = os.environ.get(key_env, "")
        else:
            api_key = cfg.get("api_key", "")
        self.api_key = api_key
        self.temperature = float(cfg.get("temperature", 0.7))
        self.timeout = float(cfg.get("timeout", 120))
        self.extra_body = dict(cfg.get("extra_body") or {})

    async def reply(self, history) -> str:
        headers = {}
        if self.api_key:  # localservice (llama.cpp/LM Studio, etc.) without key
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload = {
            "model": self.model,
            "messages": self.to_api_messages(history),
            "temperature": self.temperature,
        }
        payload.update(self.extra_body)  # Transparent service provider-specific argument (e.g., chat_template_kwargs for llama.cpp)
        async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:  # the endpoints are local/local area networks, never take the system agent (VPN switch does not affect)
            r = await client.post(f"{self.base_url}/chat/completions", json=payload, headers=headers)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
            data = r.json()
        return data["choices"][0]["message"]["content"]

    supports_tools = True

    def get_tool_specs(self, host_note: str = "") -> list[dict]:
        from .base import shell_tool_spec

        return shell_tool_spec(host_note)

    async def chat_with_tools(self, messages: list[dict], tools: list[dict]) -> dict:
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload = {"model": self.model, "messages": [dict(m) for m in messages]}
        if tools:
            payload["tools"] = tools
        payload.update(self.extra_body)
        # endpoint is a native/LAN service, never take the system agent (VPN switch does not affect)
        async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
            r = await client.post(f"{self.base_url}/chat/completions", json=payload, headers=headers)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
            data = r.json()
        m = (data["choices"][0].get("message") or {})
        tcs = []
        for t in m.get("tool_calls") or []:
            fn = t.get("function", {})
            tcs.append({"id": t.get("id", ""), "name": fn.get("name", ""), "arguments": fn.get("arguments", "{}")})
        return {"content": m.get("content"), "tool_calls": tcs or None}
