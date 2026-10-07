"""The commandexecute of the LLM members. - LocalExec: the local (this machine of runGroupChatservice) direct driver process —— for members resident in local; - RemoteExec: HTTP Forward to another machine to deploy the pc_agent.py guard process —— for remotemembers. The executor of each LLM member only points to the computer where it is located, natural isolation."""
import asyncio
import platform
import time


class LocalExec:
    def describe(self) -> str:
        return f"local subprocess ({platform.node()})"

    async def run(self, cmd: str, timeout: float = 120) -> str:
        if platform.system() == "Windows":
            argv = ["powershell", "-NoProfile", "-Command", cmd]
        else:
            argv = ["/bin/sh", "-c", cmd]
        t0 = time.time()
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            out_b, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            rc = proc.returncode
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            return f"[TIMEOUT] command exceeded {int(timeout)}s without finishing and was force-terminated"
        text = out_b.decode("utf-8", "replace")[:6000]
        return f"exit_code={rc} elapsed_ms={int((time.time() - t0) * 1000)}\n{text}"


class RemoteExec:
    def __init__(self, url: str):
        self.url = url.rstrip("/")

    def describe(self) -> str:
        return f"remote pc_agent ({self.url})"

    async def run(self, cmd: str, timeout: float = 120) -> str:
        import httpx

        try:
            # pc_agent On LAN machines, connect directly and never leave the system agent (VPN switch does not affect)
            async with httpx.AsyncClient(timeout=timeout + 5, trust_env=False) as client:
                r = await client.post(
                    f"{self.url}/exec", json={"cmd": str(cmd)[:4000], "timeout": int(min(max(timeout, 1), 300))})
                if r.status_code != 200:
                    raise RuntimeError(f"pc_agent returned HTTP {r.status_code}: {r.text[:200]}")
                d = r.json()
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(
                f"cannot connect to remote execution service ({self.url}): make sure pc_agent.py is running on that machine and the firewall allows the port. details: {e}") from None
        return (f"exit_code={d.get('exit_code')} elapsed_ms={int(d.get('elapsed_ms', 0))}\n"
                f"{str(d.get('output', ''))[:6000]}")


def make_executor(spec):
    # member ['executor'] in config.json: {"type":"local"}/{"type":"remote","url":...}/"http://..."
    if not spec:
        return None
    t = spec.get("type", "") if isinstance(spec, dict) else str(spec)
    if t == "local" or (not t and not isinstance(spec, dict)):
        return LocalExec()
    if t in ("remote",):
        url = (spec or {}).get("url")
        if not url:
            raise ValueError('executor type = remote requires url')
        return RemoteExec(url)
    if isinstance(spec, str):
        return RemoteExec(t)
    raise ValueError(f"unknown executor config: {spec!r}")
