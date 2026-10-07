# X-Talk Local Deployment Guide

This guide defines the standard local deployment method for X-Talk. The default deployment target is a normal user's own Windows PC. LAN and remote-agent setups are optional advanced modes.

## 1. Standard Local Deployment

The standard local deployment is:

- X-Talk server runs on the user's PC.
- Browser access is limited to `http://127.0.0.1:8321`.
- LLM endpoint runs locally, for example llama.cpp, LM Studio, vLLM, or Ollama.
- Command executor is local.
- No public internet exposure is required.

Default local URLs:

```text
X-Talk UI: http://127.0.0.1:8321
OpenAI-compatible model endpoint: http://127.0.0.1:8080/v1
```

## 2. Prerequisites

Required:

- Windows 10 or newer
- Python 3.10 or newer
- One OpenAI-compatible LLM endpoint, for example:
  - llama.cpp server
  - LM Studio
  - vLLM
  - Ollama with an OpenAI-compatible endpoint
- A model that supports tool calling if you want agents to execute commands

Optional:

- Ollama endpoint
- Remote machines running `pc_agent.py`
- LAN access for other computers on the same network

## 3. One-Time Setup

From the project folder, double-click:

```text
setup.bat
```

Or run:

```powershell
.\setup.bat
```

`setup.bat` does the following:

1. Checks that Python is available.
2. Creates `.venv`.
3. Installs dependencies from `requirements.txt`.
4. Creates `config.json` from `config.example.json` if `config.json` does not already exist.

If you do not want a virtual environment, you can install dependencies directly:

```powershell
python -m pip install -r requirements.txt
```

## 4. Configure Members

Edit:

```text
config.json
```

A minimal local configuration looks like this:

```json
{
  "group_name": "X-Talk",
  "max_llm_streak": 2,
  "max_tool_steps": 20,
  "members": [
    {
      "name": "Me",
      "type": "human",
      "role": "boss"
    },
    {
      "name": "Artemis",
      "type": "openai",
      "model": "your-model-name",
      "base_url": "http://127.0.0.1:8080/v1",
      "executor": {
        "type": "local"
      },
      "enabled": true,
      "role": "member",
      "timeout": 300
    }
  ]
}
```

Rules:

- Keep exactly one human member.
- The human member should use `role: "boss"`.
- At most one LLM member may use `role: "leader"`.
- Other LLM members should use `role: "member"`.
- `base_url` must point to an OpenAI-compatible endpoint ending in `/v1`, or to Ollama's base URL.
- Use `api_key_env` instead of putting API keys directly in `config.json`.
- Do not commit `config.json` if it contains private LAN addresses or secrets.

Common `base_url` examples:

```text
http://127.0.0.1:8080/v1
http://127.0.0.1:1234/v1
http://127.0.0.1:11434
```

## 5. Start X-Talk

Double-click:

```text
start.bat
```

`start.bat` uses `.venv\Scripts\python.exe` when it exists. If no virtual environment exists, it falls back to system Python.

Default local mode binds to:

```text
127.0.0.1:8321
```

The browser opens:

```text
http://127.0.0.1:8321
```

## 6. Stop X-Talk

Double-click:

```text
stop.bat
```

Or close the X-Talk service window.

## 7. Verify Local Deployment

Check X-Talk:

```powershell
Invoke-RestMethod http://127.0.0.1:8321/api/members
```

Force a presence probe:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8321/api/presence/probe
```

Check the model endpoint:

```powershell
Invoke-RestMethod http://127.0.0.1:8080/v1/models
```

Expected:

- `/api/members` returns `group_name: "X-Talk"`.
- `/api/presence/probe` returns `ok: true`.
- A reachable LLM member shows `presence_status: "online"`.
- The model endpoint returns a model list.

## 8. Troubleshooting

### Python not found

Install Python 3.10+ and make sure `Add Python to PATH` is checked.

### Port 8321 is already in use

Find the process:

```powershell
Get-NetTCPConnection -LocalPort 8321 -State Listen
```

Stop X-Talk:

```powershell
.\stop.bat
```

### Model endpoint is not reachable

Check the endpoint directly:

```powershell
Invoke-RestMethod http://127.0.0.1:8080/v1/models
```

If this fails, X-Talk cannot use that member.

### Member shows offline

Check:

- the model endpoint is running
- `base_url` is correct
- OpenAI-compatible URLs end in `/v1`
- the model name matches the endpoint
- the endpoint is not blocked by firewall or proxy

### Tool execution fails

Check the member executor:

```json
{
  "executor": {
    "type": "local"
  }
}
```

For local mode, the executor must be `local`.

## 9. Optional LAN Mode

Use LAN mode only when other computers on the same trusted network need to open X-Talk.

Start manually:

```powershell
$env:HOST="0.0.0.0"
$env:PORT="8321"
.\.venv\Scripts\python.exe server.py
```

Open from another computer:

```text
http://YOUR_PC_IP:8321
```

Open the firewall port only on a trusted network:

```powershell
New-NetFirewallRule -DisplayName "X-Talk" -Direction Inbound -Protocol TCP -LocalPort 8321 -Action Allow
```

Do not expose port `8321` to the public internet.

## 10. Optional Remote Agent

Use this only when an agent should execute commands on another computer.

On the remote computer, run:

```powershell
python pc_agent.py
```

Default listener:

```text
0.0.0.0:8931
```

Open the firewall port on the remote computer:

```powershell
New-NetFirewallRule -DisplayName "X-Talk PC Agent" -Direction Inbound -Protocol TCP -LocalPort 8931 -Action Allow
```

Then configure the member:

```json
{
  "name": "Demeter",
  "type": "openai",
  "model": "your-model-name",
  "base_url": "http://REMOTE_IP:8080/v1",
  "executor": {
    "type": "remote",
    "url": "http://REMOTE_IP:8931"
  },
  "enabled": true,
  "role": "member"
}
```

Verify:

```powershell
Invoke-RestMethod http://REMOTE_IP:8931/status
Invoke-RestMethod http://REMOTE_IP:8080/v1/models
```

## 11. Presence Rules

X-Talk uses active presence probing:

- Default probe interval: 20 seconds
- Default probe timeout: 5 seconds
- Offline threshold: 2 consecutive failures
- Recovery: one successful probe or one successful chat call

Environment variables:

```powershell
$env:PRESENCE_PROBE_INTERVAL="20"
$env:PRESENCE_PROBE_TIMEOUT="5"
$env:PRESENCE_FAIL_THRESHOLD="2"
```

Manual probe:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8321/api/presence/probe
```

## 11b. Turn Timeout

Each member gets a limited amount of time to use the speaking right. If they do not reply in time, the turn is forfeited.

Default: 90 seconds.

```powershell
$env:TURN_TIMEOUT="90"
```

## 11c. Context Window

Full chat history is stored on disk, but each LLM turn only receives a recent visible window to avoid overflowing the model context.

Default: 80 messages. Set `0` to disable trimming.

```powershell
$env:MAX_CONTEXT_MESSAGES="80"
```

## 12. Data and Backup

Runtime data is stored in:

```text
data/
```

Important files:

```text
data/sessions.json
data/tasks.json
data/last_active.txt
data/*.jsonl
data/uploads/
```

Back up:

- `config.json`
- `data/`

Do not commit `data/` to Git.

## 13. Security Rules

Required:

- Do not expose port `8321` directly to the public internet.
- Do not expose port `8931` directly to the public internet.
- Use `HOST=127.0.0.1` unless LAN access is required.
- Restrict firewall rules to known LAN IPs when possible.
- Use `api_key_env` for API keys.
- Keep `config.json` out of public repositories if it contains private endpoints or secrets.

Recommended `.gitignore` entries:

```gitignore
__pycache__/
*.pyc
.venv/
venv/
env/
config.json
.env
*.key
*.db
*.log
data/
uploads/
models/
*.gguf
*.safetensors
```

## 14. Release Package Checklist

Before publishing a release, confirm:

- `README.md` exists.
- `LICENSE` exists.
- `.gitignore` exists.
- `requirements.txt` exists.
- `config.example.json` exists.
- `setup.bat`, `start.bat`, and `stop.bat` work from the project root.
- `config.json` is ignored by Git.
- `data/` is ignored by Git.
- `python -m py_compile server.py core/conversation.py llms/*.py` passes.

## 15. Rollback

To roll back:

1. Stop X-Talk.
2. Restore the previous release tag.
3. Restore `config.json` and `data/` from backup.
4. Start X-Talk.
5. Run the verification commands in section 7.

## License

This project is licensed under the MIT License. See `LICENSE` for details.

Author: Xperrate  
Date: 2026-10
