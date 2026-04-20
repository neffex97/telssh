<p align="center">
  <h1 align="center">💻 TelSSH</h1>
  <p align="center">
    A lightweight Telegram bot to manage multiple VPS servers over SSH — right from your phone.
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go&logoColor=white" alt="Go 1.21+">
    <img src="https://img.shields.io/badge/Docker-Ready-2496ED?logo=docker&logoColor=white" alt="Docker">
    <img src="https://img.shields.io/badge/Telegram-Bot_API_9.3-26A5E4?logo=telegram&logoColor=white" alt="Telegram">
    <img src="https://img.shields.io/badge/License-MIT-green" alt="License">
    <img src="https://img.shields.io/badge/RAM-~10MB-brightgreen" alt="Memory">
  </p>
</p>

---

## What is TelSSH?

TelSSH is a single-binary Telegram bot written in Go that lets you manage your VPS servers directly from Telegram. Connect to any server, run commands, edit files, upload/download — all through an intuitive chat interface with inline keyboards and real-time streaming output.

---

## Features

### Multi-Server Management

- Add, remove, and edit servers dynamically via chat — no config file edits needed
- Switch between servers with inline keyboard buttons
- Password and SSH key authentication
- Persistent connections with automatic keepalive (30s interval)

### Command Execution

- **Direct typing** — just type a shell command, no prefix needed
- **`/run`** — execute with a ✋ cancel button for long-running commands
- **`/live`** — stream output for `tail -f`, `htop`, `ping`, etc. (5min timeout)
- **Real-time streaming** via Telegram `sendMessageDraft` (Bot API 9.3) — output appears progressively
- **Timeout protection** — 2min for `/run`, 5min for `/live`
- Clean process termination: SIGINT → SIGKILL on cancel

### Remote File Management

- **`/edit`** — read a file, reply with new content to overwrite
- **`/write`** — write content to a file in one command
- **`/download`** — download files from VPS (up to 50MB)
- **`/upload`** — upload files from Telegram to VPS

### Quick Commands Dashboard

14 one-tap buttons for common admin tasks:

|                 |                  |                  |
| --------------- | ---------------- | ---------------- |
| 📊 System Info  | 💾 Disk Usage    | 🧠 Memory        |
| ⚙️ CPU / Top    | 📋 Processes     | 🌐 Network Ports |
| 🐳 Docker PS    | 📦 Docker Images | 📜 System Logs   |
| 🔧 Services     | 🔄 Updates       | 🌍 IP Address    |
| 📈 Load Average | 👥 Active Users  |                  |

### Security

- **User whitelist** — only authorized Telegram user IDs can interact
- **Credential auto-delete** — `/addserver` messages are automatically removed
- **Dangerous command confirmation** — `rm -rf`, `reboot`, `shutdown`, `mkfs` require confirmation
- **No cloud storage** — all config stays on your server

### Saved Commands & History

- `/save` — save frequently used commands as named snippets
- `/saved` — list and run saved commands with one tap
- `/history` — view and re-run recent commands (last 20)

---

## Quick Start

### 1. Create a Telegram Bot

1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Send `/newbot` and follow the prompts
3. Copy the bot token

### 2. Get Your Telegram User ID

Message [@userinfobot](https://t.me/userinfobot) to get your numeric user ID.

### 3. Configure

```bash
cp config.example.yaml config.yaml
chmod 600 config.yaml
```

Edit `config.yaml`:

```yaml
bot_token: "YOUR_BOT_TOKEN"
authorized_users:
  - YOUR_TELEGRAM_ID

servers:
  - name: "my-vps"
    host: "1.2.3.4"
    port: 22
    user: "root"
    password: "your_password"
    # key_path: "/root/.ssh/id_rsa"
```

### 4. Run with Docker (Recommended)

```bash
docker compose up -d --build
```

```bash
docker compose logs -f   # view logs
docker compose down      # stop
```

> Uses ~10-15MB RAM with resource limits (64MB cap, 0.25 CPU).

For SSH key auth, uncomment the key volume mount in `docker-compose.yml`.

### Alternative: Build from Source

```bash
go build -o telssh .
./telssh -config config.yaml
```

### Cross-Compile for Deployment

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o telssh .
scp telssh root@your-vps:~/telssh/
```

---

## Bot Commands

| Command                   | Description                                 |
| ------------------------- | ------------------------------------------- |
| `/start`                  | Welcome message & quick start guide         |
| `/help`                   | Show all commands with current status       |
| `/servers`                | List all configured servers                 |
| `/connect [name]`         | Connect to a server (picker if no name)     |
| `/disconnect`             | Disconnect from current server              |
| `/status`                 | Connection status with quick action buttons |
| `/run <cmd>`              | Execute with streaming + cancel button      |
| `/live <cmd>`             | Long-running stream (5min) with stop button |
| `/edit <path>`            | Read a file, reply to overwrite             |
| `/write <path> <content>` | Write content to a remote file              |
| `/download <path>`        | Download file from server                   |
| `/upload <path>`          | Upload file to server                       |
| `/quick`                  | Quick commands dashboard                    |
| `/save <name> <cmd>`      | Save a command snippet                      |
| `/saved`                  | List & run saved commands                   |
| `/delsave`                | Delete a saved command                      |
| `/history`                | View & re-run recent commands               |
| `/addserver`              | Add a new server                            |
| `/editserver`             | Edit an existing server                     |
| `/removeserver`           | Remove a server                             |
| `/cancel`                 | Cancel current operation                    |

> **Tip:** Once connected, just type any command directly — no `/run` prefix needed.

---

## Project Structure

```
├── main.go              # Entry point, signal handling
├── bot/
│   └── bot.go           # Telegram handlers, keyboards, streaming
├── config/
│   └── config.go        # YAML config with runtime save
├── sshmanager/
│   └── manager.go       # SSH connection pool, exec, file I/O
├── Dockerfile           # Multi-stage build (golang → alpine)
├── docker-compose.yml   # Production-ready with resource limits
├── entrypoint.sh        # Config seeding for Docker volumes
└── config.example.yaml  # Template configuration
```

## Live Update — Telegram Bot API 9.3

TelSSH uses the **`sendMessageDraft`** method introduced in [Telegram Bot API 9.3](https://core.telegram.org/bots/api#april-4-2025) to deliver **true real-time streaming** of command output directly into the chat.

### How It Works

1. When you run a command (`/run`, `/live`, or direct input), TelSSH executes it over SSH with PTY allocation.
2. As output arrives, it is pushed to Telegram progressively using `sendMessageDraft` — the message updates **live** in the chat, character by character.
3. Once the command completes, the draft is finalized into a regular message with `editMessageText`.

### Why `sendMessageDraft`?

| Traditional (`editMessageText`)        | Live Update (`sendMessageDraft`)          |
| -------------------------------------- | ----------------------------------------- |
| Batched edits every few seconds        | Continuous, real-time character streaming |
| Rate-limited to ~30 edits/min          | No rate limit on draft updates            |
| Output appears in chunks               | Output appears progressively as it runs   |
| Poor experience for long-running tasks | Feels like a live terminal session        |

### Supported Commands

- **`/run <cmd>`** — streaming output with a ✋ Cancel button (2min timeout)
- **`/live <cmd>`** — long-running stream for `tail -f`, `htop`, `ping`, etc. (5min timeout)
- **Direct typing** — any command typed directly streams output the same way

> **Fallback:** If `sendMessageDraft` is unavailable, TelSSH automatically falls back to periodic `editMessageText` updates.

---

## Design Decisions

- **Long polling** over webhooks — no public URL required
- **One SSH connection per user** with keepalive — fast sequential commands
- **PTY allocation** for every command — proper terminal emulation
- **Context-based cancellation** — no zombie goroutines
- **`sendMessageDraft` streaming** (Bot API 9.3) — real-time output, falls back to `editMessageText`

---

## Security Notes

- Config stores credentials — always `chmod 600 config.yaml`
- SSH host key verification is disabled for convenience — consider known_hosts for production
- Prefer SSH key authentication over passwords
- Bot only responds to whitelisted Telegram user IDs
- `/addserver` messages are auto-deleted to prevent credential exposure

---

## Requirements

- **Go 1.21+** (for building)
- **Docker** (recommended for deployment)
- **Telegram bot token** from [@BotFather](https://t.me/BotFather)
- **SSH access** to your VPS servers

## License

MIT
