# RG Discord Bridge

> **Part of the [ResonantGenesis](https://dev-swat.com) platform** — Multi-tenant Discord bot that routes messages to platform agents.

[![Status: Production](https://img.shields.io/badge/Status-Production-brightgreen.svg)]()
[![License: RG Source Available](https://img.shields.io/badge/License-RG%20Source%20Available-blue.svg)](LICENSE.txt)

Shared platform bot that routes Discord messages to different agents based on per-guild/channel config stored in the `discord_connections` DB table. Users invite the bot to their server, create a connection via the platform API, and the bot forwards messages through the webhook trigger system.

## Features

- **Multi-tenant** — One bot instance serves all guilds, routing to the correct agent per guild/channel
- **Slash commands** — `/rg chat`, `/rg agent`, `/rg status`
- **Text commands** — `!rg` prefix for text-based interaction
- **Connection caching** — Guild lookups cached with configurable TTL
- **Polling** — Async polling for agent responses with configurable intervals

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DISCORD_BOT_TOKEN` | Bot token from Discord Developer Portal |
| `DISCORD_AGENT_ID` | Fallback agent ID for DMs / unconfigured guilds |
| `DISCORD_OWNER_USER_ID` | Fallback platform user_id |
| `DISCORD_ALLOWED_CHANNELS` | Comma-separated allowed channel IDs |
| `AGENT_ENGINE_URL` | Internal URL for agent_engine_service |
| `DISCORD_COMMAND_PREFIX` | Text command prefix (default: `!rg`) |
| `POLL_INTERVAL_SECONDS` | Poll interval (default: 2) |
| `POLL_TIMEOUT_SECONDS` | Max wait for agent response (default: 60) |
| `CONNECTION_CACHE_TTL` | Cache TTL in seconds (default: 60) |

## Quick Start

```bash
pip install -r requirements.txt
export DISCORD_BOT_TOKEN="your-bot-token"
export AGENT_ENGINE_URL="http://localhost:8000"
python bot.py
```

## Deployment Status

- **Extracted from**: `genesis2026_production_backend/discord_bridge/`
- **Server path**: `/home/deploy/RG_Discord_Bridge`
- **Docker service**: `discord_bridge`

---
**Organization**: [DevSwat-ResonantGenesis](https://github.com/DevSwat-ResonantGenesis) | **Platform**: [dev-swat.com](https://dev-swat.com)
