"""
Discord Bridge for ResonantGenesis Agent Engine (Multi-Tenant)
==============================================================

Shared platform bot that routes messages to different agents based on
per-guild/channel config stored in the discord_connections DB table.

Users invite this bot to their server, then create a connection via the
platform API. The bot looks up the correct agent for each guild on every
message and forwards it through the webhook trigger system.

Environment variables:
  DISCORD_BOT_TOKEN      - Bot token from Discord Developer Portal
  DISCORD_AGENT_ID       - Fallback agent ID for DMs / unconfigured guilds
  AGENT_ENGINE_URL       - Internal URL for agent_engine_service
  DISCORD_OWNER_USER_ID  - Fallback platform user_id (for x-user-id header)
  DISCORD_COMMAND_PREFIX  - Prefix for text commands (default: !rg)
  POLL_INTERVAL_SECONDS  - Poll interval (default: 2)
  POLL_TIMEOUT_SECONDS   - Max wait for agent response (default: 60)
  CONNECTION_CACHE_TTL   - Seconds to cache guild lookups (default: 60)
"""

import os
import sys
import asyncio
import logging
import json
import time
from datetime import datetime, timezone
from typing import Optional, Dict

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

# ------------------------------------
# Configuration
# ------------------------------------

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
DISCORD_AGENT_ID = os.getenv("DISCORD_AGENT_ID", "")
AGENT_ENGINE_URL = os.getenv("AGENT_ENGINE_URL", "http://agent_engine_service:8000")
DISCORD_OWNER_USER_ID = os.getenv("DISCORD_OWNER_USER_ID", "")
COMMAND_PREFIX = os.getenv("DISCORD_COMMAND_PREFIX", "!rg")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "2"))
POLL_TIMEOUT = int(os.getenv("POLL_TIMEOUT_SECONDS", "60"))
CONNECTION_CACHE_TTL = int(os.getenv("CONNECTION_CACHE_TTL", "60"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("discord_bridge")

# Connection cache: guild_id -> {data, timestamp}
_connection_cache: Dict[str, dict] = {}


# ------------------------------------
# Bot Setup
# ------------------------------------

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.dm_messages = True

bot = commands.Bot(command_prefix=COMMAND_PREFIX + " ", intents=intents)
http_session: aiohttp.ClientSession = None


# ------------------------------------
# Guild Connection Lookup (Multi-Tenant)
# ------------------------------------

async def lookup_connection(guild_id: str, channel_id: str = None) -> Optional[dict]:
    """
    Look up which agent handles this guild/channel.
    Uses a short TTL cache to avoid hitting the DB on every message.
    Returns dict with agent_id, user_id, connection_id, respond_to_* flags, or None.
    """
    cache_key = f"{guild_id}:{channel_id or 'all'}"
    cached = _connection_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < CONNECTION_CACHE_TTL:
        return cached["data"]

    url = f"{AGENT_ENGINE_URL}/discord/lookup/{guild_id}"
    params = {}
    if channel_id:
        params["channel_id"] = channel_id

    try:
        async with http_session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status == 200:
                data = await resp.json()
                _connection_cache[cache_key] = {"data": data, "ts": time.time()}
                return data
            elif resp.status == 404:
                _connection_cache[cache_key] = {"data": None, "ts": time.time()}
                return None
            else:
                logger.warning(f"Guild lookup returned {resp.status} for {guild_id}")
                return None
    except Exception as e:
        logger.warning(f"Guild lookup failed for {guild_id}: {e}")
        return None


async def record_message(connection_id: str):
    """Record that a message was processed for stats."""
    try:
        url = f"{AGENT_ENGINE_URL}/discord/lookup/{connection_id}/message-sent"
        async with http_session.post(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
            pass
    except Exception:
        pass


# ------------------------------------
# Agent Engine Communication
# ------------------------------------

async def trigger_agent(agent_id: str, user_id: str, message_text: str, discord_user: str, channel_name: str) -> dict:
    """
    Trigger an agent via the internal agent_engine_service webhook endpoint.
    Returns {"status": "triggered"|"received", "session_id": "..."} or error dict.
    """
    url = f"{AGENT_ENGINE_URL}/webhooks/agent/{agent_id}/trigger"

    payload = {
        "event": "discord_message",
        "data": {
            "message": message_text,
            "discord_user": discord_user,
            "channel": channel_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }

    headers = {
        "Content-Type": "application/json",
        "x-internal-service": "discord_bridge",
    }
    # Add platform user identity so agent has permissions
    if user_id:
        headers["x-user-id"] = user_id
    elif DISCORD_OWNER_USER_ID:
        headers["x-user-id"] = DISCORD_OWNER_USER_ID

    try:
        async with http_session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            data = await resp.json()
            logger.info(f"Agent trigger response ({resp.status}): {data}")
            return data
    except Exception as e:
        logger.error(f"Failed to trigger agent {agent_id}: {e}")
        return {"status": "error", "message": str(e)}


async def poll_session_result(session_id: str) -> str:
    """
    Poll agent session until it completes or times out.
    Returns the agent's final output text.
    """
    url = f"{AGENT_ENGINE_URL}/sessions/{session_id}"
    elapsed = 0

    while elapsed < POLL_TIMEOUT:
        try:
            async with http_session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status = data.get("status", "")
                    logger.info(f"Session {session_id} status: {status}")

                    if status in ("completed", "finished", "done", "success"):
                        output = data.get("final_output") or data.get("result") or ""
                        if output:
                            return output
                        # Try getting last step output
                        return await _get_last_step_output(session_id) or "Agent completed but produced no output."

                    if status in ("failed", "error", "cancelled"):
                        error = data.get("error_message") or data.get("error") or "Unknown error"
                        return f"Agent encountered an error: {error}"
                else:
                    logger.warning(f"Session poll returned {resp.status}")
        except Exception as e:
            logger.warning(f"Poll error: {e}")

        await asyncio.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

    return "Agent is still processing. The response is taking longer than expected — check back on the platform."


async def _get_last_step_output(session_id: str) -> str:
    """Try to get the last step's output from the session."""
    url = f"{AGENT_ENGINE_URL}/sessions/{session_id}/steps"
    try:
        async with http_session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                steps = await resp.json()
                if steps:
                    last = steps[-1] if isinstance(steps, list) else None
                    if last:
                        return last.get("output") or last.get("result") or last.get("action_result") or ""
    except Exception as e:
        logger.warning(f"Failed to get steps for session {session_id}: {e}")
    return ""


# ------------------------------------
# Message Processing
# ------------------------------------

async def process_message(
    message_text: str,
    discord_user: str,
    channel_name: str,
    reply_func,
    guild_id: str = None,
    channel_id: str = None,
):
    """
    Core message processing (multi-tenant):
    1. Look up agent for this guild/channel
    2. Trigger agent
    3. Poll for response
    4. Reply in Discord + record stats
    """
    agent_id = None
    user_id = None
    connection_id = None

    # Multi-tenant: look up agent for this guild
    if guild_id:
        conn = await lookup_connection(guild_id, channel_id)
        if conn:
            agent_id = conn.get("agent_id")
            user_id = conn.get("user_id")
            connection_id = conn.get("connection_id")

    # Fallback to env var for DMs or unconfigured guilds
    if not agent_id:
        agent_id = DISCORD_AGENT_ID
        user_id = DISCORD_OWNER_USER_ID

    if not agent_id:
        await reply_func(
            "No agent is configured for this server. "
            "Ask the server admin to set up a connection at https://resonantgenesis.xyz"
        )
        return

    # Trigger the agent
    result = await trigger_agent(agent_id, user_id, message_text, discord_user, channel_name)

    if result.get("status") == "error":
        await reply_func(f"Failed to reach agent: {result.get('message', 'unknown error')}")
        return

    session_id = result.get("session_id")
    if not session_id:
        msg = result.get("message", "Agent received the message but couldn't start a session.")
        await reply_func(msg)
        return

    # Poll for the result
    agent_response = await poll_session_result(session_id)

    # Discord has a 2000 char limit per message
    if len(agent_response) <= 2000:
        await reply_func(agent_response)
    else:
        chunks = [agent_response[i:i+1990] for i in range(0, len(agent_response), 1990)]
        for i, chunk in enumerate(chunks):
            await reply_func(chunk)
            if i < len(chunks) - 1:
                await asyncio.sleep(0.5)

    # Record stats for multi-tenant tracking
    if connection_id:
        asyncio.create_task(record_message(connection_id))


# ------------------------------------
# Bot Events
# ------------------------------------

@bot.event
async def on_ready():
    global http_session
    http_session = aiohttp.ClientSession()
    logger.info(f"Discord Bridge online as {bot.user} (ID: {bot.user.id})")
    logger.info(f"Fallback Agent ID: {DISCORD_AGENT_ID or 'None'}")
    logger.info(f"Agent Engine URL: {AGENT_ENGINE_URL}")
    logger.info(f"Mode: Multi-tenant (guild->agent lookup from DB)")
    logger.info(f"Guilds connected: {len(bot.guilds)}")

    # Sync slash commands
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} slash command(s)")
    except Exception as e:
        logger.error(f"Failed to sync slash commands: {e}")


@bot.event
async def on_message(message: discord.Message):
    """Handle incoming messages — respond to @mentions and DMs."""
    # Ignore own messages
    if message.author == bot.user:
        return

    # Check if this is a DM or a mention
    is_dm = isinstance(message.channel, discord.DMChannel)
    is_mention = bot.user in message.mentions
    guild_id = str(message.guild.id) if message.guild else None
    channel_id = str(message.channel.id) if not is_dm else None

    # For guild messages, check if respond_to_all is enabled
    should_respond = is_dm or is_mention
    if not should_respond and guild_id:
        conn = await lookup_connection(guild_id, channel_id)
        if conn and conn.get("respond_to_all"):
            should_respond = True

    if not should_respond:
        await bot.process_commands(message)
        return

    # Strip the bot mention from the message
    content = message.content
    if is_mention:
        content = content.replace(f"<@{bot.user.id}>", "").replace(f"<@!{bot.user.id}>", "").strip()

    if not content:
        await message.reply("Send me a message and I'll forward it to the agent!")
        return

    discord_user = f"{message.author.name}#{message.author.discriminator}" if message.author.discriminator != "0" else message.author.name
    channel_name = "DM" if is_dm else f"#{message.channel.name}"

    logger.info(f"Processing message from {discord_user} in {channel_name}: {content[:100]}...")

    async with message.channel.typing():
        await process_message(
            message_text=content,
            discord_user=discord_user,
            channel_name=channel_name,
            reply_func=message.reply,
            guild_id=guild_id,
            channel_id=channel_id,
        )

    await bot.process_commands(message)


# ------------------------------------
# Slash Commands
# ------------------------------------

@bot.tree.command(name="ask", description="Ask the ResonantGenesis agent a question")
@app_commands.describe(question="Your question or request for the agent")
async def slash_ask(interaction: discord.Interaction, question: str):
    """Slash command: /ask <question>"""
    await interaction.response.defer(thinking=True)

    discord_user = interaction.user.name
    channel_name = f"#{interaction.channel.name}" if interaction.guild else "DM"
    guild_id = str(interaction.guild.id) if interaction.guild else None
    channel_id = str(interaction.channel.id) if interaction.guild else None

    logger.info(f"/ask from {discord_user}: {question[:100]}...")

    async def reply(text: str):
        try:
            await interaction.followup.send(text)
        except discord.HTTPException as e:
            logger.error(f"Failed to send followup: {e}")

    await process_message(
        message_text=question,
        discord_user=discord_user,
        channel_name=channel_name,
        reply_func=reply,
        guild_id=guild_id,
        channel_id=channel_id,
    )


@bot.tree.command(name="connect", description="Connect a ResonantGenesis agent to this server")
@app_commands.describe(agent_id="Your agent ID from the ResonantGenesis platform dashboard")
async def slash_connect(interaction: discord.Interaction, agent_id: str):
    """Slash command: /connect <agent_id> — link an agent to this Discord server."""
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server, not in DMs.", ephemeral=True)
        return

    # Only server admins can connect agents
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "You need **Manage Server** permission to connect an agent.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    guild_id = str(interaction.guild.id)
    guild_name = interaction.guild.name

    # Call the internal connect endpoint
    url = f"{AGENT_ENGINE_URL}/discord/internal/connect"
    payload = {
        "agent_id": agent_id.strip(),
        "guild_id": guild_id,
        "guild_name": guild_name,
        "discord_user_id": str(interaction.user.id),
        "discord_user_name": interaction.user.name,
    }
    headers = {
        "Content-Type": "application/json",
        "x-internal-service": "discord_bridge",
    }

    try:
        async with http_session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()

            if resp.status == 200:
                agent_name = data.get("agent_name", "Unknown")
                # Invalidate cache for this guild
                for key in list(_connection_cache.keys()):
                    if key.startswith(f"{guild_id}:"):
                        del _connection_cache[key]

                await interaction.followup.send(
                    f"**Agent Connected!**\n"
                    f"Agent: **{agent_name}** (`{agent_id}`)\n"
                    f"Server: **{guild_name}**\n\n"
                    f"The agent will now respond to @mentions in this server.\n"
                    f"Try it: mention <@{bot.user.id}> with a message!",
                    ephemeral=False,  # Show to everyone so they know the bot is active
                )
            elif resp.status == 404:
                await interaction.followup.send(
                    f"Agent `{agent_id}` not found.\n\n"
                    f"**How to find your agent ID:**\n"
                    f"1. Go to https://resonantgenesis.xyz\n"
                    f"2. Open **Agents** in the sidebar\n"
                    f"3. Click on your agent\n"
                    f"4. Copy the **Agent ID** from the details panel",
                    ephemeral=True,
                )
            elif resp.status == 409:
                detail = data.get("detail", "A connection already exists.")
                await interaction.followup.send(
                    f"**Already connected!** {detail}\n"
                    f"To switch agents, run `/disconnect` first, then `/connect` again.",
                    ephemeral=True,
                )
            else:
                detail = data.get("detail", "Unknown error")
                await interaction.followup.send(f"Failed to connect: {detail}", ephemeral=True)
    except Exception as e:
        logger.error(f"/connect error: {e}")
        await interaction.followup.send(f"Connection failed: {e}", ephemeral=True)


@bot.tree.command(name="disconnect", description="Remove the agent connection from this server")
async def slash_disconnect(interaction: discord.Interaction):
    """Slash command: /disconnect — remove the agent from this Discord server."""
    if not interaction.guild:
        await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
        return

    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "You need **Manage Server** permission to disconnect an agent.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    guild_id = str(interaction.guild.id)
    url = f"{AGENT_ENGINE_URL}/discord/internal/disconnect"
    headers = {
        "Content-Type": "application/json",
        "x-internal-service": "discord_bridge",
    }

    try:
        async with http_session.post(url, json={"guild_id": guild_id}, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()

            if resp.status == 200:
                # Invalidate cache
                for key in list(_connection_cache.keys()):
                    if key.startswith(f"{guild_id}:"):
                        del _connection_cache[key]

                await interaction.followup.send(
                    f"**Agent Disconnected**\n"
                    f"Removed {data.get('removed_count', 1)} connection(s) from this server.\n"
                    f"To reconnect, use `/connect <agent_id>`.",
                    ephemeral=False,
                )
            elif resp.status == 404:
                await interaction.followup.send("No active agent connection found for this server.", ephemeral=True)
            else:
                detail = data.get("detail", "Unknown error")
                await interaction.followup.send(f"Failed to disconnect: {detail}", ephemeral=True)
    except Exception as e:
        logger.error(f"/disconnect error: {e}")
        await interaction.followup.send(f"Disconnect failed: {e}", ephemeral=True)


@bot.tree.command(name="agent", description="Show the connected ResonantGenesis agent for this server")
async def slash_agent(interaction: discord.Interaction):
    """Slash command: /agent — show which agent is connected to this server."""
    guild_id = str(interaction.guild.id) if interaction.guild else None
    channel_id = str(interaction.channel.id) if interaction.guild else None

    if not guild_id:
        agent_id = DISCORD_AGENT_ID or "Not configured"
        await interaction.response.send_message(
            f"**DM Mode**\nFallback Agent: `{agent_id}`",
            ephemeral=True,
        )
        return

    conn = await lookup_connection(guild_id, channel_id)
    if conn:
        agent_id = conn.get("agent_id", "Unknown")
        await interaction.response.send_message(
            f"**Connected Agent**\n"
            f"Agent ID: `{agent_id}`\n"
            f"Responds to: mentions={conn.get('respond_to_mentions')}, all={conn.get('respond_to_all')}\n\n"
            f"Use `/disconnect` to remove, or `/connect <new_agent_id>` to switch.",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message(
            "**No agent connected to this server.**\n\n"
            "**Quick Setup:**\n"
            "1. Get your agent ID from https://resonantgenesis.xyz (Agents page)\n"
            "2. Run `/connect <agent_id>` here\n"
            "3. Done! Mention the bot to chat with your agent.",
            ephemeral=True,
        )


# ------------------------------------
# Guild Join / Leave Events
# ------------------------------------

@bot.event
async def on_guild_join(guild: discord.Guild):
    """Welcome message when the bot is added to a new server."""
    logger.info(f"Joined guild: {guild.name} (ID: {guild.id}), members: {guild.member_count}")

    # Find a suitable channel to post the welcome message
    target_channel = guild.system_channel
    if not target_channel:
        # Fall back to first text channel the bot can write to
        for channel in guild.text_channels:
            if channel.permissions_for(guild.me).send_messages:
                target_channel = channel
                break

    if target_channel:
        try:
            await target_channel.send(
                "**ResonantGenesis Agent Bridge**\n\n"
                "Thanks for adding me! I connect AI agents from the ResonantGenesis platform to your Discord server.\n\n"
                "**Quick Setup:**\n"
                "1. Go to https://resonantgenesis.xyz and copy your **Agent ID** from the Agents page\n"
                "2. Run `/connect <agent_id>` in any channel\n"
                "3. Mention me to chat with your agent!\n\n"
                "**Commands:**\n"
                "- `/connect <agent_id>` — Connect an agent to this server\n"
                "- `/disconnect` — Remove the agent\n"
                "- `/agent` — Show current connection status\n"
                "- `/ask <question>` — Ask the agent a question\n\n"
                "*Server admins with Manage Server permission can run `/connect` and `/disconnect`.*"
            )
        except Exception as e:
            logger.warning(f"Could not send welcome message to {guild.name}: {e}")


@bot.event
async def on_guild_remove(guild: discord.Guild):
    """Clean up when the bot is removed from a server."""
    logger.info(f"Removed from guild: {guild.name} (ID: {guild.id})")

    # Invalidate cache
    guild_id = str(guild.id)
    for key in list(_connection_cache.keys()):
        if key.startswith(f"{guild_id}:"):
            del _connection_cache[key]

    # Auto-disconnect via internal API
    if http_session and not http_session.closed:
        try:
            url = f"{AGENT_ENGINE_URL}/discord/internal/disconnect"
            headers = {"Content-Type": "application/json", "x-internal-service": "discord_bridge"}
            async with http_session.post(url, json={"guild_id": guild_id}, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                logger.info(f"Auto-disconnected guild {guild_id}: {resp.status}")
        except Exception as e:
            logger.warning(f"Failed to auto-disconnect guild {guild_id}: {e}")


# ------------------------------------
# Prefix Command (fallback)
# ------------------------------------

@bot.command(name="ask")
async def prefix_ask(ctx: commands.Context, *, question: str):
    """Prefix command: !rg ask <question>"""
    discord_user = ctx.author.name
    channel_name = f"#{ctx.channel.name}" if ctx.guild else "DM"
    guild_id = str(ctx.guild.id) if ctx.guild else None
    channel_id = str(ctx.channel.id) if ctx.guild else None

    logger.info(f"!rg ask from {discord_user}: {question[:100]}...")

    async with ctx.typing():
        await process_message(
            message_text=question,
            discord_user=discord_user,
            channel_name=channel_name,
            reply_func=ctx.reply,
            guild_id=guild_id,
            channel_id=channel_id,
        )


# ------------------------------------
# Graceful Shutdown
# ------------------------------------

@bot.event
async def on_close():
    if http_session and not http_session.closed:
        await http_session.close()


# ------------------------------------
# Entry Point
# ------------------------------------

def main():
    if not DISCORD_BOT_TOKEN:
        logger.error("DISCORD_BOT_TOKEN is not set! Cannot start bot.")
        sys.exit(1)

    if not DISCORD_AGENT_ID:
        logger.info("No DISCORD_AGENT_ID fallback set — all routing from DB connections.")

    logger.info("Starting Discord Bridge...")
    bot.run(DISCORD_BOT_TOKEN, log_handler=None)


if __name__ == "__main__":
    main()
