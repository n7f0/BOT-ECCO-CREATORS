import discord
from discord import app_commands
from discord.ext import commands, tasks
from discord.ui import Button, View, Modal, TextInput, Select
import asyncio
from datetime import datetime, timezone
import json
import os
import sys
import aiohttp
import re
import cloudscraper
from fake_useragent import UserAgent
import asyncpg

# ========= CONFIGURAÇÕES =========
TOKEN = os.getenv("DISCORD_TOKEN_LIVE")
if not TOKEN:
    print("ERRO: Token do Discord não encontrado (DISCORD_TOKEN_LIVE).")
    sys.exit(1)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERRO: Variável DATABASE_URL não encontrada (banco PostgreSQL no Railway).")
    sys.exit(1)

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# ========= CONEXÃO COM BANCO DE DADOS =========
db_pool = None

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with db_pool.acquire() as conn:
        # Tabela de configuração geral (agora com painel_channel_id)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_config (
                guild_id TEXT PRIMARY KEY,
                target_guild_id BIGINT,
                channel_ids JSONB,
                role_id BIGINT,
                platforms JSONB,
                painel_channel_id BIGINT
            )
        """)
        # Tabela de streamers (com created_at)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_streamers (
                guild_id TEXT,
                user_id TEXT,
                nome TEXT,
                twitch TEXT,
                youtube TEXT,
                kick TEXT,
                tiktok TEXT,
                observacao TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (guild_id, user_id)
            )
        """)
        # Garantir coluna created_at (para upgrades)
        await conn.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='live_streamers' AND column_name='created_at') THEN
                    ALTER TABLE live_streamers ADD COLUMN created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='live_config' AND column_name='painel_channel_id') THEN
                    ALTER TABLE live_config ADD COLUMN painel_channel_id BIGINT;
                END IF;
            END $$;
        """)
        # Demais tabelas (mantidas)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_last_notified (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_status (
                guild_id TEXT,
                user_id TEXT,
                platform TEXT,
                is_live BOOLEAN,
                PRIMARY KEY (guild_id, user_id, platform)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_sessions (
                guild_id TEXT,
                user_id TEXT,
                platform TEXT,
                start_time TIMESTAMP WITH TIME ZONE,
                three_hour_notified BOOLEAN,
                PRIMARY KEY (guild_id, user_id, platform)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_hours (
                guild_id TEXT,
                user_id TEXT,
                total_seconds REAL DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            )
        """)
        # Retrocompatibilidade para colunas antigas
        await conn.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='live_config' AND column_name='target_guild_id') THEN
                    ALTER TABLE live_config ADD COLUMN target_guild_id BIGINT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='live_config' AND column_name='channel_ids') THEN
                    ALTER TABLE live_config ADD COLUMN channel_ids JSONB DEFAULT '[]'::jsonb;
                END IF;
            END $$;
        """)
    print("✅ Banco de dados PostgreSQL inicializado.")

async def load_all_data():
    """Carrega todos os dados do banco para o dicionário 'dados'."""
    dados = {
        "lives": {
            "config": {},
            "streamers": {},
            "last_notified": {},
            "status": {},
            "sessions": {},
            "hours": {}
        }
    }
    async with db_pool.acquire() as conn:
        # Config
        rows = await conn.fetch("SELECT guild_id, target_guild_id, channel_ids, role_id, platforms, painel_channel_id FROM live_config")
        for r in rows:
            guild_id = r["guild_id"]
            dados["lives"]["config"][guild_id] = {
                "channel_ids": json.loads(r["channel_ids"]) if r["channel_ids"] else [],
                "role": r["role_id"],
                "target_guild": r["target_guild_id"],
                "platforms": json.loads(r["platforms"]) if r["platforms"] else {"twitch": True, "youtube": True, "kick": True, "tiktok": True},
                "painel_channel_id": r["painel_channel_id"]
            }
        # Streamers (com created_at)
        rows = await conn.fetch("SELECT guild_id, user_id, nome, twitch, youtube, kick, tiktok, observacao, created_at FROM live_streamers")
        for r in rows:
            guild_id = r["guild_id"]
            user_id = r["user_id"]
            if guild_id not in dados["lives"]["streamers"]:
                dados["lives"]["streamers"][guild_id] = {}
            dados["lives"]["streamers"][guild_id][user_id] = {
                "nome": r["nome"],
                "twitch": r["twitch"],
                "youtube": r["youtube"],
                "kick": r["kick"],
                "tiktok": r["tiktok"],
                "observacao": r["observacao"],
                "created_at": r["created_at"]
            }
        # Last notified
        rows = await conn.fetch("SELECT key, value FROM live_last_notified")
        for r in rows:
            dados["lives"]["last_notified"][r["key"]] = r["value"]
        # Status
        rows = await conn.fetch("SELECT guild_id, user_id, platform, is_live FROM live_status")
        for r in rows:
            guild_id = r["guild_id"]
            user_id = r["user_id"]
            platform = r["platform"]
            if guild_id not in dados["lives"]["status"]:
                dados["lives"]["status"][guild_id] = {}
            if user_id not in dados["lives"]["status"][guild_id]:
                dados["lives"]["status"][guild_id][user_id] = {}
            dados["lives"]["status"][guild_id][user_id][platform] = r["is_live"]
        # Sessions
        rows = await conn.fetch("SELECT guild_id, user_id, platform, start_time, three_hour_notified FROM live_sessions")
        for r in rows:
            guild_id = r["guild_id"]
            user_id = r["user_id"]
            platform = r["platform"]
            if guild_id not in dados["lives"]["sessions"]:
                dados["lives"]["sessions"][guild_id] = {}
            if user_id not in dados["lives"]["sessions"][guild_id]:
                dados["lives"]["sessions"][guild_id][user_id] = {}
            start = r["start_time"]
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            dados["lives"]["sessions"][guild_id][user_id][platform] = {
                "start_time": start,
                "three_hour_notified": r["three_hour_notified"]
            }
        # Hours
        rows = await conn.fetch("SELECT guild_id, user_id, total_seconds FROM live_hours")
        for r in rows:
            guild_id = r["guild_id"]
            user_id = r["user_id"]
            if guild_id not in dados["lives"]["hours"]:
                dados["lives"]["hours"][guild_id] = {}
            dados["lives"]["hours"][guild_id][user_id] = r["total_seconds"]
    return dados

# ========= FUNÇÕES DE BANCO (com suporte a created_at) =========
async def save_config(guild_id, target_guild_id, channel_ids, role_id, platforms, painel_channel_id=None):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO live_config (guild_id, target_guild_id, channel_ids, role_id, platforms, painel_channel_id)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (guild_id) DO UPDATE SET
                target_guild_id = EXCLUDED.target_guild_id,
                channel_ids = EXCLUDED.channel_ids,
                role_id = EXCLUDED.role_id,
                platforms = EXCLUDED.platforms,
                painel_channel_id = EXCLUDED.painel_channel_id
        """, guild_id, target_guild_id, json.dumps(channel_ids), role_id, json.dumps(platforms), painel_channel_id)

async def save_streamer(guild_id, user_id, nome, twitch, youtube, kick, tiktok, observacao):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO live_streamers (guild_id, user_id, nome, twitch, youtube, kick, tiktok, observacao, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT (guild_id, user_id) DO UPDATE SET
                nome = EXCLUDED.nome,
                twitch = EXCLUDED.twitch,
                youtube = EXCLUDED.youtube,
                kick = EXCLUDED.kick,
                tiktok = EXCLUDED.tiktok,
                observacao = EXCLUDED.observacao
                -- created_at NÃO é atualizado
        """, guild_id, user_id, nome, twitch, youtube, kick, tiktok, observacao)

async def delete_streamer(guild_id, user_id):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM live_streamers WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)
        await conn.execute("DELETE FROM live_status WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)
        await conn.execute("DELETE FROM live_sessions WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)
        await conn.execute("DELETE FROM live_hours WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)

async def save_last_notified(key, value):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO live_last_notified (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, key, value)

async def save_status(guild_id, user_id, platform, is_live):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO live_status (guild_id, user_id, platform, is_live)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (guild_id, user_id, platform) DO UPDATE SET is_live = EXCLUDED.is_live
        """, guild_id, user_id, platform, is_live)

async def save_session(guild_id, user_id, platform, start_time, three_hour_notified):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO live_sessions (guild_id, user_id, platform, start_time, three_hour_notified)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (guild_id, user_id, platform) DO UPDATE SET
                start_time = EXCLUDED.start_time,
                three_hour_notified = EXCLUDED.three_hour_notified
        """, guild_id, user_id, platform, start_time, three_hour_notified)

async def delete_session(guild_id, user_id, platform):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM live_sessions WHERE guild_id=$1 AND user_id=$2 AND platform=$3", guild_id, user_id, platform)

async def add_streamer_hours(guild_id, user_id, seconds):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO live_hours (guild_id, user_id, total_seconds)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET
                total_seconds = live_hours.total_seconds + EXCLUDED.total_seconds
        """, guild_id, user_id, seconds)

async def reset_streamer_hours(guild_id, user_id=None):
    async with db_pool.acquire() as conn:
        if user_id:
            await conn.execute("UPDATE live_hours SET total_seconds = 0 WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)
        else:
            await conn.execute("UPDATE live_hours SET total_seconds = 0 WHERE guild_id = $1", guild_id)

# ========= CARREGAR DADOS INICIALMENTE =========
dados = None

async def refresh_dados():
    global dados
    dados = await load_all_data()

# ========= FUNÇÕES AUXILIARES =========
def is_admin(member):
    """Verifica se o membro tem permissão de administrador no servidor."""
    return member.guild_permissions.administrator

def extract_platform_from_url(url: str):
    url = url.strip().lower()
    if "twitch.tv" in url:
        match = re.search(r"twitch\.tv/([a-zA-Z0-9_]+)", url)
        if match:
            return ("twitch", match.group(1))
    elif "youtube.com" in url or "youtu.be" in url:
        if "youtube.com/@" in url:
            return ("youtube", url.split("@")[-1].split("/")[0])
        elif "youtube.com/channel/" in url:
            return ("youtube", url.split("/channel/")[-1].split("?")[0])
        elif "youtube.com/c/" in url:
            return ("youtube", url.split("/c/")[-1].split("/")[0])
    elif "kick.com" in url:
        match = re.search(r"kick\.com/([a-zA-Z0-9_]+)", url)
        if match:
            return ("kick", match.group(1))
    elif "tiktok.com" in url:
        match = re.search(r"tiktok\.com/@([a-zA-Z0-9_.]+)", url)
        if match:
            return ("tiktok", match.group(1))
    return (None, None)

def format_hours(seconds):
    if not seconds: return "0h 0m"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"

def format_date(dt):
    if not dt:
        return "Data desconhecida"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%d/%m/%Y às %H:%M")

# ========= ROTAÇÃO DE USER-AGENT =========
ua = UserAgent()

def get_headers():
    return {
        "User-Agent": ua.random,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }

# ========= VERIFICAÇÃO DE LIVES =========
twitch_token = None
twitch_token_expiry = 0

async def get_twitch_token():
    global twitch_token, twitch_token_expiry
    if twitch_token and datetime.utcnow().timestamp() < twitch_token_expiry:
        return twitch_token
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        return None
    async with aiohttp.ClientSession() as session:
        async with session.post("https://id.twitch.tv/oauth2/token",
                                params={"client_id": TWITCH_CLIENT_ID,
                                        "client_secret": TWITCH_CLIENT_SECRET,
                                        "grant_type": "client_credentials"}) as resp:
            if resp.status == 200:
                data = await resp.json()
                twitch_token = data["access_token"]
                twitch_token_expiry = datetime.utcnow().timestamp() + data["expires_in"] - 60
                return twitch_token
    return None

async def check_twitch_lives(streamers):
    token = await get_twitch_token()
    if not token:
        return {}
    usernames = [s for s in streamers if s]
    if not usernames:
        return {}
    headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"}
    url = "https://api.twitch.tv/helix/streams?user_login=" + "&user_login=".join(usernames)
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                return {s["user_login"].lower(): s for s in data.get("data", [])}
    return {}

async def check_youtube_lives(streamers):
    if not YOUTUBE_API_KEY:
        return {}
    live_data = {}
    for ch_id in streamers:
        if not ch_id:
            continue
        url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={ch_id}&eventType=live&type=video&key={YOUTUBE_API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data.get("items", []):
                        live_data[ch_id] = item
    return live_data

def check_kick_live_sync(username):
    scraper = cloudscraper.create_scraper()
    url = f"https://kick.com/api/v2/channels/{username}"
    headers = get_headers()
    try:
        resp = scraper.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            livestream = data.get("livestream")
            if livestream:
                return True, {
                    "title": livestream.get("session_title", "Sem título"),
                    "viewer_count": livestream.get("viewer_count", 0)
                }
            else:
                return False, None
        else:
            return False, None
    except Exception as e:
        print(f"Erro Kick {username}: {e}")
        return False, None

async def check_kick_live(username):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, check_kick_live_sync, username)

def check_tiktok_live_sync(username):
    scraper = cloudscraper.create_scraper()
    url = f"https://www.tiktok.com/@{username}/live"
    headers = get_headers()
    try:
        resp = scraper.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        html = resp.text
        title_match = re.search(r'"title":"(.*?)"', html)
        title = title_match.group(1).replace('\\u002F', '/').replace('\\u0026', '&') if title_match else "Live"
        thumb_match = re.search(r'"thumbnail_url":"(.*?)"', html)
        thumbnail = thumb_match.group(1).replace('\\u002F', '/') if thumb_match else None
        if "data-e2e=\"live-status\"" in html or "live" in title.lower():
            return {"title": title, "thumbnail": thumbnail, "url": url}
        return None
    except Exception as e:
        print(f"Erro TikTok {username}: {e}")
        return None

async def check_tiktok_live(username):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, check_tiktok_live_sync, username)

# ========= TASK DE VERIFICAÇÃO =========
async def send_notification(canal, role_mention, embed, view=None):
    try:
        if view:
            await canal.send(content=role_mention, embed=embed, view=view)
        else:
            await canal.send(content=role_mention, embed=embed)
    except Exception as e:
        print(f"Erro ao enviar notificação para {canal.id}: {e}")

async def send_to_all_channels(guild, channel_ids, role_mention, embed, view=None):
    for cid in channel_ids:
        canal = guild.get_channel(cid)
        if canal:
            await send_notification(canal, role_mention, embed, view)

@tasks.loop(minutes=1)
async def live_check_loop():
    global dados
    await refresh_dados()
    for guild_id_str in dados["lives"]["config"]:
        config = dados["lives"]["config"][guild_id_str]
        
        target_guild_id = config.get("target_guild")
        if target_guild_id:
            guild = bot.get_guild(target_guild_id)
        else:
            guil
