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
            guild = bot.get_guild(int(guild_id_str))
        
        if not guild:
            continue
        
        plataformas = config.get("platforms", {"twitch": True, "youtube": True, "kick": True, "tiktok": True})
        channel_ids = config.get("channel_ids", [])
        role_id = config.get("role")
        role_mention = f"<@&{role_id}>" if role_id and guild.get_role(role_id) else ""
        
        streamers_dict = dados["lives"]["streamers"].get(guild_id_str, {})
        status_server = dados["lives"]["status"].setdefault(guild_id_str, {})
        sessions_server = dados["lives"]["sessions"].setdefault(guild_id_str, {})

        # (A lógica de verificação de cada plataforma permanece igual ao original)
        # ===== Twitch =====
        if plataformas.get("twitch"):
            twitch_users = [data.get("twitch") for data in streamers_dict.values() if data.get("twitch")]
            lives = await check_twitch_lives(twitch_users)
            for uid, data in streamers_dict.items():
                twitch_name = data.get("twitch")
                if not twitch_name:
                    await save_status(guild_id_str, uid, "twitch", False)
                    continue
                
                is_live = twitch_name.lower() in lives
                await save_status(guild_id_str, uid, "twitch", is_live)
                status_server.setdefault(uid, {})["twitch"] = is_live
                
                if is_live:
                    live_info = lives[twitch_name.lower()]
                    title = live_info.get("title", "")
                    last_key = f"twitch_{uid}"
                    last_id = dados["lives"]["last_notified"].get(last_key)
                    stream_id = live_info["id"]
                    
                    if last_id != stream_id:
                        dados["lives"]["last_notified"][last_key] = stream_id
                        await save_last_notified(last_key, stream_id)
                        now_utc = datetime.now(timezone.utc)
                        await save_session(guild_id_str, uid, "twitch", now_utc, False)
                        
                        nome_streamer = data.get("nome", twitch_name)
                        observacao = data.get("observacao", "")
                        embed = discord.Embed(title="🔴 LIVE NA TWITCH", color=0x9146ff)
                        desc = f"**{nome_streamer}** está ao vivo na Twitch!"
                        if observacao:
                            desc += f"\n{observacao}"
                        embed.description = desc
                        embed.add_field(name="Título", value=title, inline=False)
                        embed.add_field(name="Link", value=f"https://twitch.tv/{twitch_name}", inline=False)
                        if 'thumbnail_url' in live_info:
                            thumb_url = live_info['thumbnail_url'].replace('{width}', '640').replace('{height}', '360')
                            embed.set_image(url=thumb_url)
                        await send_to_all_channels(guild, channel_ids, role_mention, embed)
                    else:
                        sess = sessions_server.get(uid, {}).get("twitch")
                        if sess and not sess.get("three_hour_notified"):
                            start = sess["start_time"]
                            if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
                            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
                            if elapsed >= 3 * 3600:
                                await save_session(guild_id_str, uid, "twitch", start, True)
                                nome_streamer = data.get("nome", twitch_name)
                                observacao = data.get("observacao", "")
                                embed = discord.Embed(title="⏰ AINDA AO VIVO (3+ HORAS)", color=0xffaa00)
                                desc = f"**{nome_streamer}** continua ao vivo na Twitch após mais de 3 horas!"
                                if observacao:
                                    desc += f"\n{observacao}"
                                embed.description = desc
                                embed.add_field(name="Título", value=title, inline=False)
                                embed.add_field(name="Link", value=f"https://twitch.tv/{twitch_name}", inline=False)
                                await send_to_all_channels(guild, channel_ids, role_mention, embed)
                else:
                    sess = sessions_server.get(uid, {}).get("twitch")
                    if sess:
                        start = sess["start_time"]
                        if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
                        duration = (datetime.now(timezone.utc) - start).total_seconds()
                        if duration > 0:
                            await add_streamer_hours(guild_id_str, uid, duration)
                        await delete_session(guild_id_str, uid, "twitch")
                        if uid in sessions_server and "twitch" in sessions_server[uid]:
                            del sessions_server[uid]["twitch"]

        # ===== YouTube =====
        if plataformas.get("youtube"):
            yt_users = [data.get("youtube") for data in streamers_dict.values() if data.get("youtube")]
            lives = await check_youtube_lives(yt_users)
            for uid, data in streamers_dict.items():
                yt_ch = data.get("youtube")
                if not yt_ch:
                    await save_status(guild_id_str, uid, "youtube", False)
                    continue
                
                is_live = yt_ch in lives
                await save_status(guild_id_str, uid, "youtube", is_live)
                status_server.setdefault(uid, {})["youtube"] = is_live
                
                if is_live:
                    video = lives[yt_ch]
                    title = video['snippet']['title']
                    last_key = f"yt_{uid}"
                    video_id = video["id"]["videoId"]
                    last_id = dados["lives"]["last_notified"].get(last_key)
                    
                    if last_id != video_id:
                        dados["lives"]["last_notified"][last_key] = video_id
                        await save_last_notified(last_key, video_id)
                        now_utc = datetime.now(timezone.utc)
                        await save_session(guild_id_str, uid, "youtube", now_utc, False)
                        
                        nome_streamer = data.get("nome", yt_ch)
                        observacao = data.get("observacao", "")
                        embed = discord.Embed(title="🔴 LIVE NO YOUTUBE", color=0xff0000)
                        desc = f"**{nome_streamer}** está ao vivo no YouTube!"
                        if observacao:
                            desc += f"\n{observacao}"
                        embed.description = desc
                        embed.add_field(name="Título", value=title, inline=False)
                        embed.add_field(name="Link", value=f"https://youtube.com/watch?v={video_id}", inline=False)
                        await send_to_all_channels(guild, channel_ids, role_mention, embed)
                    else:
                        sess = sessions_server.get(uid, {}).get("youtube")
                        if sess and not sess.get("three_hour_notified"):
                            start = sess["start_time"]
                            if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
                            if (datetime.now(timezone.utc) - start).total_seconds() >= 3 * 3600:
                                await save_session(guild_id_str, uid, "youtube", start, True)
                                nome_streamer = data.get("nome", yt_ch)
                                observacao = data.get("observacao", "")
                                embed = discord.Embed(title="⏰ AINDA AO VIVO (3+ HORAS)", color=0xffaa00)
                                desc = f"**{nome_streamer}** continua ao vivo no YouTube após mais de 3 horas!"
                                if observacao:
                                    desc += f"\n{observacao}"
                                embed.description = desc
                                embed.add_field(name="Título", value=title, inline=False)
                                embed.add_field(name="Link", value=f"https://youtube.com/watch?v={video_id}", inline=False)
                                await send_to_all_channels(guild, channel_ids, role_mention, embed)
                else:
                    sess = sessions_server.get(uid, {}).get("youtube")
                    if sess:
                        start = sess["start_time"]
                        if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
                        duration = (datetime.now(timezone.utc) - start).total_seconds()
                        if duration > 0:
                            await add_streamer_hours(guild_id_str, uid, duration)
                        await delete_session(guild_id_str, uid, "youtube")
                        if uid in sessions_server and "youtube" in sessions_server[uid]:
                            del sessions_server[uid]["youtube"]

        # ===== Kick =====
        if plataformas.get("kick"):
            for uid, data in streamers_dict.items():
                kick_name = data.get("kick")
                if not kick_name:
                    await save_status(guild_id_str, uid, "kick", False)
                    continue
                
                is_live, stream_info = await check_kick_live(kick_name)
                await save_status(guild_id_str, uid, "kick", is_live)
                status_server.setdefault(uid, {})["kick"] = is_live
                
                if is_live:
                    title = stream_info.get("title", "")
                    last_key = f"kick_{uid}"
                    last_status = dados["lives"]["last_notified"].get(last_key)
                    
                    if last_status != "live":
                        dados["lives"]["last_notified"][last_key] = "live"
                        await save_last_notified(last_key, "live")
                        now_utc = datetime.now(timezone.utc)
                        await save_session(guild_id_str, uid, "kick", now_utc, False)
                        
                        nome_streamer = data.get("nome", kick_name)
                        observacao = data.get("observacao", "")
                        embed = discord.Embed(title="🔴 LIVE NA KICK", color=0x53fc18)
                        desc = f"**{nome_streamer}** está ao vivo na Kick!"
                        if observacao:
                            desc += f"\n{observacao}"
                        embed.description = desc
                        embed.add_field(name="Título", value=title, inline=False)
                        embed.add_field(name="Espectadores", value=stream_info['viewer_count'], inline=False)
                        embed.add_field(name="Link", value=f"https://kick.com/{kick_name}", inline=False)
                        await send_to_all_channels(guild, channel_ids, role_mention, embed)
                    else:
                        sess = sessions_server.get(uid, {}).get("kick")
                        if sess and not sess.get("three_hour_notified"):
                            start = sess["start_time"]
                            if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
                            if (datetime.now(timezone.utc) - start).total_seconds() >= 3 * 3600:
                                await save_session(guild_id_str, uid, "kick", start, True)
                                nome_streamer = data.get("nome", kick_name)
                                observacao = data.get("observacao", "")
                                embed = discord.Embed(title="⏰ AINDA AO VIVO (3+ HORAS)", color=0xffaa00)
                                desc = f"**{nome_streamer}** continua ao vivo na Kick após mais de 3 horas!"
                                if observacao:
                                    desc += f"\n{observacao}"
                                embed.description = desc
                                embed.add_field(name="Título", value=title, inline=False)
                                embed.add_field(name="Link", value=f"https://kick.com/{kick_name}", inline=False)
                                await send_to_all_channels(guild, channel_ids, role_mention, embed)
                else:
                    if status_server.get(uid, {}).get("kick", False):
                        dados["lives"]["last_notified"][f"kick_{uid}"] = "offline"
                        await save_last_notified(f"kick_{uid}", "offline")
                    
                    sess = sessions_server.get(uid, {}).get("kick")
                    if sess:
                        start = sess["start_time"]
                        if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
                        duration = (datetime.now(timezone.utc) - start).total_seconds()
                        if duration > 0:
                            await add_streamer_hours(guild_id_str, uid, duration)
                        await delete_session(guild_id_str, uid, "kick")
                        if uid in sessions_server and "kick" in sessions_server[uid]:
                            del sessions_server[uid]["kick"]

        # ===== TikTok =====
        if plataformas.get("tiktok"):
            for uid, data in streamers_dict.items():
                tiktok_name = data.get("tiktok")
                if not tiktok_name:
                    await save_status(guild_id_str, uid, "tiktok", False)
                    continue
                
                live_info = await check_tiktok_live(tiktok_name)
                is_live = live_info is not None
                await save_status(guild_id_str, uid, "tiktok", is_live)
                status_server.setdefault(uid, {})["tiktok"] = is_live
                
                if is_live:
                    title = live_info.get("title", "")
                    last_key = f"tiktok_{uid}"
                    last_status = dados["lives"]["last_notified"].get(last_key)
                    
                    if last_status != "live":
                        dados["lives"]["last_notified"][last_key] = "live"
                        await save_last_notified(last_key, "live")
                        now_utc = datetime.now(timezone.utc)
                        await save_session(guild_id_str, uid, "tiktok", now_utc, False)
                        
                        nome_streamer = data.get("nome", tiktok_name)
                        observacao = data.get("observacao", "")
                        embed = discord.Embed(title="🔴 LIVE NO TIKTOK", description=f"**{nome_streamer}** está ao vivo no TikTok!", color=0xff0050, url=live_info["url"])
                        if observacao:
                            embed.description += f"\n{observacao}"
                        embed.add_field(name="Título", value=title, inline=False)
                        embed.set_footer(text="TikTok • " + datetime.now().strftime("%H:%M"))
                        if live_info.get("thumbnail"):
                            embed.set_image(url=live_info["thumbnail"])
                        view = View(timeout=None)
                        view.add_item(Button(label="Assistir Agora", style=discord.ButtonStyle.link, url=live_info["url"]))
                        await send_to_all_channels(guild, channel_ids, role_mention, embed, view=view)
                    else:
                        sess = sessions_server.get(uid, {}).get("tiktok")
                        if sess and not sess.get("three_hour_notified"):
                            start = sess["start_time"]
                            if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
                            if (datetime.now(timezone.utc) - start).total_seconds() >= 3 * 3600:
                                await save_session(guild_id_str, uid, "tiktok", start, True)
                                nome_streamer = data.get("nome", tiktok_name)
                                observacao = data.get("observacao", "")
                                embed = discord.Embed(title="⏰ AINDA AO VIVO (3+ HORAS)", color=0xffaa00)
                                desc = f"**{nome_streamer}** continua ao vivo no TikTok após mais de 3 horas!"
                                if observacao:
                                    desc += f"\n{observacao}"
                                embed.description = desc
                                embed.add_field(name="Título", value=title, inline=False)
                                embed.set_footer(text="TikTok • " + datetime.now().strftime("%H:%M"))
                                view = View(timeout=None)
                                view.add_item(Button(label="Assistir Agora", style=discord.ButtonStyle.link, url=live_info["url"]))
                                await send_to_all_channels(guild, channel_ids, role_mention, embed, view=view)
                else:
                    if status_server.get(uid, {}).get("tiktok", False):
                        dados["lives"]["last_notified"][f"tiktok_{uid}"] = "offline"
                        await save_last_notified(f"tiktok_{uid}", "offline")
                    
                    sess = sessions_server.get(uid, {}).get("tiktok")
                    if sess:
                        start = sess["start_time"]
                        if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
                        duration = (datetime.now(timezone.utc) - start).total_seconds()
                        if duration > 0:
                            await add_streamer_hours(guild_id_str, uid, duration)
                        await delete_session(guild_id_str, uid, "tiktok")
                        if uid in sessions_server and "tiktok" in sessions_server[uid]:
                            del sessions_server[uid]["tiktok"]

        # ===== ATUALIZAR PAINEL =====
        painel_channel_id = config.get("painel_channel_id")
        if painel_channel_id:
            painel_channel = guild.get_channel(painel_channel_id)
            if painel_channel:
                try:
                    # Procura uma mensagem do bot no canal para editar
                    async for msg in painel_channel.history(limit=10):
                        if msg.author == bot.user:
                            view = LiveConfigView(guild.id)
                            embed = await view.build_embed()
                            await msg.edit(embed=embed, view=view)
                            break
                    else:
                        # Nenhuma mensagem do bot encontrada, enviar nova
                        view = LiveConfigView(guild.id)
                        embed = await view.build_embed()
                        await painel_channel.send(embed=embed, view=view)
                except Exception as e:
                    print(f"Erro ao atualizar painel no canal {painel_channel_id}: {e}")

@live_check_loop.before_loop
async def before_live_check():
    await bot.wait_until_ready()

# ========= CLASSES DO PAINEL =========
class LiveConfigView(View):
    def __init__(self, guild_id):
        super().__init__(timeout=None)
        self.guild_id = guild_id

    async def get_config(self):
        return dados["lives"]["config"].get(str(self.guild_id), {
            "channel_ids": [],
            "role": None,
            "target_guild": None,
            "platforms": {"twitch": True, "youtube": True, "kick": True, "tiktok": True},
            "painel_channel_id": None
        })

    async def build_embed(self):
        config = await self.get_config()
        if config['channel_ids']:
            canais_txt = "\n".join(f"<#{cid}>" for cid in config['channel_ids'])
        else:
            canais_txt = "Não definido"
        cargo_info = f"<@&{config['role']}>" if config['role'] else "Não definido"
        target_info = f"Servidor: {config['target_guild']}" if config.get('target_guild') else "Mesmo servidor"
        plats = config['platforms']
        embed = discord.Embed(title="🔔 NOTIFICAÇÃO DE LIVES", color=0x99aab5)
        embed.add_field(name="📢 Canais", value=canais_txt, inline=False)
        embed.add_field(name="👥 Cargo (ping)", value=cargo_info, inline=False)
        embed.add_field(name="🎯 Destino", value=target_info, inline=False)
        status = "\n".join([
            f"Twitch: {'✅ Ativado' if plats['twitch'] else '❌ Desativado'}",
            f"YouTube: {'✅ Ativado' if plats['youtube'] else '❌ Desativado'}",
            f"Kick: {'✅ Ativado' if plats['kick'] else '❌ Desativado'}",
            f"TikTok: {'✅ Ativado' if plats['tiktok'] else '❌ Desativado'}"
        ])
        embed.add_field(name="🎮 Plataformas Monitoradas", value=status, inline=False)

        streamers = dados["lives"]["streamers"].get(str(self.guild_id), {})
        if streamers:
            lista_streamers = ""
            for uid, data in streamers.items():
                nome = data.get("nome", uid)
                created_at = data.get("created_at")
                data_str = format_date(created_at) if created_at else "Data desconhecida"
                
                total_sec = dados["lives"]["hours"].get(str(self.guild_id), {}).get(uid, 0)
                for p in ["twitch", "youtube", "kick", "tiktok"]:
                    sess = dados["lives"]["sessions"].get(str(self.guild_id), {}).get(uid, {}).get(p)
                    if sess:
                        start = sess["start_time"]
                        if start.tzinfo is None:
                            start = start.replace(tzinfo=timezone.utc)
                        duration = (datetime.now(timezone.utc) - start).total_seconds()
                        if duration > 0:
                            total_sec += duration
                horas_formatadas = format_hours(total_sec)

                plats_list = []
                for p in ["twitch", "youtube", "kick", "tiktok"]:
                    if data.get(p):
                        online = dados["lives"]["status"].get(str(self.guild_id), {}).get(uid, {}).get(p, False)
                        emoji = "🟢" if online else "🔴"
                        plats_list.append(f"{emoji} {p.capitalize()}: {data[p]}")
                if plats_list:
                    lista_streamers += f"**<@{uid}>** - ⏱️ {horas_formatadas}\n"
                    lista_streamers += "\n".join(plats_list)
                    lista_streamers += f"\n📅 Adicionado em: {data_str}\n\n"
            if lista_streamers:
                embed.add_field(name="📋 Streamers Cadastrados", value=lista_streamers[:1024], inline=False)
        return embed

    @discord.ui.button(label="📝 Definir Canais", style=discord.ButtonStyle.secondary, emoji="📝")
    async def set_channels(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user):
            await interaction.response.send_message("Apenas administradores podem definir os canais.", ephemeral=True)
            return
        modal = SetChannelsModal(self.guild_id, self)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="⚙️ Configuração", style=discord.ButtonStyle.secondary, emoji="⚙️")
    async def configuracao(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user):
            await interaction.response.send_message("Apenas administradores podem configurar.", ephemeral=True)
            return
        await interaction.response.defer()
        view = ConfigStreamersView(self.guild_id, self)
        embed = discord.Embed(title="⚙️ CONFIGURAÇÃO DE STREAMERS",
                              description="Gerencie os streamers e plataformas.", color=0x7289da)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="➕ Adicionar Streamer", style=discord.ButtonStyle.success, emoji="➕", row=1)
    async def adicionar(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user) and str(interaction.user.id) not in [str(uid) for uid in dados["lives"]["streamers"].get(str(self.guild_id), {}).keys()]:
            # Permite adicionar a si mesmo mesmo sem admin
            pass
        # Se não for admin, só permite adicionar a si mesmo (verificação no modal)
        await interaction.response.send_modal(AddStreamerByLinkModal(self.guild_id, self))

    @discord.ui.button(label="🔄 Atualizar Painel", style=discord.ButtonStyle.secondary, emoji="🔄", row=1)
    async def atualizar_painel(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        await interaction.response.defer()
        await refresh_dados()
        embed = await self.build_embed()
        await interaction.message.edit(embed=embed, view=self)

class SetChannelsModal(Modal, title="Definir Canais e Cargo"):
    canais_ids = TextInput(label="IDs dos canais (separados por vírgula)", placeholder="Ex: 123456789,987654321", required=True)
    cargo_id = TextInput(label="ID do cargo para mencionar", required=True)
    servidor_id = TextInput(label="ID do servidor de destino (opcional)", required=False, placeholder="Deixe em branco para usar este servidor")

    def __init__(self, guild_id, parent_view):
        super().__init__()
        self.guild_id = guild_id
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            ids_raw = self.canais_ids.value.strip()
            channel_ids = [int(x.strip()) for x in ids_raw.split(",") if x.strip().isdigit()]
            if not channel_ids:
                await interaction.followup.send("Nenhum ID de canal válido informado.", ephemeral=True)
                return
            
            rid = int(self.cargo_id.value.strip())
            target_gid = self.servidor_id.value.strip()
            if target_gid:
                target_gid = int(target_gid)
                if not bot.get_guild(target_gid):
                    await interaction.followup.send("❌ Bot não está presente no servidor informado.", ephemeral=True)
                    return
            else:
                target_gid = None

            config = dados["lives"]["config"].setdefault(str(self.guild_id),
                                                         {"platforms": {"twitch": True, "youtube": True, "kick": True, "tiktok": True}})
            config["channel_ids"] = channel_ids
            config["role"] = rid
            config["target_guild"] = target_gid

            await save_config(str(self.guild_id), target_gid, channel_ids, rid, config["platforms"], config.get("painel_channel_id"))
            await refresh_dados()
            embed = await self.parent_view.build_embed()
            await interaction.message.edit(embed=embed, view=self.parent_view)
            await interaction.followup.send("✅ Configuração salva! As notificações serão enviadas para os canais configurados.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"Erro: {e}", ephemeral=True)

class ConfigStreamersView(View):
    def __init__(self, guild_id, parent_view):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.parent_view = parent_view

    @discord.ui.button(label="➕ Adicionar Streamer", style=discord.ButtonStyle.success, emoji="➕", row=0)
    async def add(self, interaction: discord.Interaction, button: Button):
        # Permite adicionar se for admin ou se for o próprio usuário
        await interaction.response.send_modal(AddStreamerByLinkModal(self.guild_id, self.parent_view))

    @discord.ui.button(label="🗑️ Remover Streamer", style=discord.ButtonStyle.danger, emoji="🗑️", row=0)
    async def remove(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user):
            await interaction.response.send_message("Apenas administradores podem remover streamers.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        streamers = dados["lives"]["streamers"].get(str(self.guild_id), {})
        if not streamers:
            await interaction.followup.send("Nenhum streamer cadastrado.", ephemeral=True)
            return
        view = RemoveStreamerSelectView(self.guild_id, self.parent_view)
        await interaction.followup.send("Selecione o streamer para remover:", view=view, ephemeral=True)

    @discord.ui.button(label="⏱️ Resetar Horas", style=discord.ButtonStyle.primary, emoji="⏱️", row=0)
    async def reset_hours(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user):
            await interaction.response.send_message("Apenas administradores podem resetar horas.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        view = ResetHoursSelectView(self.guild_id, self.parent_view)
        await interaction.followup.send("Selecione de quem você quer zerar as horas:", view=view, ephemeral=True)

    @discord.ui.button(label="📺 Twitch", style=discord.ButtonStyle.secondary, emoji="📺", row=1)
    async def toggle_twitch(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        config = dados["lives"]["config"].setdefault(str(self.guild_id), {"platforms": {"twitch": True}})
        config["platforms"]["twitch"] = not config["platforms"].get("twitch", True)
        await save_config(str(self.guild_id), config.get("target_guild"), config.get("channel_ids", []), config.get("role"), config["platforms"], config.get("painel_channel_id"))
        await refresh_dados()
        await interaction.followup.send(f"Twitch {'ativado' if config['platforms']['twitch'] else 'desativado'}.", ephemeral=True)

    @discord.ui.button(label="▶️ YouTube", style=discord.ButtonStyle.danger, emoji="▶️", row=1)
    async def toggle_youtube(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        config = dados["lives"]["config"].setdefault(str(self.guild_id), {"platforms": {"youtube": True}})
        config["platforms"]["youtube"] = not config["platforms"].get("youtube", True)
        await save_config(str(self.guild_id), config.get("target_guild"), config.get("channel_ids", []), config.get("role"), config["platforms"], config.get("painel_channel_id"))
        await refresh_dados()
        await interaction.followup.send(f"YouTube {'ativado' if config['platforms']['youtube'] else 'desativado'}.", ephemeral=True)

    @discord.ui.button(label="🟢 Kick", style=discord.ButtonStyle.success, emoji="🟢", row=1)
    async def toggle_kick(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        config = dados["lives"]["config"].setdefault(str(self.guild_id), {"platforms": {"kick": True}})
        config["platforms"]["kick"] = not config["platforms"].get("kick", True)
        await save_config(str(self.guild_id), config.get("target_guild"), config.get("channel_ids", []), config.get("role"), config["platforms"], config.get("painel_channel_id"))
        await refresh_dados()
        await interaction.followup.send(f"Kick {'ativado' if config['platforms']['kick'] else 'desativado'}.", ephemeral=True)

    @discord.ui.button(label="🎵 TikTok", style=discord.ButtonStyle.secondary, emoji="🎵", row=1)
    async def toggle_tiktok(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        config = dados["lives"]["config"].setdefault(str(self.guild_id), {"platforms": {"tiktok": True}})
        config["platforms"]["tiktok"] = not config["platforms"].get("tiktok", True)
        await save_config(str(self.guild_id), config.get("target_guild"), config.get("channel_ids", []), config.get("role"), config["platforms"], config.get("painel_channel_id"))
        await refresh_dados()
        await interaction.followup.send(f"TikTok {'ativado' if config['platforms']['tiktok'] else 'desativado'}.", ephemeral=True)

    @discord.ui.button(label="↩️ Voltar", style=discord.ButtonStyle.secondary, emoji="↩️", row=2)
    async def voltar(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        embed = await self.parent_view.build_embed()
        await interaction.followup.send(embed=embed, view=self.parent_view, ephemeral=True)

class RemoveStreamerSelectView(View):
    def __init__(self, guild_id, parent_view):
        super().__init__(timeout=120)
        self.guild_id = guild_id
        self.parent_view = parent_view
        streamers = dados["lives"]["streamers"].get(str(guild_id), {})
        options = []
        for uid, data in streamers.items():
            nome = data.get("nome", uid)
            plats = [p.capitalize() for p in ["twitch", "youtube", "kick", "tiktok"] if data.get(p)]
            desc = f"{nome} ({', '.join(plats)})" if plats else nome
            options.append(discord.SelectOption(label=desc[:100], value=uid))
        if len(options) > 25:
            options = options[:25]
        if options:
            self.add_item(StreamerRemoveDropdown(options, guild_id, parent_view))

class StreamerRemoveDropdown(Select):
    def __init__(self, options, guild_id, parent_view):
        super().__init__(placeholder="Escolha um streamer para remover...", options=options)
        self.guild_id = guild_id
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        uid = self.values[0]
        await delete_streamer(str(self.guild_id), uid)
        await refresh_dados()
        await interaction.followup.send("Streamer removido com sucesso!", ephemeral=True)
        try:
            embed = await self.parent_view.build_embed()
            await interaction.message.edit(embed=embed, view=self.parent_view)
        except:
            pass

class ResetHoursSelectView(View):
    def __init__(self, guild_id, parent_view):
        super().__init__(timeout=120)
        self.guild_id = guild_id
        self.parent_view = parent_view
        streamers = dados["lives"]["streamers"].get(str(guild_id), {})
        options = [discord.SelectOption(label="⚠️ ZERAR TODOS OS STREAMERS", value="ALL", emoji="☢️")]
        for uid, data in streamers.items():
            nome = data.get("nome", uid)
            options.append(discord.SelectOption(label=nome[:100], value=uid))
        if len(options) > 25:
            options = options[:25]
        if options:
            self.add_item(ResetHoursDropdown(options, guild_id, parent_view))

class ResetHoursDropdown(Select):
    def __init__(self, options, guild_id, parent_view):
        super().__init__(placeholder="Escolha de quem zerar as horas...", options=options)
        self.guild_id = guild_id
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        uid = self.values[0]
        if uid == "ALL":
            await reset_streamer_hours(str(self.guild_id))
            mensagem = "As horas de **TODOS** os streamers foram zeradas com sucesso!"
        else:
            await reset_streamer_hours(str(self.guild_id), uid)
            mensagem = "As horas do streamer foram zeradas com sucesso!"
        await refresh_dados()
        await interaction.followup.send(mensagem, ephemeral=True)
        try:
            embed = await self.parent_view.build_embed()
            await interaction.message.edit(embed=embed, view=self.parent_view)
        except:
            pass

class AddStreamerByLinkModal(Modal, title="Adicionar Streamer"):
    plataforma = TextInput(label="PLATAFORMA (twitch/youtube/kick/tiktok)", placeholder="Ex: twitch", required=True)
    username = TextInput(label="USERNAME OU LINK DO STREAMER", placeholder="Ex: alanzoka ou https://twitch.tv/alanzoka", required=True)
    discord_user = TextInput(label="DISCORD DO STREAMER (opcional)", placeholder="ID ou @ do usuário", required=False)
    observacao = TextInput(label="OBSERVAÇÃO (mensagem padrão)", placeholder="Aparecerá na notificação da live", required=False)

    def __init__(self, guild_id, parent_view):
        super().__init__()
        self.guild_id = guild_id
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        plat_input = self.plataforma.value.strip().lower()
        username_input = self.username.value.strip()
        obs = self.observacao.value.strip()

        extracted_plat, extracted_id = extract_platform_from_url(username_input)
        if extracted_plat and extracted_id:
            platform = extracted_plat
            identifier = extracted_id
            nome_streamer = identifier
        else:
            if plat_input not in ["twitch", "youtube", "kick", "tiktok"]:
                await interaction.followup.send("Plataforma inválida.", ephemeral=True)
                return
            platform = plat_input
            identifier = username_input
            nome_streamer = identifier

        uid = str(interaction.user.id)
        if self.discord_user.value.strip():
            try:
                uid_str = self.discord_user.value.strip().replace("<@!", "").replace("<@", "").replace(">", "")
                uid = str(int(uid_str))
                member = interaction.guild.get_member(int(uid))
                if member:
                    nome_streamer = member.display_name
            except:
                pass

        # Se não for admin, só pode adicionar a si mesmo
        if not is_admin(interaction.user) and uid != str(interaction.user.id):
            await interaction.followup.send("Você só pode adicionar seu próprio canal.", ephemeral=True)
            return

        guild_str = str(self.guild_id)
        current = dados["lives"]["streamers"].get(guild_str, {}).get(uid, {})
        await save_streamer(
            guild_str, uid,
            nome=current.get("nome", nome_streamer),
            twitch=current.get("twitch") if platform != "twitch" else identifier,
            youtube=current.get("youtube") if platform != "youtube" else identifier,
            kick=current.get("kick") if platform != "kick" else identifier,
            tiktok=current.get("tiktok") if platform != "tiktok" else identifier,
            observacao=obs or current.get("observacao", "")
        )
        await refresh_dados()
        await interaction.followup.send(f"Streamer **{nome_streamer}** adicionado em **{platform}**!", ephemeral=True)
        try:
            embed = await self.parent_view.build_embed()
            await interaction.message.edit(embed=embed, view=self.parent_view)
        except:
            pass

# ========= COMANDOS SLASH =========
@bot.tree.command(name="setpainel", description="Define o canal onde o painel de lives será exibido e atualizado.")
@app_commands.default_permissions(administrator=True)
async def setpainel(interaction: discord.Interaction, canal: discord.TextChannel):
    """Comando para definir o canal do painel."""
    try:
        # Salvar configuração
        guild_id = str(interaction.guild_id)
        config = dados["lives"]["config"].get(guild_id, {})
        config["painel_channel_id"] = canal.id
        # Manter outras configurações
        await save_config(
            guild_id,
            config.get("target_guild"),
            config.get("channel_ids", []),
            config.get("role"),
            config.get("platforms", {"twitch": True, "youtube": True, "kick": True, "tiktok": True}),
            canal.id
        )
        await refresh_dados()

        # Enviar painel no canal
        view = LiveConfigView(interaction.guild_id)
        embed = await view.build_embed()
        await canal.send(embed=embed, view=view)
        await interaction.response.send_message(f"✅ Painel configurado para {canal.mention}.", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Erro ao configurar: {e}", ephemeral=True)

# ========= BOT PRINCIPAL =========
bot = commands.Bot(command_prefix="!", intents=discord.Intents.all())

@bot.event
async def on_ready():
    await init_db()
    global dados
    dados = await load_all_data()
    print(f"✅ Bot de Lives online: {bot.user}")

    # Sincronizar comandos slash (globalmente)
    try:
        synced = await bot.tree.sync()
        print(f"✅ Comandos slash sincronizados: {len(synced)}")
    except Exception as e:
        print(f"❌ Erro ao sincronizar comandos: {e}")

    # Iniciar a task de verificação
    live_check_loop.start()

if __name__ == "__main__":
    bot.run(TOKEN)