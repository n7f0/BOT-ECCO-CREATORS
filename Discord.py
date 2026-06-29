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
import logging
from urllib.parse import quote

# ========= CONFIGURAÇÕES DE LOG =========
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ========= CONFIGURAÇÕES =========
TOKEN = os.getenv("DISCORD_TOKEN_LIVE")
if not TOKEN:
    logger.error("Token do Discord não encontrado (DISCORD_TOKEN_LIVE).")
    sys.exit(1)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("Variável DATABASE_URL não encontrada (banco PostgreSQL no Railway).")
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
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_config (
                guild_id TEXT PRIMARY KEY,
                target_guild_id BIGINT,
                channel_ids_live JSONB,
                channel_ids_staff JSONB,
                role_live_id BIGINT,
                role_staff_id BIGINT,
                admin_role_id BIGINT,
                platforms JSONB,
                painel_channel_id BIGINT,
                observacao_padrao TEXT
            )
        """)
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
                last_milestone_hours INTEGER DEFAULT 0,
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
        await conn.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='live_config' AND column_name='channel_ids_live') THEN
                    ALTER TABLE live_config ADD COLUMN channel_ids_live JSONB DEFAULT '[]'::jsonb;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='live_config' AND column_name='channel_ids_staff') THEN
                    ALTER TABLE live_config ADD COLUMN channel_ids_staff JSONB DEFAULT '[]'::jsonb;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='live_config' AND column_name='role_live_id') THEN
                    ALTER TABLE live_config ADD COLUMN role_live_id BIGINT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='live_config' AND column_name='role_staff_id') THEN
                    ALTER TABLE live_config ADD COLUMN role_staff_id BIGINT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='live_config' AND column_name='admin_role_id') THEN
                    ALTER TABLE live_config ADD COLUMN admin_role_id BIGINT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='live_config' AND column_name='observacao_padrao') THEN
                    ALTER TABLE live_config ADD COLUMN observacao_padrao TEXT;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                               WHERE table_name='live_sessions' AND column_name='last_milestone_hours') THEN
                    ALTER TABLE live_sessions ADD COLUMN last_milestone_hours INTEGER DEFAULT 0;
                END IF;
            END $$;
        """)
        logger.info("Banco de dados PostgreSQL inicializado.")

async def load_all_data():
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
        rows = await conn.fetch("""
            SELECT guild_id, target_guild_id, channel_ids_live, channel_ids_staff,
                   role_live_id, role_staff_id, admin_role_id, platforms, painel_channel_id, observacao_padrao
            FROM live_config
        """)
        for r in rows:
            guild_id = r["guild_id"]
            dados["lives"]["config"][guild_id] = {
                "channel_ids_live": json.loads(r["channel_ids_live"]) if r["channel_ids_live"] else [],
                "channel_ids_staff": json.loads(r["channel_ids_staff"]) if r["channel_ids_staff"] else [],
                "role_live": r["role_live_id"],
                "role_staff": r["role_staff_id"],
                "admin_role": r["admin_role_id"],
                "target_guild": r["target_guild_id"],
                "platforms": json.loads(r["platforms"]) if r["platforms"] else {"twitch": True, "youtube": True, "kick": True, "tiktok": True},
                "painel_channel_id": r["painel_channel_id"],
                "observacao_padrao": r["observacao_padrao"] or ""
            }
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
        rows = await conn.fetch("SELECT key, value FROM live_last_notified")
        for r in rows:
            dados["lives"]["last_notified"][r["key"]] = r["value"]
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
        rows = await conn.fetch("SELECT guild_id, user_id, platform, start_time, last_milestone_hours FROM live_sessions")
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
                "last_milestone_hours": r["last_milestone_hours"]
            }
        rows = await conn.fetch("SELECT guild_id, user_id, total_seconds FROM live_hours")
        for r in rows:
            guild_id = r["guild_id"]
            user_id = r["user_id"]
            if guild_id not in dados["lives"]["hours"]:
                dados["lives"]["hours"][guild_id] = {}
            dados["lives"]["hours"][guild_id][user_id] = r["total_seconds"]
    return dados

async def save_config(guild_id, channel_ids_live, channel_ids_staff,
                      role_live, role_staff, admin_role, platforms, painel_channel_id, observacao_padrao):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO live_config (guild_id, target_guild_id, channel_ids_live, channel_ids_staff,
                                     role_live_id, role_staff_id, admin_role_id, platforms, painel_channel_id, observacao_padrao)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (guild_id) DO UPDATE SET
                target_guild_id = EXCLUDED.target_guild_id,
                channel_ids_live = EXCLUDED.channel_ids_live,
                channel_ids_staff = EXCLUDED.channel_ids_staff,
                role_live_id = EXCLUDED.role_live_id,
                role_staff_id = EXCLUDED.role_staff_id,
                admin_role_id = EXCLUDED.admin_role_id,
                platforms = EXCLUDED.platforms,
                painel_channel_id = EXCLUDED.painel_channel_id,
                observacao_padrao = EXCLUDED.observacao_padrao
        """, guild_id, None, json.dumps(channel_ids_live), json.dumps(channel_ids_staff),
           role_live, role_staff, admin_role, json.dumps(platforms), painel_channel_id, observacao_padrao)

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
    if dados:
        dados["lives"]["status"].setdefault(guild_id, {}).setdefault(user_id, {})[platform] = is_live

async def save_session(guild_id, user_id, platform, start_time, last_milestone_hours=0):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO live_sessions (guild_id, user_id, platform, start_time, last_milestone_hours)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (guild_id, user_id, platform) DO UPDATE SET
                start_time = EXCLUDED.start_time,
                last_milestone_hours = EXCLUDED.last_milestone_hours
        """, guild_id, user_id, platform, start_time, last_milestone_hours)
    if dados:
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        dados["lives"]["sessions"].setdefault(guild_id, {}).setdefault(user_id, {})[platform] = {
            "start_time": start_time,
            "last_milestone_hours": last_milestone_hours
        }

async def update_milestone(guild_id, user_id, platform, milestone_hours):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE live_sessions SET last_milestone_hours = $1
            WHERE guild_id = $2 AND user_id = $3 AND platform = $4
        """, milestone_hours, guild_id, user_id, platform)
    if dados:
        sess = dados["lives"]["sessions"].get(guild_id, {}).get(user_id, {}).get(platform)
        if sess:
            sess["last_milestone_hours"] = milestone_hours

async def delete_session(guild_id, user_id, platform):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM live_sessions WHERE guild_id=$1 AND user_id=$2 AND platform=$3", guild_id, user_id, platform)
    if dados:
        try:
            dados["lives"]["sessions"].get(guild_id, {}).get(user_id, {}).pop(platform, None)
        except Exception:
            pass

async def add_streamer_hours(guild_id, user_id, seconds):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO live_hours (guild_id, user_id, total_seconds)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET
                total_seconds = live_hours.total_seconds + EXCLUDED.total_seconds
        """, guild_id, user_id, seconds)
    if dados:
        current = dados["lives"]["hours"].get(guild_id, {}).get(user_id, 0)
        dados["lives"]["hours"].setdefault(guild_id, {})[user_id] = current + seconds

async def reset_streamer_hours(guild_id, user_id=None):
    async with db_pool.acquire() as conn:
        if user_id:
            await conn.execute("UPDATE live_hours SET total_seconds = 0 WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)
        else:
            await conn.execute("UPDATE live_hours SET total_seconds = 0 WHERE guild_id = $1", guild_id)

dados = None

async def refresh_dados():
    global dados
    dados = await load_all_data()

def is_admin(member, guild_id=None):
    if member.guild_permissions.administrator:
        return True
    if guild_id is not None:
        config = dados["lives"]["config"].get(str(guild_id), {})
        admin_role_id = config.get("admin_role")
        if admin_role_id:
            role = member.guild.get_role(admin_role_id)
            if role and role in member.roles:
                return True
    return False

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

# ========= USER-AGENT =========
ua = UserAgent()

def get_headers():
    return {
        "User-Agent": ua.random,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
    }

# ========= VERIFICAÇÃO DE LIVES =========
twitch_token = None
twitch_token_expiry = 0

async def get_twitch_token():
    global twitch_token, twitch_token_expiry
    if twitch_token and datetime.now(timezone.utc).timestamp() < twitch_token_expiry:
        logger.info("Usando token Twitch em cache")
        return twitch_token
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        logger.error("TWITCH_CLIENT_ID ou TWITCH_CLIENT_SECRET não configurados")
        return None
    logger.info("Obtendo novo token Twitch...")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post("https://id.twitch.tv/oauth2/token",
                                    params={"client_id": TWITCH_CLIENT_ID,
                                            "client_secret": TWITCH_CLIENT_SECRET,
                                            "grant_type": "client_credentials"}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    twitch_token = data["access_token"]
                    twitch_token_expiry = datetime.now(timezone.utc).timestamp() + data["expires_in"] - 60
                    logger.info(f"Token Twitch obtido com sucesso, expira em {data['expires_in']}s")
                    return twitch_token
                else:
                    error_text = await resp.text()
                    logger.error(f"Falha ao obter token Twitch: status {resp.status}, resposta: {error_text[:200]}")
                    return None
        except Exception as e:
            logger.error(f"Exceção ao obter token Twitch: {e}")
            return None

async def check_twitch_lives(usernames):
    token = await get_twitch_token()
    if not token:
        logger.error("Sem token Twitch válido, pulando verificação")
        return {}
    
    valid_usernames = []
    invalid_usernames = []
    for u in usernames:
        if not u:
            continue
        if re.match(r'^[a-zA-Z0-9_]+$', u):
            valid_usernames.append(u)
        else:
            invalid_usernames.append(u)
    
    if invalid_usernames:
        logger.debug(f"Nomes de usuário inválidos para Twitch (serão ignorados): {invalid_usernames}")
    
    if not valid_usernames:
        logger.info("Nenhum nome de usuário Twitch válido para verificar")
        return {}
    
    headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"}
    params = [("user_login", u) for u in valid_usernames]
    
    logger.info(f"Verificando Twitch para {len(valid_usernames)} usuários: {valid_usernames[:5]}...")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://api.twitch.tv/helix/streams", headers=headers, params=params, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logger.info(f"Twitch: {len(data.get('data', []))} lives encontradas")
                    return {s["user_login"].lower(): s for s in data.get("data", [])}
                else:
                    error_text = await resp.text()
                    logger.warning(f"Twitch API retornou {resp.status}, resposta: {error_text[:300]}")
                    return {}
        except Exception as e:
            logger.error(f"Erro ao verificar Twitch: {e}")
            return {}

async def get_youtube_channel_id(handle: str) -> str | None:
    if not YOUTUBE_API_KEY:
        return None
    handle = handle.lstrip('@')
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "q": handle,
        "type": "channel",
        "maxResults": 1,
        "key": YOUTUBE_API_KEY
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params, timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = data.get("items", [])
                    if items:
                        channel_id = items[0]["snippet"]["channelId"]
                        logger.info(f"Resolvido handle {handle} -> channel ID {channel_id}")
                        return channel_id
                    else:
                        logger.warning(f"Nenhum canal encontrado para {handle}")
                else:
                    error_text = await resp.text()
                    logger.warning(f"Falha ao buscar channel ID para {handle}: {resp.status} - {error_text[:200]}")
        except Exception as e:
            logger.error(f"Erro ao buscar channel ID: {e}")
    return None

async def check_youtube_lives(identifiers):
    if not YOUTUBE_API_KEY:
        logger.error("YOUTUBE_API_KEY não configurada")
        return {}
    live_data = {}
    for identifier in identifiers:
        if not identifier:
            continue
        if not identifier.startswith("UC"):
            channel_id = await get_youtube_channel_id(identifier)
            if not channel_id:
                logger.warning(f"Não foi possível resolver canal para {identifier}, pulando")
                continue
        else:
            channel_id = identifier

        url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={channel_id}&eventType=live&type=video&key={YOUTUBE_API_KEY}"
        logger.info(f"Verificando YouTube para canal: {channel_id}")
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=15) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("items"):
                            live_data[identifier] = data["items"][0]
                            logger.info(f"YouTube: live encontrada para {identifier}")
                        else:
                            logger.info(f"YouTube: sem live para {identifier}")
                    else:
                        error_text = await resp.text()
                        logger.warning(f"YouTube API retornou {resp.status} para {identifier}, resposta: {error_text[:200]}")
            except Exception as e:
                logger.error(f"Erro ao verificar YouTube para {identifier}: {e}")
    return live_data

def check_kick_live_sync(username, retries=2):
    scraper = cloudscraper.create_scraper()
    url = f"https://kick.com/api/v2/channels/{username}"
    headers = get_headers()
    for attempt in range(retries):
        try:
            resp = scraper.get(url, headers=headers, timeout=20)
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
                logger.warning(f"Kick retornou {resp.status_code} para {username}, tentativa {attempt+1}")
        except Exception as e:
            logger.warning(f"Erro Kick {username} (tentativa {attempt+1}): {e}")
            if attempt == retries - 1:
                logger.error(f"Falha ao verificar Kick para {username} após {retries} tentativas")
    return False, None

async def check_kick_live(username):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, check_kick_live_sync, username)

def check_tiktok_live_sync(username, retries=2):
    scraper = cloudscraper.create_scraper()
    url = f"https://www.tiktok.com/@{username}/live"
    headers = get_headers()
    for attempt in range(retries):
        try:
            resp = scraper.get(url, headers=headers, timeout=20, allow_redirects=True)
            if resp.status_code != 200:
                logger.warning(f"TikTok retornou {resp.status_code} para {username}, tentativa {attempt+1}")
                continue

            # Quando o usuário NÃO está ao vivo, o TikTok redireciona para o perfil,
            # removendo o "/live" da URL final.
            final_url = str(resp.url)
            if "/live" not in final_url:
                logger.info(f"TikTok: {username} não está ao vivo (redirecionado para {final_url})")
                return None

            html = resp.text

            # Markers confiáveis presentes no JSON da página apenas quando há live ativa.
            # NÃO usar "live" no título, pois gera falsos positivos.
            live_markers = [
                '"liveRoomInfo"',
                '"isLiving":true',
                '"status":2',
                '"liveRoomUserInfo"',
                '"roomId"',
                '"LIVE_STREAMING"',
            ]
            is_live = any(marker in html for marker in live_markers)

            if not is_live:
                logger.info(f"TikTok: nenhum marcador de live encontrado para {username}")
                return None

            title_match = re.search(r'"title"\s*:\s*"((?:[^"\\]|\\.)*)"', html)
            if title_match:
                title = title_match.group(1)
                title = title.replace('\\u002F', '/').replace('\\u0026', '&').replace('\\"', '"')
            else:
                title = "Live"

            thumb_match = re.search(r'"thumbnail_url"\s*:\s*"([^"]*)"', html)
            thumbnail = thumb_match.group(1).replace('\\/', '/') if thumb_match else None

            logger.info(f"TikTok: {username} está ao vivo! Título: {title}")
            return {"title": title, "thumbnail": thumbnail, "url": url}
        except Exception as e:
            logger.warning(f"Erro TikTok {username} (tentativa {attempt+1}): {e}")
            if attempt == retries - 1:
                logger.error(f"Falha ao verificar TikTok para {username} após {retries} tentativas")
    return None

async def check_tiktok_live(username):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, check_tiktok_live_sync, username)

# ========= ENVIO DE NOTIFICAÇÕES =========
async def send_notification(canal, content, embed, view=None):
    try:
        if view:
            await canal.send(content=content, embed=embed, view=view)
        else:
            await canal.send(content=content, embed=embed)
    except Exception as e:
        logger.error(f"Erro ao enviar notificação para {canal.id}: {e}")

async def send_to_channels(guild, channel_ids, role_mention, embed, view=None):
    for cid in channel_ids:
        canal = guild.get_channel(cid)
        if canal:
            await send_notification(canal, role_mention, embed, view)

# ========= FUNÇÃO AUXILIAR PARA TESTE MANUAL =========
async def test_streamer_live(guild_id_str, uid, guild):
    config = dados["lives"]["config"].get(guild_id_str, {})
    streamer_data = dados["lives"]["streamers"].get(guild_id_str, {}).get(uid)
    if not streamer_data:
        return {"erro": "Streamer não encontrado"}

    channel_ids_live = config.get("channel_ids_live", [])
    role_live_id = config.get("role_live")
    role_mention = f"<@&{role_live_id}>" if role_live_id and guild.get_role(role_live_id) else ""
    observacao_padrao = config.get("observacao_padrao", "")
    
    resultados = {}
    qualquer_online = False

    # ---- Twitch ----
    if streamer_data.get("twitch"):
        twitch_name = streamer_data["twitch"]
        lives = await check_twitch_lives([twitch_name])
        is_live = twitch_name.lower() in lives
        resultados["twitch"] = is_live
        await save_status(guild_id_str, uid, "twitch", is_live)
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
                await save_session(guild_id_str, uid, "twitch", now_utc, 0)
                nome = streamer_data.get("nome", twitch_name)
                obs = streamer_data.get("observacao") or observacao_padrao
                embed = discord.Embed(title="🔴 LIVE NA TWITCH", color=0x9146ff)
                desc = f"**{nome}** está ao vivo na Twitch!"
                if obs:
                    desc += f"\n{obs}"
                embed.description = desc
                embed.add_field(name="Título", value=title, inline=False)
                embed.add_field(name="Link", value=f"https://twitch.tv/{twitch_name}", inline=False)
                if 'thumbnail_url' in live_info:
                    thumb_url = live_info['thumbnail_url'].replace('{width}', '640').replace('{height}', '360')
                    embed.set_image(url=thumb_url)
                embed.set_footer(text="Twitch • " + datetime.now().strftime("%H:%M"))
                await send_to_channels(guild, channel_ids_live, role_mention, embed)
                qualquer_online = True
            else:
                qualquer_online = True
    else:
        resultados["twitch"] = False

    # ---- YouTube ----
    if streamer_data.get("youtube"):
        yt_identifier = streamer_data["youtube"]
        lives = await check_youtube_lives([yt_identifier])
        is_live = yt_identifier in lives
        resultados["youtube"] = is_live
        await save_status(guild_id_str, uid, "youtube", is_live)
        if is_live:
            video = lives[yt_identifier]
            title = video['snippet']['title']
            video_id = video["id"]["videoId"]
            last_key = f"yt_{uid}"
            last_id = dados["lives"]["last_notified"].get(last_key)
            if last_id != video_id:
                dados["lives"]["last_notified"][last_key] = video_id
                await save_last_notified(last_key, video_id)
                now_utc = datetime.now(timezone.utc)
                await save_session(guild_id_str, uid, "youtube", now_utc, 0)
                nome = streamer_data.get("nome", yt_identifier)
                obs = streamer_data.get("observacao") or observacao_padrao
                embed = discord.Embed(title="🔴 LIVE NO YOUTUBE", color=0xff0000)
                desc = f"**{nome}** está ao vivo no YouTube!"
                if obs:
                    desc += f"\n{obs}"
                embed.description = desc
                embed.add_field(name="Título", value=title, inline=False)
                embed.add_field(name="Link", value=f"https://youtube.com/watch?v={video_id}", inline=False)
                embed.set_footer(text="YouTube • " + datetime.now().strftime("%H:%M"))
                await send_to_channels(guild, channel_ids_live, role_mention, embed)
                qualquer_online = True
            else:
                qualquer_online = True
    else:
        resultados["youtube"] = False

    # ---- Kick ----
    if streamer_data.get("kick"):
        kick_name = streamer_data["kick"]
        is_live, stream_info = await check_kick_live(kick_name)
        resultados["kick"] = is_live
        await save_status(guild_id_str, uid, "kick", is_live)
        if is_live:
            title = stream_info.get("title", "")
            last_key = f"kick_{uid}"
            last_status = dados["lives"]["last_notified"].get(last_key)
            if last_status != "live":
                dados["lives"]["last_notified"][last_key] = "live"
                await save_last_notified(last_key, "live")
                now_utc = datetime.now(timezone.utc)
                await save_session(guild_id_str, uid, "kick", now_utc, 0)
                nome = streamer_data.get("nome", kick_name)
                obs = streamer_data.get("observacao") or observacao_padrao
                embed = discord.Embed(title="🔴 LIVE NA KICK", color=0x53fc18)
                desc = f"**{nome}** está ao vivo na Kick!"
                if obs:
                    desc += f"\n{obs}"
                embed.description = desc
                embed.add_field(name="Título", value=title, inline=False)
                embed.add_field(name="Espectadores", value=stream_info['viewer_count'], inline=False)
                embed.add_field(name="Link", value=f"https://kick.com/{kick_name}", inline=False)
                embed.set_footer(text="Kick • " + datetime.now().strftime("%H:%M"))
                await send_to_channels(guild, channel_ids_live, role_mention, embed)
                qualquer_online = True
            else:
                qualquer_online = True
    else:
        resultados["kick"] = False

    # ---- TikTok ----
    if streamer_data.get("tiktok"):
        tiktok_name = streamer_data["tiktok"]
        live_info = await check_tiktok_live(tiktok_name)
        is_live = live_info is not None
        resultados["tiktok"] = is_live
        await save_status(guild_id_str, uid, "tiktok", is_live)
        if is_live:
            title = live_info.get("title", "")
            last_key = f"tiktok_{uid}"
            last_status = dados["lives"]["last_notified"].get(last_key)
            if last_status != "live":
                dados["lives"]["last_notified"][last_key] = "live"
                await save_last_notified(last_key, "live")
                now_utc = datetime.now(timezone.utc)
                await save_session(guild_id_str, uid, "tiktok", now_utc, 0)
                nome = streamer_data.get("nome", tiktok_name)
                obs = streamer_data.get("observacao") or observacao_padrao
                embed = discord.Embed(title="🔴 LIVE NO TIKTOK", color=0xff0050, url=live_info["url"])
                desc = f"**{nome}** está ao vivo no TikTok!"
                if obs:
                    desc += f"\n{obs}"
                embed.description = desc
                embed.add_field(name="Título", value=title, inline=False)
                embed.set_footer(text="TikTok • " + datetime.now().strftime("%H:%M"))
                if live_info.get("thumbnail"):
                    embed.set_image(url=live_info["thumbnail"])
                view = View(timeout=None)
                view.add_item(Button(label="Assistir Agora", style=discord.ButtonStyle.link, url=live_info["url"]))
                await send_to_channels(guild, channel_ids_live, role_mention, embed, view=view)
                qualquer_online = True
            else:
                qualquer_online = True
    else:
        resultados["tiktok"] = False

    resultados["notificacao_enviada"] = qualquer_online
    return resultados

# ========= CRIAÇÃO DO BOT =========
bot = commands.Bot(command_prefix="!", intents=discord.Intents.all())

# ========= CLASSES DO PAINEL =========
class LiveConfigView(View):
    def __init__(self, guild_id, page=0):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.current_page = page

    async def get_config(self):
        return dados["lives"]["config"].get(str(self.guild_id), {
            "channel_ids_live": [],
            "channel_ids_staff": [],
            "role_live": None,
            "role_staff": None,
            "admin_role": None,
            "platforms": {"twitch": True, "youtube": True, "kick": True, "tiktok": True},
            "painel_channel_id": None,
            "observacao_padrao": ""
        })

    async def build_embed(self, page=None):
        if page is not None:
            self.current_page = page
        config = await self.get_config()
        live_canais = "\n".join(f"<#{cid}>" for cid in config['channel_ids_live']) or "Não definido"
        staff_canais = "\n".join(f"<#{cid}>" for cid in config['channel_ids_staff']) or "Não definido"
        cargo_live = f"<@&{config['role_live']}>" if config['role_live'] else "Não definido"
        cargo_staff = f"<@&{config['role_staff']}>" if config['role_staff'] else "Não definido"
        cargo_admin = f"<@&{config['admin_role']}>" if config['admin_role'] else "Não definido"
        obs_padrao = config.get('observacao_padrao') or "Nenhuma"

        plats = config['platforms']
        status_plats = "\n".join([
            f"Twitch: {'✅' if plats['twitch'] else '❌'}",
            f"YouTube: {'✅' if plats['youtube'] else '❌'}",
            f"Kick: {'✅' if plats['kick'] else '❌'}",
            f"TikTok: {'✅' if plats['tiktok'] else '❌'}"
        ])

        embed = discord.Embed(title="🔔 PAINEL DE NOTIFICAÇÕES DE LIVES", color=0x99aab5)
        embed.add_field(name="📢 Canais de Live (público)", value=live_canais, inline=False)
        embed.add_field(name="🛡️ Canais de Staff (marcos)", value=staff_canais, inline=False)
        embed.add_field(name="👥 Cargo para ping (live)", value=cargo_live, inline=False)
        embed.add_field(name="👥 Cargo para ping (staff)", value=cargo_staff, inline=False)
        embed.add_field(name="🔑 Cargo Administrador", value=cargo_admin, inline=False)
        embed.add_field(name="📝 Observação padrão", value=obs_padrao, inline=False)
        embed.add_field(name="🎮 Plataformas", value=status_plats, inline=True)

        streamers = dados["lives"]["streamers"].get(str(self.guild_id), {})
        if streamers:
            items = list(streamers.items())
            total = len(items)
            per_page = 10
            total_pages = max(1, (total + per_page - 1) // per_page)

            if self.current_page >= total_pages:
                self.current_page = total_pages - 1
            if self.current_page < 0:
                self.current_page = 0

            start = self.current_page * per_page
            end = start + per_page
            page_items = items[start:end]

            lista = ""
            for uid, data in page_items:
                nome_db = data.get("nome", uid)
                # Garante que o formato de menção (@) com ID e o nome sempre apareçam claramente.
                if uid.isdigit():
                    nome_exibicao = f"<@{uid}> (`{nome_db}`)"
                else:
                    nome_exibicao = f"**{nome_db}**"
                    
                created_at = data.get("created_at")
                data_str = format_date(created_at) if created_at else "Data desconhecida"
                total_sec = dados["lives"]["hours"].get(str(self.guild_id), {}).get(uid, 0)
                for p in ["twitch", "youtube", "kick", "tiktok"]:
                    sess = dados["lives"]["sessions"].get(str(self.guild_id), {}).get(uid, {}).get(p)
                    if sess:
                        start_time = sess["start_time"]
                        if start_time.tzinfo is None:
                            start_time = start_time.replace(tzinfo=timezone.utc)
                        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
                        if duration > 0:
                            total_sec += duration
                horas = format_hours(total_sec)

                plats_list = []
                for p in ["twitch", "youtube", "kick", "tiktok"]:
                    if data.get(p):
                        online = dados["lives"]["status"].get(str(self.guild_id), {}).get(uid, {}).get(p, False)
                        emoji = "🟢" if online else "🔴"
                        plats_list.append(f"{emoji} {p.capitalize()}: {data[p]}")
                if plats_list:
                    lista += f"**{nome_exibicao}** - ⏱️ {horas}\n" + "\n".join(plats_list) + f"\n📅 {data_str}\n\n"

            if lista:
                embed.add_field(name=f"📋 Streamers Cadastrados (página {self.current_page+1}/{total_pages})",
                                value=lista[:1024], inline=False)
            else:
                embed.add_field(name="📋 Streamers", value="Nenhum streamer nesta página.", inline=False)

            embed.set_footer(text=f"Página {self.current_page+1} de {total_pages} | Total: {total} streamers")
        else:
            embed.add_field(name="📋 Streamers Cadastrados", value="Nenhum streamer cadastrado.", inline=False)

        return embed

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.secondary, row=2)
    async def previous_page(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user, self.guild_id):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        if self.current_page > 0:
            self.current_page -= 1
            embed = await self.build_embed()
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.send_message("Você já está na primeira página.", ephemeral=True)

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.secondary, row=2)
    async def next_page(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user, self.guild_id):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        streamers = dados["lives"]["streamers"].get(str(self.guild_id), {})
        total = len(streamers)
        total_pages = max(1, (total + 9) // 10)
        if self.current_page < total_pages - 1:
            self.current_page += 1
            embed = await self.build_embed()
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.send_message("Você já está na última página.", ephemeral=True)

    @discord.ui.button(label="⚙️ Configurar Canais/Cargos", style=discord.ButtonStyle.secondary, row=0)
    async def set_channels(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user, self.guild_id):
            await interaction.response.send_message("Você não tem permissão para isso.", ephemeral=True)
            return
        try:
            modal = SetChannelsModal(self.guild_id, self)
            await interaction.response.send_modal(modal)
        except Exception as e:
            logger.error(f"Erro ao abrir modal: {e}", exc_info=True)
            await interaction.response.send_message(f"❌ Erro ao abrir o formulário: {e}", ephemeral=True)

    @discord.ui.button(label="👥 Gerenciar Streamers", style=discord.ButtonStyle.secondary, row=1)
    async def gerenciar(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user, self.guild_id):
            await interaction.response.send_message("Permissão negada.", ephemeral=True)
            return
        await interaction.response.defer()
        view = ConfigStreamersView(self.guild_id, self)
        embed = discord.Embed(title="⚙️ GERENCIAR STREAMERS", color=0x7289da)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="🔄 Atualizar", style=discord.ButtonStyle.secondary, row=1)
    async def atualizar(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user, self.guild_id):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        await interaction.response.defer()
        await refresh_dados()
        self.current_page = 0
        embed = await self.build_embed()
        await interaction.message.edit(embed=embed, view=self)

    @discord.ui.button(label="⏱️ Resetar Horas", style=discord.ButtonStyle.primary, row=2)
    async def resetar_horas(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user, self.guild_id):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await reset_streamer_hours(str(self.guild_id))
        await refresh_dados()
        embed = await self.build_embed()
        await interaction.message.edit(embed=embed, view=self)
        await interaction.followup.send("✅ Horas de todos os streamers resetadas.", ephemeral=True)

# ---- MODAL CONFIG ----
class SetChannelsModal(Modal, title="Configurar Canais e Cargos"):
    live_canais = TextInput(
        label="IDs dos canais de LIVE (vírgula)",
        placeholder="Ex: 123456,789101",
        required=True
    )
    staff_canais = TextInput(
        label="IDs dos canais de STAFF (vírgula)",
        placeholder="Ex: 112233,445566",
        required=True
    )
    cargo_live = TextInput(label="ID cargo ping (live)", required=True)
    cargo_staff = TextInput(label="ID cargo ping (staff)", required=True)
    cargo_admin = TextInput(label="ID cargo administrador", required=True, placeholder="Obrigatório")

    def __init__(self, guild_id, parent_view):
        super().__init__(timeout=600)
        self.guild_id = guild_id
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            live_ids = [int(x.strip()) for x in self.live_canais.value.split(',') if x.strip().isdigit()]
            staff_ids = [int(x.strip()) for x in self.staff_canais.value.split(',') if x.strip().isdigit()]

            if not live_ids or not staff_ids:
                await interaction.followup.send("IDs de canais inválidos. Use apenas números separados por vírgula.", ephemeral=True)
                return

            role_live = int(self.cargo_live.value.strip())
            role_staff = int(self.cargo_staff.value.strip())
            admin_role = int(self.cargo_admin.value.strip())

            guild = interaction.guild
            for role_id in [role_live, role_staff, admin_role]:
                if not guild.get_role(role_id):
                    await interaction.followup.send(f"❌ O cargo com ID {role_id} não existe neste servidor.", ephemeral=True)
                    return

            config = dados["lives"]["config"].setdefault(str(self.guild_id), {})
            config["channel_ids_live"] = live_ids
            config["channel_ids_staff"] = staff_ids
            config["role_live"] = role_live
            config["role_staff"] = role_staff
            config["admin_role"] = admin_role
            config["platforms"] = config.get("platforms", {"twitch": True, "youtube": True, "kick": True, "tiktok": True})
            config["painel_channel_id"] = config.get("painel_channel_id")
            config["observacao_padrao"] = config.get("observacao_padrao", "")

            await save_config(
                str(self.guild_id),
                live_ids,
                staff_ids,
                role_live,
                role_staff,
                admin_role,
                config["platforms"],
                config.get("painel_channel_id"),
                config.get("observacao_padrao", "")
            )
            await refresh_dados()
            embed = await self.parent_view.build_embed()
            await interaction.message.edit(embed=embed, view=self.parent_view)
            await interaction.followup.send("✅ Configuração salva!", ephemeral=True)
        except ValueError as ve:
            await interaction.followup.send(f"❌ Erro ao converter IDs: {ve}. Certifique-se de que os campos contenham apenas números.", ephemeral=True)
        except Exception as e:
            logger.error(f"Erro ao salvar configuração: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Erro inesperado: {e}", ephemeral=True)

# ---- GERENCIAR STREAMERS ----
class ConfigStreamersView(View):
    def __init__(self, guild_id, parent_view):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.parent_view = parent_view

    @discord.ui.button(label="➕ Adicionar Streamer", style=discord.ButtonStyle.success, row=0)
    async def adicionar(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user, self.guild_id):
            await interaction.response.send_message("Permissão negada.", ephemeral=True)
            return
        await interaction.response.send_modal(AddStreamerByLinkModal(self.guild_id, self.parent_view))

    @discord.ui.button(label="🗑️ Remover", style=discord.ButtonStyle.danger, row=0)
    async def remove(self, interaction: discord.Interaction, button: Button):
        if not is_admin(interaction.user, self.guild_id):
            await interaction.response.send_message("Permissão negada.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await refresh_dados()
        streamers = dados["lives"]["streamers"].get(str(self.guild_id), {})
        if not streamers:
            await interaction.followup.send("Nenhum streamer cadastrado.", ephemeral=True)
            return
        view = RemoveStreamerSelectView(self.guild_id, self.parent_view, page=0)
        total = len(streamers)
        await interaction.followup.send(
            f"Selecione o streamer para remover ({total} cadastrado(s)):",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="↩️ Voltar", style=discord.ButtonStyle.secondary, row=0)
    async def voltar(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        embed = await self.parent_view.build_embed()
        await interaction.followup.send(embed=embed, view=self.parent_view, ephemeral=True)


class PrevPageButton(Button):
    def __init__(self, remove_view: "RemoveStreamerSelectView"):
        super().__init__(label="◀ Anterior", style=discord.ButtonStyle.secondary, row=1)
        self.remove_view = remove_view

    async def callback(self, interaction: discord.Interaction):
        if self.remove_view.page > 0:
            self.remove_view.page -= 1
            self.remove_view._rebuild()
            await interaction.response.edit_message(view=self.remove_view)
        else:
            await interaction.response.send_message("Já está na primeira página.", ephemeral=True)


class NextPageButton(Button):
    def __init__(self, remove_view: "RemoveStreamerSelectView"):
        super().__init__(label="Próxima ▶", style=discord.ButtonStyle.secondary, row=1)
        self.remove_view = remove_view

    async def callback(self, interaction: discord.Interaction):
        if self.remove_view.page < self.remove_view.total_pages - 1:
            self.remove_view.page += 1
            self.remove_view._rebuild()
            await interaction.response.edit_message(view=self.remove_view)
        else:
            await interaction.response.send_message("Já está na última página.", ephemeral=True)


class RemoveStreamerSelectView(View):
    def __init__(self, guild_id, parent_view, page=0):
        super().__init__(timeout=120)
        self.guild_id = guild_id
        self.parent_view = parent_view
        self.page = page
        self.per_page = 25
        self.total_pages = 1
        self._rebuild()

    def _rebuild(self):
        self.clear_items()

        streamers = dados["lives"]["streamers"].get(str(self.guild_id), {})
        items = list(streamers.items())
        total = len(items)
        self.total_pages = max(1, (total + self.per_page - 1) // self.per_page)

        if self.page >= self.total_pages:
            self.page = self.total_pages - 1
        if self.page < 0:
            self.page = 0

        start = self.page * self.per_page
        end = min(start + self.per_page, total)
        page_items = items[start:end]

        options = []
        for uid, data in page_items:
            nome = data.get("nome", uid)
            plats = [p.capitalize() for p in ["twitch", "youtube", "kick", "tiktok"] if data.get(p)]
            desc_plats = f"Plataformas: {', '.join(plats)}" if plats else "Nenhuma plataforma vinculada"
            
            # Mostra explicitamente no select quem é a pessoa, incluindo ID se houver
            if uid.isdigit():
                label_text = f"👤 {nome} (ID/Conta Vinculada)"
            else:
                label_text = f"👤 {nome} (Adicionado manualmente)"
                
            options.append(discord.SelectOption(label=label_text[:100], description=desc_plats[:100], value=uid))

        if not options:
            options.append(discord.SelectOption(label="Nenhum streamer", value="none", default=True))

        select = StreamerRemoveDropdown(options, self.guild_id, self.parent_view)
        self.add_item(select)

        if self.total_pages > 1:
            self.add_item(PrevPageButton(self))
            self.add_item(NextPageButton(self))


class StreamerRemoveDropdown(Select):
    def __init__(self, options, guild_id, parent_view):
        super().__init__(
            placeholder=f"Escolha um streamer para remover...",
            options=options,
            row=0
        )
        self.guild_id = guild_id
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        uid = self.values[0]
        if uid == "none":
            await interaction.followup.send("Nenhum streamer disponível.", ephemeral=True)
            return
        streamer_data = dados["lives"]["streamers"].get(str(self.guild_id), {}).get(uid, {})
        nome = streamer_data.get("nome", uid)
        await delete_streamer(str(self.guild_id), uid)
        await refresh_dados()
        await interaction.followup.send(f"✅ Streamer **{nome}** removido com sucesso!", ephemeral=True)
        try:
            embed = await self.parent_view.build_embed()
        except Exception:
            pass


# ---- ADICIONAR STREAMER ----
class AddStreamerByLinkModal(Modal, title="Adicionar Streamer"):
    plataforma = TextInput(
        label="PLATAFORMA (twitch/youtube/kick/tiktok)",
        placeholder="Ex: twitch",
        required=True
    )
    username = TextInput(
        label="USERNAME OU LINK",
        placeholder="Ex: alanzoka ou https://twitch.tv/alanzoka",
        required=True
    )
    discord_user = TextInput(
        label="DISCORD DO STREAMER (opcional)",
        placeholder="ID Numérico ou @ (Menção)",
        required=False
    )

    def __init__(self, guild_id, parent_view):
        super().__init__(timeout=300)
        self.guild_id = guild_id
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            plat_input = self.plataforma.value.strip().lower()
            username_input = self.username.value.strip()

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

            if platform == "youtube":
                if not identifier.startswith("UC"):
                    channel_id = await get_youtube_channel_id(identifier)
                    if channel_id:
                        identifier = channel_id
                        logger.info(f"YouTube: handle {username_input} resolvido para ID {channel_id}")
                    else:
                        await interaction.followup.send("❌ Não foi possível encontrar o canal do YouTube. Verifique o nome/handle.", ephemeral=True)
                        return

            uid = str(interaction.user.id)
            discord_input = self.discord_user.value.strip()
            
            # Tratamento blindado para ID: Evita que administradores sobrescrevam IDs acidentalmente 
            # digitando nomes soltos ou menções quebradas ao invés de IDs válidos.
            if discord_input:
                # Extrai apenas os números da entrada, ignorando "texto", "@" e etc
                extracted_uid = re.sub(r"\D", "", discord_input)
                if extracted_uid:
                    uid = extracted_uid
                    member = interaction.guild.get_member(int(uid))
                    if member:
                        nome_streamer = member.display_name
                    else:
                        # Tenta buscar pelo banco global de usuários do bot caso ele já tenha saído do servidor
                        try:
                            user_obj = await interaction.client.fetch_user(int(uid))
                            nome_streamer = user_obj.display_name
                        except:
                            pass 
                else:
                    # O Administrador digitou texto puro sem nenhum número. 
                    # Criamos um ID virtual para evitar sobrescrever a conta dele próprio.
                    if is_admin(interaction.user, self.guild_id):
                        uid = f"user_{discord_input}"
                        nome_streamer = discord_input

            if not is_admin(interaction.user, self.guild_id) and uid != str(interaction.user.id):
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
                observacao=current.get("observacao", "")
            )
            await refresh_dados()

            guild = interaction.guild
            resultado = await test_streamer_live(guild_str, uid, guild)

            if "erro" in resultado:
                await interaction.followup.send(f"❌ {resultado['erro']}", ephemeral=True)
                return

            status_texto = []
            for plat, status in resultado.items():
                if plat in ["twitch", "youtube", "kick", "tiktok"]:
                    status_texto.append(f"{plat.capitalize()}: {'🟢 Ao vivo' if status else '🔴 Offline'}")

            mensagem = f"✅ Streamer **{nome_streamer}** adicionado em **{platform}**!\n\n**Status atual:**\n" + "\n".join(status_texto)

            if resultado.get("notificacao_enviada"):
                mensagem += "\n\n📢 **Notificação de live enviada!**"
            else:
                mensagem += "\n\n❌ **Nenhuma live ativa no momento.**"

            await interaction.followup.send(mensagem, ephemeral=True)

            try:
                embed = await self.parent_view.build_embed()
                await interaction.message.edit(embed=embed, view=self.parent_view)
            except:
                pass
        except Exception as e:
            logger.error(f"Erro ao adicionar streamer: {e}", exc_info=True)
            await interaction.followup.send(f"❌ Erro inesperado: {e}", ephemeral=True)

# ========= TASK DE VERIFICAÇÃO =========
@tasks.loop(minutes=1)
async def live_check_loop():
    global dados
    await refresh_dados()
    for guild_id_str in dados["lives"]["config"]:
        config = dados["lives"]["config"][guild_id_str]
        guild = bot.get_guild(int(guild_id_str))
        if not guild:
            logger.warning(f"Servidor não encontrado para guild_id {guild_id_str}")
            continue

        channel_ids_live = config.get("channel_ids_live", [])
        channel_ids_staff = config.get("channel_ids_staff", [])
        role_live_id = config.get("role_live")
        role_staff_id = config.get("role_staff")
        role_live_mention = f"<@&{role_live_id}>" if role_live_id and guild.get_role(role_live_id) else ""
        role_staff_mention = f"<@&{role_staff_id}>" if role_staff_id and guild.get_role(role_staff_id) else ""
        observacao_padrao = config.get("observacao_padrao", "")

        streamers_dict = dados["lives"]["streamers"].get(guild_id_str, {})
        status_server = dados["lives"]["status"].setdefault(guild_id_str, {})
        sessions_server = dados["lives"]["sessions"].setdefault(guild_id_str, {})

        # ---- TWITCH ----
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
                    await save_session(guild_id_str, uid, "twitch", now_utc, 0)
                    sessions_server.setdefault(uid, {})["twitch"] = {
                        "start_time": now_utc,
                        "last_milestone_hours": 0
                    }

                    nome = data.get("nome", twitch_name)
                    obs = data.get("observacao") or observacao_padrao
                    embed = discord.Embed(title="🔴 LIVE NA TWITCH", color=0x9146ff)
                    desc = f"**{nome}** está ao vivo na Twitch!"
                    if obs:
                        desc += f"\n{obs}"
                    embed.description = desc
                    embed.add_field(name="Título", value=title, inline=False)
                    embed.add_field(name="Link", value=f"https://twitch.tv/{twitch_name}", inline=False)
                    if 'thumbnail_url' in live_info:
                        thumb_url = live_info['thumbnail_url'].replace('{width}', '640').replace('{height}', '360')
                        embed.set_image(url=thumb_url)
                    embed.set_footer(text="Twitch • " + datetime.now().strftime("%H:%M"))
                    await send_to_channels(guild, channel_ids_live, role_live_mention, embed)
                else:
                    sess = sessions_server.get(uid, {}).get("twitch")
                    if sess:
                        start = sess["start_time"]
                        if start.tzinfo is None:
                            start = start.replace(tzinfo=timezone.utc)
                        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
                        current_hour = int(elapsed // 3600)
                        last_milestone = sess.get("last_milestone_hours", 0)
                        if current_hour > last_milestone and current_hour >= 1:
                            await update_milestone(guild_id_str, uid, "twitch", current_hour)
                            nome = data.get("nome", twitch_name)
                            obs = data.get("observacao") or observacao_padrao
                            embed = discord.Embed(title=f"⏰ {current_hour}h DE LIVE NA TWITCH!", color=0xffaa00)
                            desc = f"**{nome}** está ao vivo há **{current_hour} horas** consecutivas!"
                            if obs:
                                desc += f"\n{obs}"
                            embed.description = desc
                            embed.add_field(name="Título", value=title, inline=False)
                            embed.add_field(name="Link", value=f"https://twitch.tv/{twitch_name}", inline=False)
                            embed.set_footer(text="Twitch • " + datetime.now().strftime("%H:%M"))
                            await send_to_channels(guild, channel_ids_staff, role_staff_mention, embed)
            else:
                sess = sessions_server.get(uid, {}).get("twitch")
                if sess:
                    start = sess["start_time"]
                    if start.tzinfo is None:
                        start = start.replace(tzinfo=timezone.utc)
                    duration = (datetime.now(timezone.utc) - start).total_seconds()
                    if duration > 0:
                        await add_streamer_hours(guild_id_str, uid, duration)
                    await delete_session(guild_id_str, uid, "twitch")
                    if uid in sessions_server and "twitch" in sessions_server[uid]:
                        del sessions_server[uid]["twitch"]
                    dados["lives"]["last_notified"][f"twitch_{uid}"] = "offline"
                    await save_last_notified(f"twitch_{uid}", "offline")

        # ---- YOUTUBE ----
        yt_identifiers = [data.get("youtube") for data in streamers_dict.values() if data.get("youtube")]
        lives = await check_youtube_lives(yt_identifiers)
        for uid, data in streamers_dict.items():
            yt_identifier = data.get("youtube")
            if not yt_identifier:
                await save_status(guild_id_str, uid, "youtube", False)
                continue
            is_live = yt_identifier in lives
            await save_status(guild_id_str, uid, "youtube", is_live)
            status_server.setdefault(uid, {})["youtube"] = is_live

            if is_live:
                video = lives[yt_identifier]
                title = video['snippet']['title']
                video_id = video["id"]["videoId"]
                last_key = f"yt_{uid}"
                last_id = dados["lives"]["last_notified"].get(last_key)

                if last_id != video_id:
                    dados["lives"]["last_notified"][last_key] = video_id
                    await save_last_notified(last_key, video_id)
                    now_utc = datetime.now(timezone.utc)
                    await save_session(guild_id_str, uid, "youtube", now_utc, 0)
                    sessions_server.setdefault(uid, {})["youtube"] = {
                        "start_time": now_utc,
                        "last_milestone_hours": 0
                    }

                    nome = data.get("nome", yt_identifier)
                    obs = data.get("observacao") or observacao_padrao
                    embed = discord.Embed(title="🔴 LIVE NO YOUTUBE", color=0xff0000)
                    desc = f"**{nome}** está ao vivo no YouTube!"
                    if obs:
                        desc += f"\n{obs}"
                    embed.description = desc
                    embed.add_field(name="Título", value=title, inline=False)
                    embed.add_field(name="Link", value=f"https://youtube.com/watch?v={video_id}", inline=False)
                    embed.set_footer(text="YouTube • " + datetime.now().strftime("%H:%M"))
                    await send_to_channels(guild, channel_ids_live, role_live_mention, embed)
                else:
                    sess = sessions_server.get(uid, {}).get("youtube")
                    if sess:
                        start = sess["start_time"]
                        if start.tzinfo is None:
                            start = start.replace(tzinfo=timezone.utc)
                        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
                        current_hour = int(elapsed // 3600)
                        last_milestone = sess.get("last_milestone_hours", 0)
                        if current_hour > last_milestone and current_hour >= 1:
                            await update_milestone(guild_id_str, uid, "youtube", current_hour)
                            nome = data.get("nome", yt_identifier)
                            obs = data.get("observacao") or observacao_padrao
                            embed = discord.Embed(title=f"⏰ {current_hour}h DE LIVE NO YOUTUBE!", color=0xffaa00)
                            desc = f"**{nome}** está ao vivo há **{current_hour} horas** consecutivas!"
                            if obs:
                                desc += f"\n{obs}"
                            embed.description = desc
                            embed.add_field(name="Título", value=title, inline=False)
                            embed.add_field(name="Link", value=f"https://youtube.com/watch?v={video_id}", inline=False)
                            embed.set_footer(text="YouTube • " + datetime.now().strftime("%H:%M"))
                            await send_to_channels(guild, channel_ids_staff, role_staff_mention, embed)
            else:
                sess = sessions_server.get(uid, {}).get("youtube")
                if sess:
                    start = sess["start_time"]
                    if start.tzinfo is None:
                        start = start.replace(tzinfo=timezone.utc)
                    duration = (datetime.now(timezone.utc) - start).total_seconds()
                    if duration > 0:
                        await add_streamer_hours(guild_id_str, uid, duration)
                    await delete_session(guild_id_str, uid, "youtube")
                    if uid in sessions_server and "youtube" in sessions_server[uid]:
                        del sessions_server[uid]["youtube"]
                    dados["lives"]["last_notified"][f"yt_{uid}"] = "offline"
                    await save_last_notified(f"yt_{uid}", "offline")

        # ---- KICK ----
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
                    await save_session(guild_id_str, uid, "kick", now_utc, 0)
                    sessions_server.setdefault(uid, {})["kick"] = {
                        "start_time": now_utc,
                        "last_milestone_hours": 0
                    }

                    nome = data.get("nome", kick_name)
                    obs = data.get("observacao") or observacao_padrao
                    embed = discord.Embed(title="🔴 LIVE NA KICK", color=0x53fc18)
                    desc = f"**{nome}** está ao vivo na Kick!"
                    if obs:
                        desc += f"\n{obs}"
                    embed.description = desc
                    embed.add_field(name="Título", value=title, inline=False)
                    embed.add_field(name="Espectadores", value=stream_info['viewer_count'], inline=False)
                    embed.add_field(name="Link", value=f"https://kick.com/{kick_name}", inline=False)
                    embed.set_footer(text="Kick • " + datetime.now().strftime("%H:%M"))
                    await send_to_channels(guild, channel_ids_live, role_live_mention, embed)
                else:
                    sess = sessions_server.get(uid, {}).get("kick")
                    if sess:
                        start = sess["start_time"]
                        if start.tzinfo is None:
                            start = start.replace(tzinfo=timezone.utc)
                        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
                        current_hour = int(elapsed // 3600)
                        last_milestone = sess.get("last_milestone_hours", 0)
                        if current_hour > last_milestone and current_hour >= 1:
                            await update_milestone(guild_id_str, uid, "kick", current_hour)
                            nome = data.get("nome", kick_name)
                            obs = data.get("observacao") or observacao_padrao
                            embed = discord.Embed(title=f"⏰ {current_hour}h DE LIVE NA KICK!", color=0xffaa00)
                            desc = f"**{nome}** está ao vivo há **{current_hour} horas** consecutivas!"
                            if obs:
                                desc += f"\n{obs}"
                            embed.description = desc
                            embed.add_field(name="Título", value=title, inline=False)
                            embed.add_field(name="Link", value=f"https://kick.com/{kick_name}", inline=False)
                            embed.set_footer(text="Kick • " + datetime.now().strftime("%H:%M"))
                            await send_to_channels(guild, channel_ids_staff, role_staff_mention, embed)
            else:
                # Usa last_notified para verificar se estava ao vivo antes.
                # status_server já foi atualizado para False acima, por isso não serve aqui.
                if dados["lives"]["last_notified"].get(f"kick_{uid}") == "live":
                    dados["lives"]["last_notified"][f"kick_{uid}"] = "offline"
                    await save_last_notified(f"kick_{uid}", "offline")
                sess = sessions_server.get(uid, {}).get("kick")
                if sess:
                    start = sess["start_time"]
                    if start.tzinfo is None:
                        start = start.replace(tzinfo=timezone.utc)
                    duration = (datetime.now(timezone.utc) - start).total_seconds()
                    if duration > 0:
                        await add_streamer_hours(guild_id_str, uid, duration)
                    await delete_session(guild_id_str, uid, "kick")
                    if uid in sessions_server and "kick" in sessions_server[uid]:
                        del sessions_server[uid]["kick"]

        # ---- TIKTOK ----
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
                    await save_session(guild_id_str, uid, "tiktok", now_utc, 0)
                    sessions_server.setdefault(uid, {})["tiktok"] = {
                        "start_time": now_utc,
                        "last_milestone_hours": 0
                    }

                    nome = data.get("nome", tiktok_name)
                    obs = data.get("observacao") or observacao_padrao
                    embed = discord.Embed(title="🔴 LIVE NO TIKTOK", color=0xff0050, url=live_info["url"])
                    desc = f"**{nome}** está ao vivo no TikTok!"
                    if obs:
                        desc += f"\n{obs}"
                    embed.description = desc
                    embed.add_field(name="Título", value=title, inline=False)
                    embed.set_footer(text="TikTok • " + datetime.now().strftime("%H:%M"))
                    if live_info.get("thumbnail"):
                        embed.set_image(url=live_info["thumbnail"])
                    view = View(timeout=None)
                    view.add_item(Button(label="Assistir Agora", style=discord.ButtonStyle.link, url=live_info["url"]))
                    await send_to_channels(guild, channel_ids_live, role_live_mention, embed, view=view)
                else:
                    sess = sessions_server.get(uid, {}).get("tiktok")
                    if sess:
                        start = sess["start_time"]
                        if start.tzinfo is None:
                            start = start.replace(tzinfo=timezone.utc)
                        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
                        current_hour = int(elapsed // 3600)
                        last_milestone = sess.get("last_milestone_hours", 0)
                        if current_hour > last_milestone and current_hour >= 1:
                            await update_milestone(guild_id_str, uid, "tiktok", current_hour)
                            nome = data.get("nome", tiktok_name)
                            obs = data.get("observacao") or observacao_padrao
                            embed = discord.Embed(title=f"⏰ {current_hour}h DE LIVE NO TIKTOK!", color=0xffaa00)
                            desc = f"**{nome}** está ao vivo há **{current_hour} horas** consecutivas!"
                            if obs:
                                desc += f"\n{obs}"
                            embed.description = desc
                            embed.add_field(name="Título", value=title, inline=False)
                            embed.set_footer(text="TikTok • " + datetime.now().strftime("%H:%M"))
                            view = View(timeout=None)
                            view.add_item(Button(label="Assistir Agora", style=discord.ButtonStyle.link, url=live_info["url"]))
                            await send_to_channels(guild, channel_ids_staff, role_staff_mention, embed, view=view)
            else:
                # Usa last_notified para verificar se estava ao vivo antes.
                # status_server já foi atualizado para False acima, por isso não serve aqui.
                if dados["lives"]["last_notified"].get(f"tiktok_{uid}") == "live":
                    dados["lives"]["last_notified"][f"tiktok_{uid}"] = "offline"
                    await save_last_notified(f"tiktok_{uid}", "offline")
                sess = sessions_server.get(uid, {}).get("tiktok")
                if sess:
                    start = sess["start_time"]
                    if start.tzinfo is None:
                        start = start.replace(tzinfo=timezone.utc)
                    duration = (datetime.now(timezone.utc) - start).total_seconds()
                    if duration > 0:
                        await add_streamer_hours(guild_id_str, uid, duration)
                    await delete_session(guild_id_str, uid, "tiktok")
                    if uid in sessions_server and "tiktok" in sessions_server[uid]:
                        del sessions_server[uid]["tiktok"]

        # ----- ATUALIZAR PAINEL -----
        painel_channel_id = config.get("painel_channel_id")
        if painel_channel_id:
            painel_channel = guild.get_channel(painel_channel_id)
            if painel_channel:
                try:
                    async for msg in painel_channel.history(limit=10):
                        if msg.author == bot.user:
                            view = LiveConfigView(guild.id, page=0)
                            embed = await view.build_embed()
                            await msg.edit(embed=embed, view=view)
                            break
                    else:
                        view = LiveConfigView(guild.id, page=0)
                        embed = await view.build_embed()
                        await painel_channel.send(embed=embed, view=view)
                except Exception as e:
                    logger.error(f"Erro ao atualizar painel no canal {painel_channel_id}: {e}")

@live_check_loop.before_loop
async def before_live_check():
    await bot.wait_until_ready()

# ========= COMANDOS (apenas slash) =========
@bot.tree.command(name="setpainel", description="Define o canal onde o painel de lives será exibido.")
@app_commands.default_permissions(administrator=True)
async def setpainel(interaction: discord.Interaction, canal: discord.TextChannel):
    guild_id = str(interaction.guild_id)
    config = dados["lives"]["config"].get(guild_id, {})
    config["painel_channel_id"] = canal.id
    await save_config(
        guild_id,
        config.get("channel_ids_live", []),
        config.get("channel_ids_staff", []),
        config.get("role_live"),
        config.get("role_staff"),
        config.get("admin_role"),
        config.get("platforms", {"twitch": True, "youtube": True, "kick": True, "tiktok": True}),
        canal.id,
        config.get("observacao_padrao", "")
    )
    await refresh_dados()
    view = LiveConfigView(interaction.guild_id, page=0)
    embed = await view.build_embed()
    await canal.send(embed=embed, view=view)
    await interaction.response.send_message(f"✅ Painel configurado em {canal.mention}.", ephemeral=True)

# ========= EVENTO ON_READY =========
@bot.event
async def on_ready():
    await init_db()
    global dados
    dados = await load_all_data()
    logger.info(f"Bot de Lives online: {bot.user}")

    try:
        synced = await bot.tree.sync()
        logger.info(f"Comandos slash sincronizados globalmente: {len(synced)}")
    except Exception as e:
        logger.error(f"Erro ao sincronizar comandos: {e}")

    if not live_check_loop.is_running():
        live_check_loop.start()

# ========= INICIALIZAÇÃO =========
if __name__ == "__main__":
    bot.run(TOKEN)
