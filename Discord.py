import discord
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
from TikTokLive import TikTokLiveClient
import logging

# ========= CONFIGURAÇÕES DE LOG =========
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ========= CONFIGURAÇÕES =========
TOKEN = os.getenv("DISCORD_TOKEN_LIVE")
if not TOKEN:
    logger.error("Token do Discord (LIVES) não encontrado. Defina DISCORD_TOKEN_LIVE")
    sys.exit(1)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.error("DATABASE_URL não encontrada")
    sys.exit(1)

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# ========= CRIAÇÃO DO BOT =========
class LiveBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=discord.Intents.all())

    async def setup_hook(self):
        self.add_view(LiveConfigView())
        self.add_view(StreamerManagementView())
        logger.info("Views persistentes registradas.")

bot = LiveBot()

# ========= BANCO DE DADOS =========
db_pool = None

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_bot_subscriptions (
                guild_id VARCHAR(50) PRIMARY KEY,
                user_email VARCHAR(255) NOT NULL,
                status VARCHAR(20) DEFAULT 'pending_activation',
                expires_at TIMESTAMP NOT NULL,
                plan_days INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_bot_settings (
                guild_id VARCHAR(50) PRIMARY KEY REFERENCES live_bot_subscriptions(guild_id) ON DELETE CASCADE,
                channel_ids TEXT,
                role_id VARCHAR(50),
                staff_channel_ids TEXT DEFAULT '[]',
                staff_role_id VARCHAR(50),
                target_guild_id VARCHAR(50),
                platforms JSONB,
                custom_message TEXT,
                cargo_admin_lives_id VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_streamers (
                id SERIAL PRIMARY KEY,
                guild_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                nome TEXT NOT NULL,
                twitch TEXT,
                youtube TEXT,
                kick TEXT,
                tiktok TEXT,
                observacao TEXT,
                UNIQUE(guild_id, user_id)
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
                start_time TIMESTAMP,
                three_hour_notified BOOLEAN,
                PRIMARY KEY (guild_id, user_id, platform)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_streamer_stats (
                guild_id TEXT,
                user_id TEXT,
                total_seconds INTEGER DEFAULT 0,
                last_notified_hour INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, user_id)
            )
        """)
    logger.info("Banco de dados do bot de lives inicializado.")

# ========= CONTROLE DE ASSINATURA =========
active_live_guilds = {}

async def update_active_live_guilds():
    global active_live_guilds
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT guild_id, expires_at FROM live_bot_subscriptions WHERE status = 'active'")
        now = datetime.now(timezone.utc)
        new_active = {}
        for row in rows:
            expires = row['expires_at'].replace(tzinfo=timezone.utc)
            if expires > now:
                new_active[int(row['guild_id'])] = True
        active_live_guilds = new_active
        logger.info(f"Assinaturas ativas (lives): {len(active_live_guilds)}")
    except Exception as e:
        logger.error(f"Erro ao atualizar assinaturas live: {e}")

def is_live_guild_active(gid: int) -> bool:
    return active_live_guilds.get(gid, False)

async def load_live_settings(guild_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT channel_ids, role_id, staff_channel_ids, staff_role_id, target_guild_id, platforms, custom_message, cargo_admin_lives_id FROM live_bot_settings WHERE guild_id = $1",
            str(guild_id)
        )
    if row:
        channel_ids = json.loads(row['channel_ids']) if row['channel_ids'] else []
        staff_channel_ids = json.loads(row['staff_channel_ids']) if row['staff_channel_ids'] else []
        role_id = int(row['role_id']) if row['role_id'] else None
        staff_role_id = int(row['staff_role_id']) if row['staff_role_id'] else None
        target_guild_id = int(row['target_guild_id']) if row['target_guild_id'] else None
        platforms = json.loads(row['platforms']) if row['platforms'] else {"twitch": True, "youtube": True, "kick": True, "tiktok": True}
        custom_message = row['custom_message'] or ""
        cargo_admin_lives_id = row['cargo_admin_lives_id'] or None
        return {
            "channel_ids": channel_ids,
            "role_id": role_id,
            "staff_channel_ids": staff_channel_ids,
            "staff_role_id": staff_role_id,
            "target_guild_id": target_guild_id,
            "platforms": platforms,
            "custom_message": custom_message,
            "cargo_admin_lives_id": cargo_admin_lives_id
        }
    return None

# ========= DADOS EM MEMÓRIA =========
dados = {
    "lives": {
        "streamers": {},
        "last_notified": {},
        "status": {},
        "sessions": {}
    }
}

async def load_all_data():
    dados["lives"]["streamers"] = {}
    dados["lives"]["last_notified"] = {}
    dados["lives"]["status"] = {}
    dados["lives"]["sessions"] = {}

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT guild_id, user_id, nome, twitch, youtube, kick, tiktok, observacao FROM live_streamers")
        for r in rows:
            gid = r["guild_id"]
            uid = r["user_id"]
            if gid not in dados["lives"]["streamers"]:
                dados["lives"]["streamers"][gid] = {}
            dados["lives"]["streamers"][gid][uid] = {
                "nome": r["nome"],
                "twitch": r["twitch"],
                "youtube": r["youtube"],
                "kick": r["kick"],
                "tiktok": r["tiktok"],
                "observacao": r["observacao"]
            }
        rows = await conn.fetch("SELECT key, value FROM live_last_notified")
        for r in rows:
            dados["lives"]["last_notified"][r["key"]] = r["value"]
        rows = await conn.fetch("SELECT guild_id, user_id, platform, is_live FROM live_status")
        for r in rows:
            gid = r["guild_id"]
            uid = r["user_id"]
            plat = r["platform"]
            if gid not in dados["lives"]["status"]:
                dados["lives"]["status"][gid] = {}
            if uid not in dados["lives"]["status"][gid]:
                dados["lives"]["status"][gid][uid] = {}
            dados["lives"]["status"][gid][uid][plat] = r["is_live"]
        rows = await conn.fetch("SELECT guild_id, user_id, platform, start_time, three_hour_notified FROM live_sessions")
        for r in rows:
            gid = r["guild_id"]
            uid = r["user_id"]
            plat = r["platform"]
            if gid not in dados["lives"]["sessions"]:
                dados["lives"]["sessions"][gid] = {}
            if uid not in dados["lives"]["sessions"][gid]:
                dados["lives"]["sessions"][gid][uid] = {}
            dados["lives"]["sessions"][gid][uid][plat] = {
                "start_time": r["start_time"],
                "three_hour_notified": r["three_hour_notified"]
            }
    logger.info("Dados de lives carregados do banco.")

async def save_streamer(guild_id, user_id, nome, twitch, youtube, kick, tiktok, observacao):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO live_streamers (guild_id, user_id, nome, twitch, youtube, kick, tiktok, observacao)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
        await conn.execute("DELETE FROM live_streamer_stats WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)

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
    # Converte para naive para compatibilidade com TIMESTAMP
    start_time_naive = start_time.replace(tzinfo=None) if start_time.tzinfo else start_time
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO live_sessions (guild_id, user_id, platform, start_time, three_hour_notified)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (guild_id, user_id, platform) DO UPDATE SET
                start_time = EXCLUDED.start_time,
                three_hour_notified = EXCLUDED.three_hour_notified
        """, guild_id, user_id, platform, start_time_naive, three_hour_notified)

async def delete_session(guild_id, user_id, platform):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM live_sessions WHERE guild_id=$1 AND user_id=$2 AND platform=$3",
                           guild_id, user_id, platform)

# ========= FUNÇÕES AUXILIARES =========
def is_admin(member):
    return member.guild_permissions.administrator

async def is_admin_by_role(member, guild_id):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT cargo_admin_lives_id FROM live_bot_settings WHERE guild_id = $1",
            str(guild_id)
        )
    if row and row['cargo_admin_lives_id']:
        role_id = int(row['cargo_admin_lives_id'])
        role = member.guild.get_role(role_id)
        if role and role in member.roles:
            return True
    return False

async def is_admin_or_owner(member, guild_id):
    return await is_admin_by_role(member, guild_id)

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

# ========= NOTIFICAÇÃO DE LIVE =========
async def send_live_notification(guild, channel_ids, role_mention, custom_message, platform, streamer_data, stream_info, uid):
    nome_streamer = streamer_data.get("nome", uid)
    observacao = streamer_data.get("observacao", "")
    
    colors = {
        "twitch": 0x9146ff,
        "youtube": 0xff0000,
        "kick": 0x53fc18,
        "tiktok": 0xff0050
    }
    embed = discord.Embed(title=f"🔴 LIVE NA {platform.upper()}", color=colors.get(platform, 0x99aab5))
    desc = f"**{nome_streamer}** está ao vivo na {platform.capitalize()}!"
    if observacao:
        desc += f"\n{observacao}"
    embed.description = desc
    
    if platform == "twitch":
        embed.add_field(name="Título", value=stream_info.get("title", ""), inline=False)
        embed.add_field(name="Link", value=f"https://twitch.tv/{streamer_data.get('twitch')}", inline=False)
        if 'thumbnail_url' in stream_info:
            thumb_url = stream_info['thumbnail_url'].replace('{width}', '640').replace('{height}', '360')
            embed.set_image(url=thumb_url)
    elif platform == "youtube":
        video_id = stream_info.get("id", {}).get("videoId")
        embed.add_field(name="Título", value=stream_info.get("snippet", {}).get("title", ""), inline=False)
        embed.add_field(name="Link", value=f"https://youtube.com/watch?v={video_id}", inline=False)
    elif platform == "kick":
        embed.add_field(name="Título", value=stream_info.get("title", ""), inline=False)
        embed.add_field(name="Espectadores", value=stream_info.get('viewer_count', 0), inline=False)
        embed.add_field(name="Link", value=f"https://kick.com/{streamer_data.get('kick')}", inline=False)
    elif platform == "tiktok":
        embed.add_field(name="Título", value=stream_info.get("title", ""), inline=False)
        embed.add_field(name="Link", value=stream_info.get("url", ""), inline=False)
        if stream_info.get("thumbnail"):
            embed.set_image(url=stream_info["thumbnail"])
    
    view = None
    if platform == "tiktok" and stream_info.get("url"):
        view = View(timeout=None)
        view.add_item(Button(label="Assistir Agora", style=discord.ButtonStyle.link, url=stream_info["url"]))
    
    await send_to_channels(guild, channel_ids, role_mention, embed, view=view, custom_message=custom_message)

# ========= NOTIFICAÇÃO DE MARCO DE HORAS (STAFF) =========
async def send_hours_notification(guild, staff_channel_ids, staff_role_mention, custom_message, streamer_data, hours, uid):
    nome_streamer = streamer_data.get("nome", uid)
    observacao = streamer_data.get("observacao", "")
    
    embed = discord.Embed(title="⏱️ MARCO DE HORAS ATINGIDO!", color=0x3498db)
    desc = f"🎉 **{nome_streamer}** acaba de completar **{hours} horas** de transmissão acumuladas!"
    if observacao:
        desc += f"\n{observacao}"
    embed.description = desc
    embed.set_footer(text="NEXZY • Monitoramento de Lives", icon_url="https://nexzystore.com.br/logo.png")
    
    if streamer_data.get("twitch"):
        embed.add_field(name="Twitch", value=f"https://twitch.tv/{streamer_data.get('twitch')}", inline=True)
    if streamer_data.get("youtube"):
        embed.add_field(name="YouTube", value=f"https://youtube.com/@{streamer_data.get('youtube')}", inline=True)
    if streamer_data.get("kick"):
        embed.add_field(name="Kick", value=f"https://kick.com/{streamer_data.get('kick')}", inline=True)
    if streamer_data.get("tiktok"):
        embed.add_field(name="TikTok", value=f"https://tiktok.com/@{streamer_data.get('tiktok')}", inline=True)
    
    await send_to_channels(guild, staff_channel_ids, staff_role_mention, embed, custom_message=custom_message)

# ========= VERIFICAÇÃO DE LIVES =========
ua = UserAgent()

def get_headers():
    return {
        "User-Agent": ua.random,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }

twitch_token = None
twitch_token_expiry = 0

async def get_twitch_token():
    global twitch_token, twitch_token_expiry
    now_ts = datetime.now(timezone.utc).timestamp()
    if twitch_token and now_ts < twitch_token_expiry:
        return twitch_token
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        logger.error("TWITCH_CLIENT_ID ou TWITCH_CLIENT_SECRET não definidos")
        return None
    logger.info("Obtendo novo token Twitch...")
    async with aiohttp.ClientSession() as session:
        async with session.post("https://id.twitch.tv/oauth2/token",
                                params={"client_id": TWITCH_CLIENT_ID,
                                        "client_secret": TWITCH_CLIENT_SECRET,
                                        "grant_type": "client_credentials"}) as resp:
            if resp.status == 200:
                data = await resp.json()
                twitch_token = data["access_token"]
                twitch_token_expiry = now_ts + data["expires_in"] - 60
                logger.info(f"Token Twitch obtido com sucesso (expira em {data['expires_in']}s)")
                return twitch_token
            else:
                error_text = await resp.text()
                logger.error(f"Falha ao obter token Twitch: status {resp.status}, resposta: {error_text[:200]}")
                return None

async def check_twitch_lives(usernames):
    token = await get_twitch_token()
    if not token or not usernames:
        return {}
    headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"}
    result = {}
    logger.info(f"Verificando Twitch para {len(usernames)} usuários")
    for i in range(0, len(usernames), 100):
        batch = usernames[i:i+100]
        url = "https://api.twitch.tv/helix/streams?user_login=" + "&user_login=".join(batch)
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for s in data.get("data", []):
                        result[s["user_login"].lower()] = s
                    logger.info(f"Twitch: {len(data.get('data', []))} lives encontradas no batch")
                else:
                    error_text = await resp.text()
                    logger.warning(f"Twitch API retornou {resp.status} (batch), resposta: {error_text[:200]}")
    return result

async def check_youtube_lives(channel_ids):
    if not YOUTUBE_API_KEY or not channel_ids:
        if not YOUTUBE_API_KEY:
            logger.warning("YOUTUBE_API_KEY não definida")
        return {}
    live_data = {}
    logger.info(f"Verificando YouTube para {len(channel_ids)} canais")
    for ch_id in channel_ids:
        if not ch_id:
            continue
        url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={ch_id}&eventType=live&type=video&key={YOUTUBE_API_KEY}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("items"):
                        live_data[ch_id] = data["items"][0]
                        logger.info(f"YouTube: live encontrada para canal {ch_id}")
                    else:
                        logger.debug(f"YouTube: sem live para canal {ch_id}")
                else:
                    error_text = await resp.text()
                    logger.warning(f"YouTube API retornou {resp.status} para {ch_id}, resposta: {error_text[:200]}")
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
            logger.warning(f"Kick retornou {resp.status_code} para {username}")
            return False, None
    except Exception as e:
        logger.error(f"Erro ao verificar Kick para {username}: {e}")
        return False, None

async def check_kick_live(username):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, check_kick_live_sync, username)

# ===== TIKTOK - USA A BIBLIOTECA TikTokLive =====
async def check_tiktok_live(username: str):
    """
    Verifica se um usuário do TikTok está em live usando a biblioteca TikTokLive.
    """
    try:
        client = TikTokLiveClient(unique_id=f"@{username}")
        is_live = await client.is_live()
        if is_live:
            logger.info(f"TikTok: live detectada para @{username}")
            return {
                "title": "Live no TikTok",
                "thumbnail": None,
                "url": f"https://www.tiktok.com/@{username}/live"
            }
        else:
            logger.debug(f"TikTok: sem live para @{username}")
            return None
    except Exception as e:
        logger.error(f"Erro ao verificar TikTok para @{username}: {e}")
        return None

# ========= NOTIFICAÇÕES EM CANAIS =========
async def send_to_channels(guild, channel_ids, role_mention, embed, view=None, custom_message=""):
    if not channel_ids:
        return
    for cid in channel_ids:
        try:
            cid_int = int(cid)
            canal = guild.get_channel(cid_int)
        except (ValueError, TypeError):
            continue
        if canal:
            try:
                content = role_mention
                if custom_message:
                    content += f"\n{custom_message}"
                if view:
                    await canal.send(content=content, embed=embed, view=view)
                else:
                    await canal.send(content=content, embed=embed)
            except Exception as e:
                logger.error(f"Erro ao enviar para canal {cid_int}: {e}")

# ========= TASK DE VERIFICAÇÃO =========
@tasks.loop(minutes=1)
async def live_check_loop():
    logger.debug("Executando live_check_loop...")
    await update_active_live_guilds()
    for guild_id in list(active_live_guilds.keys()):
        if not is_live_guild_active(guild_id):
            continue
        config = await load_live_settings(guild_id)
        if not config:
            continue
        target_guild_id = config.get("target_guild_id")
        if target_guild_id:
            try:
                guild = bot.get_guild(int(target_guild_id))
            except (ValueError, TypeError):
                guild = bot.get_guild(guild_id)
        else:
            guild = bot.get_guild(guild_id)
        if not guild:
            logger.warning(f"Servidor não encontrado para guild_id {guild_id}")
            continue

        plataformas = config.get("platforms", {"twitch": True, "youtube": True, "kick": True, "tiktok": True})
        channel_ids = config.get("channel_ids", [])
        staff_channel_ids = config.get("staff_channel_ids", [])
        role_id = config.get("role_id")
        staff_role_id = config.get("staff_role_id")
        role_mention = f"<@&{role_id}>" if role_id and guild.get_role(role_id) else ""
        staff_role_mention = f"<@&{staff_role_id}>" if staff_role_id and guild.get_role(staff_role_id) else ""
        custom_message = config.get("custom_message", "")

        guild_id_str = str(guild_id)
        streamers_dict = dados["lives"]["streamers"].get(guild_id_str, {})
        status_server = dados["lives"]["status"].setdefault(guild_id_str, {})
        sessions_server = dados["lives"]["sessions"].setdefault(guild_id_str, {})

        # ===== TWITCH =====
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
                        now_utc = datetime.now()
                        await save_session(guild_id_str, uid, "twitch", now_utc, False)
                        await send_live_notification(guild, channel_ids, role_mention, custom_message, "twitch", data, live_info, uid)
                else:
                    if uid in sessions_server and "twitch" in sessions_server[uid]:
                        await delete_session(guild_id_str, uid, "twitch")

        # ===== YOUTUBE =====
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
                        now_utc = datetime.now()
                        await save_session(guild_id_str, uid, "youtube", now_utc, False)
                        await send_live_notification(guild, channel_ids, role_mention, custom_message, "youtube", data, video, uid)
                else:
                    if uid in sessions_server and "youtube" in sessions_server[uid]:
                        await delete_session(guild_id_str, uid, "youtube")

        # ===== KICK =====
        if plataformas.get("kick"):
            for uid, data in streamers_dict.items():
                kick_name = data.get("kick")
                if not kick_name:
                    await save_status(guild_id_str, uid, "kick", False)
                    continue
                is_live, stream_info = await check_kick_live(kick_name)
                await save_status(guild_id_str, uid, "kick", is_live)
                status_server.setdefault(uid, {})["kick"] = is_live
                
                last_key = f"kick_{uid}"
                last_status = dados["lives"]["last_notified"].get(last_key)

                if is_live:
                    title = stream_info.get("title", "")
                    if last_status != "live":
                        dados["lives"]["last_notified"][last_key] = "live"
                        await save_last_notified(last_key, "live")
                        now_utc = datetime.now()
                        await save_session(guild_id_str, uid, "kick", now_utc, False)
                        await send_live_notification(guild, channel_ids, role_mention, custom_message, "kick", data, stream_info, uid)
                else:
                    if last_status == "live":
                        dados["lives"]["last_notified"][f"kick_{uid}"] = "offline"
                        await save_last_notified(f"kick_{uid}", "offline")
                    if uid in sessions_server and "kick" in sessions_server[uid]:
                        await delete_session(guild_id_str, uid, "kick")

        # ===== TIKTOK - VERIFICAÇÃO EM PARALELO =====
        if plataformas.get("tiktok"):
            tiktok_streamers = {uid: data for uid, data in streamers_dict.items() if data.get("tiktok")}
            if tiktok_streamers:
                # Cria tarefas para verificar todos em paralelo
                tasks = []
                for uid, data in tiktok_streamers.items():
                    tiktok_name = data.get("tiktok")
                    tasks.append(check_tiktok_live(tiktok_name))
                
                # Aguarda todas as verificações
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Processa os resultados
                for idx, (uid, data) in enumerate(tiktok_streamers.items()):
                    tiktok_name = data.get("tiktok")
                    live_info = results[idx]
                    is_live = live_info is not None and not isinstance(live_info, Exception)
                    
                    await save_status(guild_id_str, uid, "tiktok", is_live)
                    status_server.setdefault(uid, {})["tiktok"] = is_live
                    
                    last_key = f"tiktok_{uid}"
                    last_status = dados["lives"]["last_notified"].get(last_key)

                    if is_live:
                        title = live_info.get("title", "")
                        if last_status != "live":
                            dados["lives"]["last_notified"][last_key] = "live"
                            await save_last_notified(last_key, "live")
                            now_utc = datetime.now()
                            await save_session(guild_id_str, uid, "tiktok", now_utc, False)
                            await send_live_notification(guild, channel_ids, role_mention, custom_message, "tiktok", data, live_info, uid)
                    else:
                        if last_status == "live":
                            dados["lives"]["last_notified"][f"tiktok_{uid}"] = "offline"
                            await save_last_notified(f"tiktok_{uid}", "offline")
                        if uid in sessions_server and "tiktok" in sessions_server[uid]:
                            await delete_session(guild_id_str, uid, "tiktok")

        # ===== SISTEMA DE HORAS =====
        for uid, data in streamers_dict.items():
            online_platforms = [p for p in ["twitch", "youtube", "kick", "tiktok"] if status_server.get(uid, {}).get(p, False)]
            if online_platforms:
                async with db_pool.acquire() as conn:
                    row = await conn.fetchrow("""
                        INSERT INTO live_streamer_stats (guild_id, user_id, total_seconds, last_notified_hour)
                        VALUES ($1, $2, 60, 0)
                        ON CONFLICT (guild_id, user_id) DO UPDATE SET
                            total_seconds = live_streamer_stats.total_seconds + 60
                        RETURNING total_seconds, last_notified_hour
                    """, guild_id_str, uid)
                    if row:
                        total_sec = row['total_seconds']
                        last_hour = row['last_notified_hour']
                        current_hour = total_sec // 3600
                        if current_hour > last_hour and current_hour > 0:
                            await conn.execute("UPDATE live_streamer_stats SET last_notified_hour = $1 WHERE guild_id = $2 AND user_id = $3", current_hour, guild_id_str, uid)
                            if staff_channel_ids:
                                await send_hours_notification(guild, staff_channel_ids, staff_role_mention, custom_message, data, current_hour, uid)
                            elif channel_ids:
                                await send_hours_notification(guild, channel_ids, role_mention, custom_message, data, current_hour, uid)

@live_check_loop.before_loop
async def before_live_check():
    await bot.wait_until_ready()

# ========= PAINEL DE CONFIGURAÇÃO =========
class LiveConfigView(View):
    def __init__(self):
        super().__init__(timeout=None)

    async def get_config(self, guild_id):
        config = await load_live_settings(guild_id)
        if not config:
            return {
                "channel_ids": [],
                "role_id": None,
                "staff_channel_ids": [],
                "staff_role_id": None,
                "target_guild_id": None,
                "platforms": {"twitch": True, "youtube": True, "kick": True, "tiktok": True},
                "custom_message": "",
                "cargo_admin_lives_id": None
            }
        return config

    async def build_embed(self, guild_id):
        config = await self.get_config(guild_id)
        channel_ids = config['channel_ids']
        staff_channel_ids = config['staff_channel_ids']
        canais_txt = "\n".join(f"<#{cid}>" for cid in channel_ids) if channel_ids else "Não definido"
        staff_canais_txt = "\n".join(f"<#{cid}>" for cid in staff_channel_ids) if staff_channel_ids else "Não definido"
        cargo_info = f"<@&{config['role_id']}>" if config['role_id'] else "Não definido"
        staff_cargo_info = f"<@&{config['staff_role_id']}>" if config['staff_role_id'] else "Não definido"
        cargo_admin_info = f"<@&{config['cargo_admin_lives_id']}>" if config.get('cargo_admin_lives_id') else "Não definido"
        target_info = f"Servidor: {config['target_guild_id']}" if config.get('target_guild_id') else "Mesmo servidor"
        plats = config['platforms']
        embed = discord.Embed(title="🔔 NOTIFICAÇÃO DE LIVES", color=0x99aab5)
        embed.add_field(name="📢 Canais (Lives)", value=canais_txt, inline=False)
        embed.add_field(name="🛡️ Canais da Staff (Horas)", value=staff_canais_txt, inline=False)
        embed.add_field(name="👥 Cargo (ping)", value=cargo_info, inline=False)
        embed.add_field(name="👥 Cargo Staff", value=staff_cargo_info, inline=False)
        embed.add_field(name="🛡️ Cargo Admin (streamers)", value=cargo_admin_info, inline=False)
        embed.add_field(name="🎯 Destino", value=target_info, inline=False)
        status = "\n".join([
            f"Twitch: {'✅ Ativado' if plats.get('twitch') else '❌ Desativado'}",
            f"YouTube: {'✅ Ativado' if plats.get('youtube') else '❌ Desativado'}",
            f"Kick: {'✅ Ativado' if plats.get('kick') else '❌ Desativado'}",
            f"TikTok: {'✅ Ativado' if plats.get('tiktok') else '❌ Desativado'}"
        ])
        embed.add_field(name="🎮 Plataformas Monitoradas", value=status, inline=False)
        guild_id_str = str(guild_id)
        streamers = dados["lives"]["streamers"].get(guild_id_str, {})
        if streamers:
            # --- Construir lista de streamers de forma paginada ---
            lista_streamers = ""
            for uid, data in streamers.items():
                nome = data.get("nome", uid)
                plats_list = []
                for p in ["twitch", "youtube", "kick", "tiktok"]:
                    if data.get(p):
                        online = dados["lives"]["status"].get(guild_id_str, {}).get(uid, {}).get(p, False)
                        emoji = "🟢" if online else "🔴"
                        plats_list.append(f"{emoji} {p.capitalize()}: {data[p]}")
                horas_texto = "0h"
                async with db_pool.acquire() as conn:
                    row = await conn.fetchrow("SELECT total_seconds FROM live_streamer_stats WHERE guild_id=$1 AND user_id=$2", guild_id_str, uid)
                    if row:
                        horas_texto = f"{row['total_seconds'] // 3600}h {(row['total_seconds'] % 3600) // 60}m"
                if plats_list:
                    # Construção compacta: uma linha por streamer
                    linha = f"**<@{uid}>** ⏱️ {horas_texto}\n" + "\n".join(plats_list) + "\n\n"
                    # Se a linha atual ultrapassar 1000 caracteres, criar novo campo
                    if len(lista_streamers) + len(linha) > 1000:
                        embed.add_field(name="📋 Streamers Cadastrados (cont.)", value=lista_streamers, inline=False)
                        lista_streamers = linha
                    else:
                        lista_streamers += linha
            if lista_streamers:
                embed.add_field(name="📋 Streamers Cadastrados", value=lista_streamers, inline=False)
        else:
            embed.add_field(name="📋 Streamers Cadastrados", value="Nenhum streamer cadastrado.", inline=False)
        return embed

    @discord.ui.button(label="📝 Definir Canais e Cargo", style=discord.ButtonStyle.secondary, emoji="📝", custom_id="btn_live_set_channels")
    async def set_channels(self, interaction: discord.Interaction, button: Button):
        if not await is_admin_or_owner(interaction.user, interaction.guild.id):
            await interaction.response.send_message("Apenas usuários com o cargo administrador configurado podem definir os canais.", ephemeral=True)
            return
        modal = SetChannelsModal(interaction.guild.id, self)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="⚙️ Gerenciar Streamers", style=discord.ButtonStyle.secondary, emoji="⚙️", custom_id="btn_live_manage_streamers")
    async def manage_streamers(self, interaction: discord.Interaction, button: Button):
        if not await is_admin_or_owner(interaction.user, interaction.guild.id):
            await interaction.response.send_message("Permissão negada. Apenas usuários com o cargo administrador configurado podem gerenciar streamers.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        view = StreamerManagementView()
        embed = discord.Embed(title="⚙️ GERENCIAR STREAMERS", description="Use os botões abaixo para gerenciar os streamers monitorados.", color=0x7289da)
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="🔄 Atualizar", style=discord.ButtonStyle.secondary, emoji="🔄", row=1, custom_id="btn_live_refresh")
    async def refresh(self, interaction: discord.Interaction, button: Button):
        if not await is_admin_or_owner(interaction.user, interaction.guild.id):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        await interaction.response.defer()
        await load_all_data()
        embed = await self.build_embed(interaction.guild.id)
        await interaction.message.edit(embed=embed, view=self)
        await interaction.followup.send("Painel atualizado!", ephemeral=True)

    @discord.ui.button(label="⏱️ Resetar Horas", style=discord.ButtonStyle.danger, emoji="⏱️", row=1, custom_id="btn_live_reset_hours")
    async def reset_hours(self, interaction: discord.Interaction, button: Button):
        if not await is_admin_or_owner(interaction.user, interaction.guild.id):
            await interaction.response.send_message("Permissão negada.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE live_streamer_stats SET total_seconds = 0, last_notified_hour = 0 WHERE guild_id = $1", str(interaction.guild.id))
            embed = await self.build_embed(interaction.guild.id)
            if interaction.message:
                await interaction.message.edit(embed=embed, view=self)
            await interaction.followup.send("✅ As horas acumuladas de todos os streamers deste servidor foram resetadas para 0!", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Erro ao resetar horas: {e}", ephemeral=True)

# ========= MODAL COM LABELS ENCURTADOS E SUPORTE A STAFF =========
class SetChannelsModal(Modal, title="Configurar Notificações"):
    canais_ids = TextInput(
        label="Canais (Lives) - IDs c/ vírgula",
        placeholder="Ex: 123456789,987654321",
        required=True
    )
    cargo_id = TextInput(
        label="Cargo (Lives) - ID",
        placeholder="ID do cargo",
        required=True
    )
    staff_canais_ids = TextInput(
        label="Canais Staff (Horas) - IDs",
        placeholder="Ex: 123456789,987654321",
        required=False
    )
    staff_cargo_id = TextInput(
        label="Cargo Staff (Horas) - ID",
        placeholder="Deixe em branco para não mencionar",
        required=False
    )
    cargo_admin_id = TextInput(
        label="Cargo Admin (streamers) - ID",
        placeholder="Deixe em branco se não quiser restringir",
        required=False
    )
    servidor_id = TextInput(
        label="Servidor destino (opcional) - ID",
        placeholder="Deixe em branco para usar este servidor",
        required=False
    )
    mensagem_personalizada = TextInput(
        label="Mensagem extra (opcional)",
        placeholder="Ex: Confira a live!",
        required=False,
        style=discord.TextStyle.long
    )

    def __init__(self, guild_id, parent_view):
        super().__init__()
        self.guild_id = guild_id
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            # Canais gerais
            ids_raw = self.canais_ids.value.strip()
            channel_ids = [str(x.strip()) for x in ids_raw.split(",") if x.strip().isdigit()]
            if not channel_ids:
                await interaction.followup.send("Nenhum ID de canal válido informado.", ephemeral=True)
                return
            rid = int(self.cargo_id.value.strip())
            
            # Canais staff
            staff_ids_raw = self.staff_canais_ids.value.strip()
            staff_channel_ids = []
            if staff_ids_raw:
                staff_channel_ids = [str(x.strip()) for x in staff_ids_raw.split(",") if x.strip().isdigit()]
            
            staff_rid = self.staff_cargo_id.value.strip()
            if staff_rid:
                staff_rid = int(staff_rid)
            else:
                staff_rid = None
            
            cargo_admin = self.cargo_admin_id.value.strip()
            if cargo_admin:
                cargo_admin = str(int(cargo_admin))
            else:
                cargo_admin = None
                
            target_gid = self.servidor_id.value.strip()
            if target_gid:
                target_gid = int(target_gid)
                if not bot.get_guild(target_gid):
                    await interaction.followup.send("❌ Bot não está presente no servidor informado.", ephemeral=True)
                    return
            else:
                target_gid = None
            custom_msg = self.mensagem_personalizada.value.strip() or ""
            
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO live_bot_settings (guild_id, channel_ids, role_id, staff_channel_ids, staff_role_id, target_guild_id, custom_message, cargo_admin_lives_id, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                    ON CONFLICT (guild_id) DO UPDATE SET
                        channel_ids = EXCLUDED.channel_ids,
                        role_id = EXCLUDED.role_id,
                        staff_channel_ids = EXCLUDED.staff_channel_ids,
                        staff_role_id = EXCLUDED.staff_role_id,
                        target_guild_id = EXCLUDED.target_guild_id,
                        custom_message = EXCLUDED.custom_message,
                        cargo_admin_lives_id = EXCLUDED.cargo_admin_lives_id,
                        updated_at = NOW()
                """, str(self.guild_id), json.dumps(channel_ids), str(rid), json.dumps(staff_channel_ids), str(staff_rid) if staff_rid else None, str(target_gid) if target_gid else None, custom_msg, cargo_admin)
            
            await interaction.followup.send("✅ Configuração salva!", ephemeral=True)
            embed = await self.parent_view.build_embed(self.guild_id)
            if interaction.message:
                await interaction.message.edit(embed=embed, view=self.parent_view)
        except Exception as e:
            await interaction.followup.send(f"Erro: {e}", ephemeral=True)

# ========= PAINEL DE GERENCIAMENTO DE STREAMERS =========
class StreamerManagementView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="➕ Adicionar Streamer", style=discord.ButtonStyle.success, emoji="➕", custom_id="btn_add_streamer")
    async def add_streamer(self, interaction: discord.Interaction, button: Button):
        if not await is_admin_or_owner(interaction.user, interaction.guild.id):
            await interaction.response.send_message("Permissão negada.", ephemeral=True)
            return
        await interaction.response.send_modal(AddStreamerModal(interaction.guild.id))

    @discord.ui.button(label="🗑️ Remover Streamer", style=discord.ButtonStyle.danger, emoji="🗑️", custom_id="btn_remove_streamer")
    async def remove_streamer(self, interaction: discord.Interaction, button: Button):
        if not await is_admin_or_owner(interaction.user, interaction.guild.id):
            await interaction.response.send_message("Permissão negada.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        streamers = dados["lives"]["streamers"].get(str(interaction.guild.id), {})
        if not streamers:
            await interaction.followup.send("Nenhum streamer cadastrado.", ephemeral=True)
            return
        view = RemoveStreamerSelectView(interaction.guild.id)
        await interaction.followup.send("Selecione o streamer para remover:", view=view, ephemeral=True)

    @discord.ui.button(label="📋 Listar Streamers", style=discord.ButtonStyle.secondary, emoji="📋", custom_id="btn_list_streamers")
    async def list_streamers(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer(ephemeral=True)
        guild_id_str = str(interaction.guild.id)
        streamers = dados["lives"]["streamers"].get(guild_id_str, {})
        if not streamers:
            await interaction.followup.send("Nenhum streamer cadastrado.", ephemeral=True)
            return
        embed = discord.Embed(title="📋 LISTA DE STREAMERS", color=0x7289da)
        for uid, data in streamers.items():
            nome = data.get("nome", uid)
            plats = []
            for p in ["twitch", "youtube", "kick", "tiktok"]:
                if data.get(p):
                    plats.append(f"{p.capitalize()}: {data[p]}")
            plats_text = "\n".join(plats) if plats else "Nenhuma plataforma"
            embed.add_field(name=f"**{nome}** (<@{uid}>)", value=plats_text, inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

class RemoveStreamerSelectView(View):
    def __init__(self, guild_id):
        super().__init__(timeout=120)
        self.guild_id = guild_id
        streamers = dados["lives"]["streamers"].get(str(guild_id), {})
        options = []
        for uid, data in streamers.items():
            nome = data.get("nome", uid)
            plats = [p.capitalize() for p in ["twitch", "youtube", "kick", "tiktok"] if data.get(p)]
            desc = f"{nome} ({', '.join(plats)})" if plats else nome
            options.append(discord.SelectOption(label=desc[:100], value=uid))
        if options:
            self.add_item(StreamerRemoveDropdown(options, guild_id))

class StreamerRemoveDropdown(Select):
    def __init__(self, options, guild_id):
        super().__init__(placeholder="Escolha um streamer para remover...", options=options, custom_id=f"remove_dropdown_{guild_id}")
        self.guild_id = guild_id

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        uid = self.values[0]
        await delete_streamer(str(self.guild_id), uid)
        await load_all_data()
        await interaction.followup.send("✅ Streamer removido com sucesso!", ephemeral=True)

class AddStreamerModal(Modal, title="Adicionar Streamer"):
    plataforma = TextInput(label="PLATAFORMA (twitch/youtube/kick/tiktok)", placeholder="Ex: twitch", required=True)
    username = TextInput(label="USERNAME OU LINK", placeholder="Ex: alanzoka ou https://twitch.tv/alanzoka", required=True)
    discord_user = TextInput(label="ID DO DISCORD DO STREAMER", placeholder="Ex: 123456789012345678", required=True)
    observacao = TextInput(label="OBSERVAÇÃO (mensagem padrão)", placeholder="Aparecerá na notificação da live", required=False)

    def __init__(self, guild_id):
        super().__init__()
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        plat_input = self.plataforma.value.strip().lower()
        username_input = self.username.value.strip()
        obs = self.observacao.value.strip()
        uid = self.discord_user.value.strip().replace("<@!", "").replace("<@", "").replace(">", "")

        if not uid or not uid.isdigit() or len(uid) < 17 or len(uid) > 20:
            await interaction.followup.send("❌ ID do Discord inválido (deve ter entre 17 e 20 números).", ephemeral=True)
            return

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

        member = interaction.guild.get_member(int(uid))
        if member:
            nome_streamer = member.display_name
        else:
            await interaction.followup.send("⚠️ O usuário informado não está neste servidor.", ephemeral=True)
            return

        guild_str = str(self.guild_id)
        current = dados["lives"]["streamers"].get(guild_str, {}).get(uid, {})
        try:
            await save_streamer(
                guild_str, uid,
                nome=current.get("nome", nome_streamer),
                twitch=current.get("twitch") if platform != "twitch" else identifier,
                youtube=current.get("youtube") if platform != "youtube" else identifier,
                kick=current.get("kick") if platform != "kick" else identifier,
                tiktok=current.get("tiktok") if platform != "tiktok" else identifier,
                observacao=obs or current.get("observacao", "")
            )
        except Exception as e:
            await interaction.followup.send(f"Erro ao salvar: {e}", ephemeral=True)
            return

        await load_all_data()
        await interaction.followup.send(f"✅ Streamer **{nome_streamer}** adicionado em **{platform}**!", ephemeral=True)

        # ===== NOTIFICAÇÃO IMEDIATA E ATUALIZAÇÃO DE STATUS =====
        guild = interaction.guild
        config = await load_live_settings(self.guild_id)
        if not config:
            logger.warning("Configuração não encontrada para notificação imediata.")
            return

        channel_ids = config.get("channel_ids", [])
        role_id = config.get("role_id")
        role_mention = f"<@&{role_id}>" if role_id and guild.get_role(role_id) else ""
        custom_message = config.get("custom_message", "")
        
        logger.info(f"Verificando live para {nome_streamer} ({platform}: {identifier})")
        
        streamer_data = dados["lives"]["streamers"].get(guild_str, {}).get(uid, {})
        
        is_live = False
        stream_info = None
        if platform == "twitch":
            lives = await check_twitch_lives([identifier])
            is_live = identifier.lower() in lives
            if is_live:
                stream_info = lives[identifier.lower()]
                logger.info(f"✅ Twitch live detectada: {stream_info.get('title')}")
        elif platform == "youtube":
            lives = await check_youtube_lives([identifier])
            is_live = identifier in lives
            if is_live:
                stream_info = lives[identifier]
                logger.info(f"✅ YouTube live detectada: {stream_info.get('snippet', {}).get('title')}")
        elif platform == "kick":
            is_live, stream_info = await check_kick_live(identifier)
            if is_live:
                logger.info(f"✅ Kick live detectada: {stream_info.get('title')}")
        elif platform == "tiktok":
            live_info = await check_tiktok_live(identifier)
            is_live = live_info is not None
            if is_live:
                stream_info = live_info
                logger.info(f"✅ TikTok live detectada: {stream_info.get('title')}")

        # Salva o status e inicia sessão se estiver ao vivo
        await save_status(guild_str, uid, platform, is_live)
        if is_live:
            now_utc = datetime.now()
            await save_session(guild_str, uid, platform, now_utc, False)
            # Atualiza a memória
            dados["lives"]["status"].setdefault(guild_str, {}).setdefault(uid, {})[platform] = True
            dados["lives"]["sessions"].setdefault(guild_str, {}).setdefault(uid, {})[platform] = {
                "start_time": now_utc,
                "three_hour_notified": False
            }

        if is_live and stream_info and channel_ids:
            logger.info(f"🔔 Enviando notificação de live para {nome_streamer}...")
            await send_live_notification(guild, channel_ids, role_mention, custom_message, platform, streamer_data, stream_info, uid)
            last_key = f"{platform}_{uid}"
            if platform == "twitch":
                await save_last_notified(last_key, stream_info.get("id"))
            elif platform == "youtube":
                await save_last_notified(last_key, stream_info.get("id", {}).get("videoId"))
            else:
                await save_last_notified(last_key, "live")
            logger.info("✅ Notificação enviada!")
        else:
            if not is_live:
                logger.info(f"❌ {nome_streamer} não está em live em {platform}.")
            elif not channel_ids:
                logger.warning("❌ Nenhum canal configurado para notificações.")

        # Atualiza o painel se possível
        try:
            # Tenta encontrar a mensagem do painel e atualizar
            for channel_id in config.get("channel_ids", []):
                canal = guild.get_channel(int(channel_id))
                if canal:
                    async for msg in canal.history(limit=20):
                        if msg.author == bot.user and msg.embeds:
                            # Verifica se é o painel (pelo título)
                            if msg.embeds[0].title and "NOTIFICAÇÃO DE LIVES" in msg.embeds[0].title:
                                view = LiveConfigView()
                                embed = await view.build_embed(guild.id)
                                await msg.edit(embed=embed, view=view)
                                break
                    break
        except Exception as e:
            logger.warning(f"Não foi possível atualizar o painel automaticamente: {e}")

# ========= COMANDOS =========
@bot.command(name="painel_lives")
@commands.has_permissions(administrator=True)
async def enviar_painel(ctx):
    if not is_live_guild_active(ctx.guild.id):
        await ctx.send("❌ Este servidor não possui uma assinatura ativa do bot de lives.")
        return
    view = LiveConfigView()
    embed = await view.build_embed(ctx.guild.id)
    await ctx.send(embed=embed, view=view)
    await ctx.message.delete()

@bot.command(name="live_status")
async def live_status(ctx):
    if not is_live_guild_active(ctx.guild.id):
        await ctx.send("❌ Este servidor não possui uma assinatura ativa do bot de lives.")
        return
    guild_id_str = str(ctx.guild.id)
    streamers = dados["lives"]["streamers"].get(guild_id_str, {})
    if not streamers:
        await ctx.send("Nenhum streamer cadastrado neste servidor.")
        return
    embed = discord.Embed(title="📡 STATUS DOS STREAMERS", color=0x2c2f33)
    for uid, data in streamers.items():
        nome = data.get("nome", uid)
        status_text = ""
        for p in ["twitch", "youtube", "kick", "tiktok"]:
            if data.get(p):
                online = dados["lives"]["status"].get(guild_id_str, {}).get(uid, {}).get(p, False)
                emoji = "🟢" if online else "🔴"
                status_text += f"{emoji} {p.capitalize()}: {data[p]}\n"
        if status_text:
            embed.add_field(name=f"**{nome}** (<@{uid}>)", value=status_text, inline=False)
    await ctx.send(embed=embed)

@bot.command(name="refresh_lives")
@commands.has_permissions(administrator=True)
async def refresh_lives(ctx):
    await ctx.send("🔄 Atualizando lista de assinaturas ativas...")
    await update_active_live_guilds()
    if is_live_guild_active(ctx.guild.id):
        await ctx.send("✅ Assinatura ativa confirmada! Use `!painel_lives` para configurar as notificações.")
    else:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT status, expires_at FROM live_bot_subscriptions WHERE guild_id = $1",
                str(ctx.guild.id)
            )
        if row:
            expires = row['expires_at'].replace(tzinfo=timezone.utc)
            if row['status'] == 'pending_activation':
                await ctx.send("⏳ A assinatura está pendente de ativação. Certifique-se de que o bot foi convidado corretamente e tente novamente em alguns segundos.")
            elif row['status'] == 'active' and expires <= datetime.now(timezone.utc):
                await ctx.send("❌ A assinatura expirou. Renove no site.")
            else:
                await ctx.send(f"❌ Status da assinatura: **{row['status']}**. Entre em contato com o suporte.")
        else:
            await ctx.send("❌ Nenhuma assinatura encontrada para este servidor. Verifique se você comprou a assinatura e convidou o bot.")

# ========= EVENTOS =========
@bot.event
async def on_ready():
    await init_db()
    await load_all_data()
    await update_active_live_guilds()
    logger.info(f"✅ Bot de Lives online: {bot.user}")
    live_check_loop.start()

@bot.event
async def on_guild_join(guild):
    logger.info(f"Bot de lives adicionado ao servidor: {guild.name} ({guild.id})")
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT user_email FROM live_bot_subscriptions WHERE guild_id = $1 AND status = 'pending_activation'", str(guild.id))
        if row:
            await conn.execute("UPDATE live_bot_subscriptions SET status = 'active', updated_at = NOW() WHERE guild_id = $1", str(guild.id))
            channel = guild.system_channel or (guild.text_channels[0] if guild.text_channels else None)
            if channel:
                view = LiveConfigView()
                embed = await view.build_embed(guild.id)
                await channel.send("🎉 Assinatura do bot de lives ativada! Aqui está o seu painel de configuração:", embed=embed, view=view)
    await update_active_live_guilds()

# ========= INICIALIZAÇÃO =========
if __name__ == "__main__":
    bot.run(TOKEN)
