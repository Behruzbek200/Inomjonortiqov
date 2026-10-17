#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ShazamIO Telegram Bot (tgmusici asosida) + Majburiy obuna + Admin paneli + Reklama.

Bitta fayl. Kutubxona: python-telegram-bot (v21+), shazamio, sqlite3 (o'rnatilgan).

Sozlamalar muhit o'zgaruvchilari orqali (.env fayli ham o'qiladi):
    BOT_TOKEN       - @BotFather bergan token (majburiy)
    MAIN_ADMIN_ID   - asosiy admin Telegram ID (majburiy emas, standart: eski config.py dagi ID)
    DB_PATH         - SQLite fayl yo'li (standart: bot_data.db)
    LOG_FILE        - log fayli (standart: bot.log)
"""

import asyncio
import html
import logging
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # python-dotenv ixtiyoriy
    pass

from telegram import (
    BotCommand,
    BotCommandScopeDefault,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ChatMemberStatus, ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    CommandHandler,
    ContextTypes,
    InlineQueryHandler,
    MessageHandler,
    TypeHandler,
    filters,
)

import httpx
from shazamio import Shazam

# ============================================================
# SOZLAMALAR
# ============================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MAIN_ADMIN_ID = int(os.getenv("MAIN_ADMIN_ID", "5207653104"))
DB_PATH = os.getenv("DB_PATH", "bot_data.db")
LOG_FILE = os.getenv("LOG_FILE", "bot.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

MAX_INLINE_RESULTS = 10
MAX_AUDIO_FILE_SIZE = 20 * 1024 * 1024
SUPPORTED_AUDIO_FORMATS = (".mp3", ".wav", ".ogg", ".m4a", ".flac")
ENABLE_RATE_LIMITING = True
MAX_REQUESTS_PER_MINUTE = 10
BROADCAST_DELAY = 0.05          # ~20 xabar/soniya (Telegram limiti ~30)
BROADCAST_MAX_RETRIES = 3
MEMBERSHIP_CACHE_TTL = 60       # soniya
LINK_CLIP_SECONDS = 45          # havoladan aniqlash uchun olinadigan audio uzunligi
LINK_MAX_FILESIZE = 80 * 1024 * 1024
LINK_DOWNLOAD_TIMEOUT = 120     # soniya
SOCIAL_DOMAINS = (
    "instagram.com", "tiktok.com", "youtube.com", "youtu.be", "facebook.com", "fb.watch",
    "twitter.com", "x.com", "vk.com", "pinterest.com", "pin.it", "reddit.com", "soundcloud.com",
)

TZ = timezone(timedelta(hours=5))  # O'zbekiston vaqti (UTC+5)

# ============================================================
# LOGGING
# ============================================================
_handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
if LOG_FILE:
    _handlers.append(RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8"))
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=_handlers,
)
# httpx URL'larida bot tokeni bo'ladi - logga tushmasin
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger("musicbot")


# ============================================================
# MA'LUMOTLAR BAZASI (SQLite)
# ============================================================
_db_lock = threading.Lock()
_conn: Optional[sqlite3.Connection] = None


def now_iso() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")


def db_init() -> None:
    global _conn
    db_dir = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(db_dir, exist_ok=True)
    _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    _conn.row_factory = sqlite3.Row
    with _db_lock:
        _conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users(
                user_id INTEGER PRIMARY KEY,
                first_name TEXT, username TEXT,
                joined_at TEXT NOT NULL, last_seen TEXT,
                blocked INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS admins(
                user_id INTEGER PRIMARY KEY, added_by INTEGER, added_at TEXT
            );
            CREATE TABLE IF NOT EXISTS channels(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL UNIQUE,
                title TEXT, link TEXT,
                kind TEXT NOT NULL,                 -- 'public' | 'private'
                request_mode INTEGER NOT NULL DEFAULT 0,
                added_at TEXT
            );
            CREATE TABLE IF NOT EXISTS join_requests(
                user_id INTEGER NOT NULL, chat_id INTEGER NOT NULL,
                source TEXT NOT NULL,               -- 'telegram' | 'manual'
                created_at TEXT,
                PRIMARY KEY(user_id, chat_id)
            );
            CREATE TABLE IF NOT EXISTS broadcasts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT, finished_at TEXT,
                total INTEGER DEFAULT 0, sent INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0, blocked INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
            """
        )
        _conn.commit()


def db_exec(sql: str, args: tuple = ()) -> int:
    with _db_lock:
        cur = _conn.execute(sql, args)
        _conn.commit()
        return cur.lastrowid


def db_all(sql: str, args: tuple = ()) -> List[sqlite3.Row]:
    with _db_lock:
        return _conn.execute(sql, args).fetchall()


def db_one(sql: str, args: tuple = ()) -> Optional[sqlite3.Row]:
    with _db_lock:
        return _conn.execute(sql, args).fetchone()


def get_setting(key: str, default: str = "") -> str:
    row = db_one("SELECT value FROM settings WHERE key=?", (key,))
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    db_exec(
        "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def upsert_user(user) -> None:
    ts = now_iso()
    db_exec(
        """INSERT INTO users(user_id, first_name, username, joined_at, last_seen, blocked)
           VALUES(?,?,?,?,?,0)
           ON CONFLICT(user_id) DO UPDATE SET
             first_name=excluded.first_name, username=excluded.username,
             last_seen=excluded.last_seen, blocked=0""",
        (user.id, user.first_name, user.username, ts, ts),
    )


def mark_blocked(user_id: int) -> None:
    db_exec("UPDATE users SET blocked=1 WHERE user_id=?", (user_id,))


# --- adminlar ---
def is_main_admin(user_id: int) -> bool:
    return user_id == MAIN_ADMIN_ID


def is_admin(user_id: int) -> bool:
    if user_id == MAIN_ADMIN_ID:
        return True
    return db_one("SELECT 1 FROM admins WHERE user_id=?", (user_id,)) is not None


def list_admins() -> List[sqlite3.Row]:
    return db_all("SELECT * FROM admins ORDER BY added_at")


# --- kanallar ---
def list_channels() -> List[sqlite3.Row]:
    return db_all("SELECT * FROM channels ORDER BY id")


def get_channel(row_id: int) -> Optional[sqlite3.Row]:
    return db_one("SELECT * FROM channels WHERE id=?", (row_id,))


def has_join_request(user_id: int, chat_id: int) -> bool:
    return db_one(
        "SELECT 1 FROM join_requests WHERE user_id=? AND chat_id=?", (user_id, chat_id)
    ) is not None


def save_join_request(user_id: int, chat_id: int, source: str) -> None:
    db_exec(
        """INSERT INTO join_requests(user_id, chat_id, source, created_at) VALUES(?,?,?,?)
           ON CONFLICT(user_id, chat_id) DO UPDATE SET source=
             CASE WHEN join_requests.source='telegram' THEN 'telegram' ELSE excluded.source END""",
        (user_id, chat_id, source, now_iso()),
    )


# ============================================================
# YORDAMCHI FUNKSIYALAR
# ============================================================
def esc(value) -> str:
    return html.escape(str(value if value is not None else ""))


def forwarded_chat(msg):
    """Forward qilingan xabarning manba chat'i (PTB eski va yangi versiyalari uchun)."""
    origin = getattr(msg, "forward_origin", None)
    if origin is not None and getattr(origin, "chat", None) is not None:
        return origin.chat
    return getattr(msg, "forward_from_chat", None)


def forwarded_user(msg):
    origin = getattr(msg, "forward_origin", None)
    if origin is not None and getattr(origin, "sender_user", None) is not None:
        return origin.sender_user
    return getattr(msg, "forward_from", None)


async def safe_edit(query, text: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await query.edit_message_text(
            text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )
    except BadRequest as e:
        if "not modified" in str(e).lower():
            return
        logger.warning("edit_message_text xatosi: %s", e)
        try:
            await query.message.reply_text(
                text, reply_markup=markup, parse_mode=ParseMode.HTML, disable_web_page_preview=True
            )
        except TelegramError as e2:
            logger.error("reply_text xatosi: %s", e2)


# ============================================================
# MATNLAR (o'zbekcha)
# ============================================================
BTN_SONG = "🔎 Qo'shiq qidirish"
BTN_ARTIST = "🎤 Ijrochi qidirish"
BTN_IDENT = "🎧 Musiqani aniqlash"
BTN_CHARTS = "🌍 Dunyo TOP 10"
BTN_HELP = "ℹ️ Yordam"
BTN_ADMIN = "🛠 Admin paneli"
MENU_BUTTONS = {BTN_SONG, BTN_ARTIST, BTN_IDENT, BTN_CHARTS, BTN_HELP, BTN_ADMIN}

START_TEXT = (
    "🎵 <b>Musiqa botiga xush kelibsiz!</b>\n\n"
    "🔎 <b>Qo'shiq nomi, ijrochisi yoki janrini kiriting — men topaman.</b>\n\n"
    "🎧 Yoki menga yuboring:\n"
    "• audio fayl yoki ovozli xabar\n"
    "• Instagram, TikTok, YouTube va boshqa ijtimoiy tarmoq havolasi\n"
    "— musiqasini aniqlab beraman.\n\n"
    "Pastdagi tugmalardan ham foydalanishingiz mumkin 👇"
)

HELP_TEXT = (
    "ℹ️ <b>Yordam</b>\n\n"
    "<b>1. Qidirish:</b> qo'shiq nomi, ijrochi yoki janrni oddiy yozing.\n\n"
    "<b>2. Audio aniqlash:</b> audio fayl (MP3, WAV, OGG, M4A, FLAC) yoki ovozli xabar yuboring.\n\n"
    "<b>3. Havola orqali:</b> Instagram, TikTok, YouTube, Facebook, X (Twitter), VK va boshqa "
    "tarmoqlardagi video havolasini yuboring — undagi musiqani topaman.\n\n"
    "<b>4. Inline:</b> istalgan chatda <code>@{bot_username} qo'shiq nomi</code>.\n\n"
    "📝 Fayl hajmi 20 MB gacha. Yopiq (private) havolalar ishlamasligi mumkin."
)

ERR = {
    "audio_recognition_failed": "❌ Kechirasiz, bu audioni aniqlay olmadim. Aniqroq audio bilan urinib ko'ring.",
    "file_too_large": "❌ Fayl juda katta! Maksimal hajm 20 MB.",
    "unsupported_format": "❌ Fayl formati qo'llab-quvvatlanmaydi! MP3, WAV, OGG, M4A yoki FLAC yuboring.",
    "no_results": "❌ Qidiruv bo'yicha natija topilmadi.",
    "rate_limited": "⚠️ Juda ko'p so'rov! Biroz kutib, qayta urinib ko'ring.",
    "api_error": "❌ Xatolik yuz berdi. Keyinroq qayta urinib ko'ring.",
    "link_failed": "❌ Havoladan audio olib bo'lmadi. Havola yopiq, o'chirilgan yoki tarmoq login talab qilayotgan bo'lishi mumkin.",
    "link_unsupported": "❌ Bu havola qo'llab-quvvatlanmaydi. Instagram, TikTok, YouTube, Facebook, X, VK havolasini yuboring.",
    "link_unavailable": "⚠️ Havola orqali aniqlash serverda hozircha yoqilmagan (yt-dlp yoki ffmpeg o'rnatilmagan).",
}


# ============================================================
# MUSIQA QISMI (tgmusici dagi funksiyalar saqlangan)
# ============================================================
shazam = Shazam()
user_requests: Dict[int, List[datetime]] = {}


def check_rate_limit(user_id: int) -> bool:
    if not ENABLE_RATE_LIMITING:
        return True
    now = datetime.now()
    recent = [t for t in user_requests.get(user_id, []) if now - t < timedelta(minutes=1)]
    if len(recent) >= MAX_REQUESTS_PER_MINUTE:
        user_requests[user_id] = recent
        return False
    recent.append(now)
    user_requests[user_id] = recent
    return True


def error_text(user_id: int, err: Optional[BaseException]) -> str:
    """Oddiy foydalanuvchiga umumiy xato; adminga xatoning aniq sababi ham ko'rsatiladi."""
    text = ERR["api_error"]
    if err is not None and is_admin(user_id):
        text += f"\n\n🛠 <code>{esc(type(err).__name__)}: {esc(str(err)[:250])}</code>"
    return text


# ---------- iTunes (Apple) qidiruvi: Shazam qidiruvi ishlamaganda zaxira ----------
ITUNES_SEARCH = "https://itunes.apple.com/search"
ITUNES_LOOKUP = "https://itunes.apple.com/lookup"


async def itunes_get(url: str, params: dict) -> dict:
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()


async def itunes_search_songs(query: str, limit: int = 5) -> List[dict]:
    data = await itunes_get(ITUNES_SEARCH, {"term": query, "media": "music", "entity": "song", "limit": limit})
    return [r for r in data.get("results", []) if r.get("trackId") and r.get("trackName")]


def itunes_details(r: dict) -> dict:
    cover = (r.get("artworkUrl100") or "").replace("100x100", "600x600") or None
    return {
        "title": r.get("trackName") or "Noma'lum qo'shiq",
        "artist": r.get("artistName") or "Noma'lum ijrochi",
        "album": r.get("collectionName"),
        "year": (r.get("releaseDate") or "")[:4] or None,
        "cover": cover,
        "genre": r.get("primaryGenreName"),
        "shazam_url": r.get("trackViewUrl"),
        "link_label": "🍎 Apple Music",
        "key": None,
        "preview": r.get("previewUrl"),
    }


def track_details(track: dict) -> dict:
    """Shazam track lug'atidan kerakli maydonlarni xavfsiz ajratib oladi."""
    title = track.get("title") or "Noma'lum qo'shiq"
    artist = track.get("subtitle") or "Noma'lum ijrochi"
    album = year = None
    for section in track.get("sections") or []:
        for meta in section.get("metadata") or []:
            name = (meta.get("title") or "").lower()
            if name == "album" and not album:
                album = meta.get("text")
            elif name == "released" and not year:
                year = meta.get("text")
    images = track.get("images") or {}
    cover = images.get("coverarthq") or images.get("coverart") or images.get("background")
    genre = (track.get("genres") or {}).get("primary")
    shazam_url = track.get("url") or (track.get("share") or {}).get("href")
    return {
        "title": title, "artist": artist, "album": album, "year": year,
        "cover": cover, "genre": genre, "shazam_url": shazam_url,
        "key": track.get("key"),
    }


def search_links(title: str, artist: str) -> Dict[str, str]:
    from urllib.parse import quote_plus
    q = quote_plus(f"{artist} {title}")
    return {
        "spotify": f"https://open.spotify.com/search/{q}",
        "youtube": f"https://www.youtube.com/results?search_query={q}",
    }


def track_text(d: dict) -> str:
    text = f"🎵 <b>{esc(d['title'])}</b>\n👤 <b>Ijrochi:</b> {esc(d['artist'])}\n"
    if d["album"]:
        text += f"💿 <b>Albom:</b> {esc(d['album'])}\n"
    if d["genre"]:
        text += f"🎼 <b>Janr:</b> {esc(d['genre'])}\n"
    if d["year"]:
        text += f"📅 <b>Yil:</b> {esc(d['year'])}\n"
    return text


def track_keyboard(d: dict) -> InlineKeyboardMarkup:
    links = search_links(d["title"], d["artist"])
    rows = [[
        InlineKeyboardButton("🎧 Spotify", url=links["spotify"]),
        InlineKeyboardButton("📺 YouTube", url=links["youtube"]),
    ]]
    if d["shazam_url"]:
        rows.append([InlineKeyboardButton(d.get("link_label") or "🔎 Shazam", url=d["shazam_url"])])
    if d["key"]:
        rows.append([InlineKeyboardButton("🎵 O'xshash qo'shiqlar", callback_data=f"sim:{d['key']}")])
    return InlineKeyboardMarkup(rows)


async def send_details(message, d: dict) -> None:
    text = track_text(d)
    markup = track_keyboard(d)
    try:
        if d["cover"]:
            await message.reply_photo(photo=d["cover"], caption=text, reply_markup=markup, parse_mode=ParseMode.HTML)
            return
    except TelegramError as e:
        logger.warning("Rasm bilan yuborib bo'lmadi, matn yuboriladi: %s", e)
    await message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)


async def send_track_info(message, track: dict) -> None:
    await send_details(message, track_details(track))


def main_menu_markup(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(BTN_SONG), KeyboardButton(BTN_ARTIST)],
        [KeyboardButton(BTN_IDENT), KeyboardButton(BTN_CHARTS)],
        [KeyboardButton(BTN_HELP)],
    ]
    if is_admin(user_id):
        rows[-1].append(KeyboardButton(BTN_ADMIN))
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.effective_message.reply_text(
        START_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_markup(update.effective_user.id)
    )


async def search_tracks(msg, user_id: int, query: str):
    """Qo'shiq nomi / ijrochi / janr bo'yicha qidiradi (iTunes).
    Shazam'ning qidiruv manzili (/services/search/v3) 404 qaytargani uchun ishlatilmaydi."""
    if not check_rate_limit(user_id):
        await msg.reply_text(ERR["rate_limited"])
        return
    try:
        rows = []
        for r in await itunes_search_songs(query, 5):
            d = itunes_details(r)
            rows.append([InlineKeyboardButton(f"{d['title']} — {d['artist']}"[:62], callback_data=f"it:{r['trackId']}")])
    except Exception as e:
        logger.exception("Qidiruvda xato (iTunes): %s", e)
        await msg.reply_text(error_text(user_id, e), parse_mode=ParseMode.HTML)
        return
    if not rows:
        await msg.reply_text(ERR["no_results"])
        return
    await msg.reply_text(
        f"🔎 <b>{esc(query)}</b> bo'yicha natijalar.\nKerakli qo'shiqni tanlang:",
        reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML,
    )


async def track_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    key = q.data.split(":", 1)[1]
    if not check_rate_limit(q.from_user.id):
        await q.message.reply_text(ERR["rate_limited"])
        return
    try:
        track = await shazam.track_about(int(key))
        if not track:
            await q.message.reply_text(ERR["no_results"])
            return
        track.setdefault("key", key)
        await send_track_info(q.message, track)
    except Exception as e:
        logger.exception("Qo'shiq ma'lumotini olishda xato: %s", e)
        await q.message.reply_text(error_text(q.from_user.id, e), parse_mode=ParseMode.HTML)


async def itunes_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    track_id = q.data.split(":", 1)[1]
    if not check_rate_limit(q.from_user.id):
        await q.message.reply_text(ERR["rate_limited"])
        return
    try:
        data = await itunes_get(ITUNES_LOOKUP, {"id": track_id})
        res = [r for r in data.get("results", []) if r.get("trackName")]
        if not res:
            await q.message.reply_text(ERR["no_results"])
            return
        d = itunes_details(res[0])
        await send_details(q.message, d)
        if d.get("preview"):
            try:
                await q.message.reply_audio(audio=d["preview"], title=d["title"], performer=d["artist"],
                                            caption="🎧 30 soniyalik namuna")
            except TelegramError as e:
                logger.warning("Namuna audioni yuborib bo'lmadi: %s", e)
    except Exception as e:
        logger.exception("iTunes ma'lumotida xato: %s", e)
        await q.message.reply_text(error_text(q.from_user.id, e), parse_mode=ParseMode.HTML)


async def search_artist(msg, user_id: int, query: str):
    if not check_rate_limit(user_id):
        await msg.reply_text(ERR["rate_limited"])
        return
    try:
        data = await itunes_get(ITUNES_SEARCH, {"term": query, "media": "music", "entity": "musicArtist", "limit": 1})
        res = data.get("results") or []
        if not res:
            await msg.reply_text(ERR["no_results"])
            return
        name = res[0].get("artistName") or query
        text = f"👤 <b>{esc(name)}</b>\n"
        if res[0].get("primaryGenreName"):
            text += f"🎼 <b>Janr:</b> {esc(res[0]['primaryGenreName'])}\n"
        if res[0].get("artistLinkUrl"):
            text += f"🔗 {esc(res[0]['artistLinkUrl'])}\n"
        await msg.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.exception("Ijrochi qidiruvida xato (iTunes): %s", e)
        await msg.reply_text(error_text(user_id, e), parse_mode=ParseMode.HTML)
        return
    await search_tracks(msg, user_id, name)


async def show_charts(msg, user_id: int):
    if not check_rate_limit(user_id):
        await msg.reply_text(ERR["rate_limited"])
        return
    try:
        results = await shazam.top_world_tracks(limit=10)
        tracks = (results or {}).get("tracks") or []
        if not tracks:
            await msg.reply_text(ERR["no_results"])
            return
        text = "🌍 <b>Dunyo bo'yicha TOP 10</b>\n\n"
        for i, tr in enumerate(tracks[:10], 1):
            text += f"{i}. <b>{esc(tr.get('title'))}</b> — {esc(tr.get('subtitle'))}\n"
        await msg.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.exception("Chartlarda xato: %s", e)
        await msg.reply_text(error_text(user_id, e), parse_mode=ParseMode.HTML)


# ---------- Ijtimoiy tarmoq havolasi orqali aniqlash ----------
LINK_RE = re.compile(r"https?://[^\s]+", re.I)
_link_sem = asyncio.Semaphore(2)  # bir vaqtda ko'pi bilan 2 ta yuklash


def extract_url(text: str) -> Optional[str]:
    m = LINK_RE.search(text or "")
    return m.group(0).rstrip(").,") if m else None


def is_social_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return any(host == d or host.endswith("." + d) for d in SOCIAL_DOMAINS)


def _download_blocking(url: str, outdir: str) -> str:
    import yt_dlp  # ixtiyoriy bog'liqlik
    opts = {
        "format": "bestaudio[abr<=160]/bestaudio/worst",
        "outtmpl": os.path.join(outdir, "src.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 20,
        "retries": 2,
        "max_filesize": LINK_MAX_FILESIZE,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])
    files = [f for f in os.listdir(outdir) if f.startswith("src.")]
    if not files:
        raise RuntimeError("Fayl yuklanmadi")
    return os.path.join(outdir, files[0])


async def _cut_audio(src: str, dst: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-i", src, "-t", str(LINK_CLIP_SECONDS), "-vn", "-ac", "1", "-ar", "44100",
        "-acodec", "libmp3lame", "-b:a", "128k", dst,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        rc = await asyncio.wait_for(proc.wait(), timeout=60)
    except asyncio.TimeoutError:
        proc.kill()
        raise RuntimeError("ffmpeg vaqti tugadi")
    if rc != 0 or not os.path.exists(dst):
        raise RuntimeError(f"ffmpeg xato kodi: {rc}")


async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str):
    msg = update.effective_message
    if not check_rate_limit(update.effective_user.id):
        await msg.reply_text(ERR["rate_limited"])
        return
    try:
        import yt_dlp  # noqa: F401
    except ImportError:
        await msg.reply_text(ERR["link_unavailable"])
        return
    if not shutil.which("ffmpeg"):
        await msg.reply_text(ERR["link_unavailable"])
        return

    processing = await msg.reply_text("🔗 Havola qayta ishlanmoqda, biroz kuting...")
    try:
        async with _link_sem:
            with tempfile.TemporaryDirectory() as tmp:
                try:
                    src = await asyncio.wait_for(
                        asyncio.to_thread(_download_blocking, url, tmp), timeout=LINK_DOWNLOAD_TIMEOUT
                    )
                    clip = os.path.join(tmp, "clip.mp3")
                    await _cut_audio(src, clip)
                except Exception as e:
                    logger.warning("Havoladan yuklab bo'lmadi (%s): %s", url, e)
                    await msg.reply_text(ERR["link_failed"])
                    return
                recognize = getattr(shazam, "recognize", None) or getattr(shazam, "recognize_song")
                result = await recognize(clip)
        if result and result.get("track"):
            await send_track_info(msg, result["track"])
        else:
            await msg.reply_text(ERR["audio_recognition_failed"])
    except Exception as e:
        logger.exception("Havolani aniqlashda xato: %s", e)
        await msg.reply_text(ERR["api_error"])
    finally:
        try:
            await processing.delete()
        except TelegramError:
            pass


async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user_id = update.effective_user.id

    media = None
    if msg.audio:
        media = msg.audio
    elif msg.voice:
        media = msg.voice
    elif msg.document and (msg.document.file_name or "").lower().endswith(SUPPORTED_AUDIO_FORMATS):
        media = msg.document
    elif msg.document:
        await msg.reply_text(ERR["unsupported_format"])
        return
    if not media:
        return

    if not check_rate_limit(user_id):
        await msg.reply_text(ERR["rate_limited"])
        return
    if (media.file_size or 0) > MAX_AUDIO_FILE_SIZE:
        await msg.reply_text(ERR["file_too_large"])
        return

    processing = await msg.reply_text("🎵 Audio qayta ishlanmoqda...")
    temp_path = None
    try:
        tg_file = await context.bot.get_file(media.file_id)
        data = await tg_file.download_as_bytearray()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as tmp:
            tmp.write(data)
            temp_path = tmp.name

        recognize = getattr(shazam, "recognize", None) or getattr(shazam, "recognize_song")
        result = await recognize(temp_path)
        if result and result.get("track"):
            await send_track_info(msg, result["track"])
        else:
            await msg.reply_text(ERR["audio_recognition_failed"])
    except Exception as e:
        logger.exception("Audio qayta ishlashda xato: %s", e)
        await msg.reply_text(ERR["api_error"])
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        try:
            await processing.delete()
        except TelegramError:
            pass


async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.inline_query
    if not query or not query.query.strip():
        return
    if not check_rate_limit(query.from_user.id):
        return

    def make_item(d: dict, idx: int) -> InlineQueryResultArticle:
        links = search_links(d["title"], d["artist"])
        return InlineQueryResultArticle(
            id=str(d["key"] or f"it{idx}"),
            title=f"{d['title']} - {d['artist']}",
            description=f"Ijrochi: {d['artist']}",
            input_message_content=InputTextMessageContent(message_text=track_text(d), parse_mode=ParseMode.HTML),
            thumbnail_url=d["cover"] or None,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🎧 Spotify", url=links["spotify"]),
                InlineKeyboardButton("📺 YouTube", url=links["youtube"]),
            ]]),
        )

    items = []
    try:
        for r in await itunes_search_songs(query.query, MAX_INLINE_RESULTS):
            items.append(make_item(itunes_details(r), len(items)))
    except Exception as e:
        logger.exception("Inline qidiruvda xato (iTunes): %s", e)
    if items:
        try:
            await query.answer(items, cache_time=300)
        except TelegramError as e:
            logger.error("inline javobida xato: %s", e)


async def similar_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Asl kodda 'O'xshash qo'shiqlar' tugmasi bor edi, lekin handler yo'q edi."""
    q = update.callback_query
    await q.answer()
    key = q.data.split(":", 1)[1]
    if not check_rate_limit(q.from_user.id):
        await q.message.reply_text(ERR["rate_limited"])
        return
    try:
        results = await shazam.related_tracks(track_id=int(key), limit=5)
        tracks = (results or {}).get("tracks") or []
        if not tracks:
            await q.message.reply_text(ERR["no_results"])
            return
        text = "🎵 <b>O'xshash qo'shiqlar:</b>\n\n"
        for i, tr in enumerate(tracks[:5], 1):
            text += f"{i}. <b>{esc(tr.get('title'))}</b> — {esc(tr.get('subtitle'))}\n"
        await q.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.exception("O'xshash qo'shiqlarda xato: %s", e)
        await q.message.reply_text(ERR["api_error"])


# ============================================================
# MAJBURIY OBUNA
# ============================================================
_member_cache: Dict[tuple, float] = {}


async def is_real_member(bot, user_id: int, ch: sqlite3.Row) -> bool:
    """Kanalning HAQIQIY a'zoligini tekshiradi (so'rov holatiga bog'liq emas)."""
    key = (user_id, ch["chat_id"])
    ts = _member_cache.get(key)
    loop_time = time.monotonic()
    if ts and loop_time - ts < MEMBERSHIP_CACHE_TTL:
        return True
    try:
        m = await bot.get_chat_member(ch["chat_id"], user_id)
    except TelegramError as e:
        # Bot kanaldan chiqarilgan/huquqi yo'q bo'lsa foydalanuvchilar qulflanib qolmasin.
        logger.error("a'zolikni tekshirib bo'lmadi (chat_id=%s): %s", ch["chat_id"], e)
        return True
    ok = m.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER) or (
        m.status == ChatMemberStatus.RESTRICTED and getattr(m, "is_member", False)
    )
    if ok:
        _member_cache[key] = loop_time
    return ok


async def is_satisfied(bot, user_id: int, ch: sqlite3.Row) -> bool:
    """Alohida mantiq: 1) haqiqiy a'zolik, 2) a'zo bo'lmasa — so'rov yuborgan (faqat so'rovli kanal)."""
    if await is_real_member(bot, user_id, ch):
        return True
    if ch["request_mode"] and has_join_request(user_id, ch["chat_id"]):
        return True
    return False


async def missing_channels(bot, user_id: int) -> List[sqlite3.Row]:
    missing = []
    for ch in list_channels():
        if not await is_satisfied(bot, user_id, ch):
            missing.append(ch)
    return missing


def subscription_keyboard(missing: List[sqlite3.Row], user_id: int) -> InlineKeyboardMarkup:
    rows = []
    for ch in missing:
        label = ("📨 " if ch["request_mode"] else "📢 ") + (ch["title"] or "Kanal")
        if ch["link"]:
            rows.append([InlineKeyboardButton(label, url=ch["link"])])
        if ch["request_mode"] and not has_join_request(user_id, ch["chat_id"]):
            rows.append([InlineKeyboardButton(
                f"✅ So'rov yubordim: {ch['title'] or 'Kanal'}"[:60], callback_data=f"req:{ch['id']}"
            )])
    rows.append([InlineKeyboardButton("✅ Tekshirish", callback_data="chk")])
    return InlineKeyboardMarkup(rows)


SUB_TEXT = (
    "❗️ <b>Botdan foydalanish uchun quyidagi kanallarga obuna bo'ling.</b>\n\n"
    "Obuna bo'lgach «✅ Tekshirish» tugmasini bosing.\n"
    "📨 belgisi bor kanallarga qo'shilish uchun so'rov yuborish kifoya."
)


async def gate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Har bir kelgan update'ni tekshiradi: foydalanuvchini saqlaydi va obunani majburlaydi."""
    user = update.effective_user
    if not user or user.is_bot:
        return
    if not (update.message or update.callback_query or update.inline_query):
        return
    chat = update.effective_chat
    if update.message and chat and chat.type != "private":
        return

    try:
        upsert_user(user)
    except Exception as e:
        logger.exception("Foydalanuvchini saqlashda xato: %s", e)

    if is_admin(user.id) or get_setting("force_sub", "1") != "1":
        return
    if not list_channels():
        return

    cb = update.callback_query
    if cb and (cb.data == "chk" or (cb.data or "").startswith("req:")):
        return  # bu tugmalar o'zining handleri orqali ishlaydi

    missing = await missing_channels(context.bot, user.id)
    if not missing:
        return

    kb = subscription_keyboard(missing, user.id)
    if update.inline_query:
        try:
            await update.inline_query.answer(
                [InlineQueryResultArticle(
                    id="sub", title="❗️ Avval kanallarga obuna bo'ling",
                    description="Botdan foydalanish uchun botga o'ting va obuna bo'ling",
                    input_message_content=InputTextMessageContent("Botdan foydalanish uchun majburiy kanallarga obuna bo'ling."),
                )],
                cache_time=5, is_personal=True,
            )
        except TelegramError as e:
            logger.warning("inline javobida xato: %s", e)
    elif cb:
        await cb.answer("Avval kanallarga obuna bo'ling!", show_alert=True)
        await cb.message.reply_text(SUB_TEXT, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(SUB_TEXT, reply_markup=kb, parse_mode=ParseMode.HTML)
    raise ApplicationHandlerStop


async def check_sub_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    user = q.from_user
    missing = await missing_channels(context.bot, user.id) if get_setting("force_sub", "1") == "1" else []
    if missing:
        await q.answer("❌ Hali hamma kanallarga obuna bo'lmadingiz!", show_alert=True)
        await safe_edit(q, SUB_TEXT, subscription_keyboard(missing, user.id))
        return
    await q.answer("✅ Rahmat! Obuna tasdiqlandi.")
    try:
        await q.message.delete()
    except TelegramError:
        pass
    await q.message.chat.send_message(
        START_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_markup(user.id)
    )


async def request_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Telegram so'rov holatini avtomatik yubormagan bo'lsa — foydalanuvchi o'zi tasdiqlaydi."""
    q = update.callback_query
    user = q.from_user
    ch = get_channel(int(q.data.split(":", 1)[1]))
    if not ch or not ch["request_mode"]:
        await q.answer("Kanal topilmadi.", show_alert=True)
        return
    save_join_request(user.id, ch["chat_id"], "manual")
    await q.answer("✅ Qabul qilindi.")
    missing = await missing_channels(context.bot, user.id)
    if missing:
        await safe_edit(q, SUB_TEXT, subscription_keyboard(missing, user.id))
    else:
        await check_sub_callback(update, context)


async def chat_join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Telegram'dan kelgan haqiqiy 'qo'shilish so'rovi' — bazaga yoziladi (admin tasdig'isiz botdan foydalanish)."""
    req = update.chat_join_request
    if not req:
        return
    ch = db_one("SELECT * FROM channels WHERE chat_id=?", (req.chat.id,))
    if ch and ch["request_mode"]:
        save_join_request(req.from_user.id, req.chat.id, "telegram")
        try:
            upsert_user(req.from_user)
        except Exception:
            pass
        logger.info("Qo'shilish so'rovi saqlandi: user=%s chat=%s", req.from_user.id, req.chat.id)


# ============================================================
# ADMIN PANELI
# ============================================================
def admin_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📌 Majburiy obuna", callback_data="adm:fs"),
         InlineKeyboardButton("➕ Kanal qo'shish", callback_data="adm:addch")],
        [InlineKeyboardButton("➖ Kanalni o'chirish", callback_data="adm:delch"),
         InlineKeyboardButton("📋 Kanallar ro'yxati", callback_data="adm:chlist")],
        [InlineKeyboardButton("👤 Admin qo'shish", callback_data="adm:addadm"),
         InlineKeyboardButton("🗑 Adminni o'chirish", callback_data="adm:deladm")],
        [InlineKeyboardButton("📢 Reklama yuborish", callback_data="adm:bc"),
         InlineKeyboardButton("📊 Statistika", callback_data="adm:stats")],
        [InlineKeyboardButton("👥 Foydalanuvchilar soni", callback_data="adm:users"),
         InlineKeyboardButton("🏠 Bosh menyu", callback_data="adm:home")],
    ])


BACK_MARKUP = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Admin paneli", callback_data="adm:menu")]])
ADMIN_TEXT = "🛠 <b>Admin paneli</b>\n\nKerakli bo'limni tanlang:"


def channel_line(ch: sqlite3.Row) -> str:
    kind = "Ommaviy" if ch["kind"] == "public" else ("Maxfiy (so'rovli)" if ch["request_mode"] else "Maxfiy")
    link = esc(ch["link"] or "—")
    return f"• <b>{esc(ch['title'])}</b> [{kind}]\n  🔗 {link}\n  🆔 <code>{ch['chat_id']}</code>"


async def verify_bot_admin(bot, chat_id: int) -> Optional[str]:
    """Bot kanalda admin bo'lsa None, aks holda xato matni qaytaradi."""
    try:
        me = await bot.get_me()
        m = await bot.get_chat_member(chat_id, me.id)
        if m.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            return "❌ Bot bu kanalda admin emas. Avval botni kanalga admin qilib qo'ying."
        return None
    except TelegramError as e:
        return f"❌ Kanalni tekshirib bo'lmadi: {esc(e)}"


async def show_force_sub(q) -> None:
    on = get_setting("force_sub", "1") == "1"
    text = (f"📌 <b>Majburiy obuna</b>\n\nHolat: {'🟢 Yoqilgan' if on else '🔴 Ochirilgan'}\n"
            f"Kanallar soni: <b>{len(list_channels())}</b>")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔴 O'chirish" if on else "🟢 Yoqish", callback_data="adm:fs_toggle")],
        [InlineKeyboardButton("➕ Kanal qo'shish", callback_data="adm:addch"),
         InlineKeyboardButton("📋 Ro'yxat", callback_data="adm:chlist")],
        [InlineKeyboardButton("⬅️ Admin paneli", callback_data="adm:menu")],
    ])
    await safe_edit(q, text, kb)


async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    user = q.from_user
    # Har bir admin amalidan oldin ruxsat tekshiriladi
    if not is_admin(user.id):
        await q.answer("⛔️ Ruxsat yo'q.", show_alert=True)
        return
    await q.answer()
    parts = q.data.split(":")
    action = parts[1]
    arg = parts[2] if len(parts) > 2 else None
    ud = context.user_data

    if action == "menu":
        ud.clear()
        await safe_edit(q, ADMIN_TEXT, admin_menu_markup())

    elif action == "home":
        ud.clear()
        try:
            await q.message.delete()
        except TelegramError:
            pass
        await q.message.chat.send_message(START_TEXT, parse_mode=ParseMode.HTML,
                                          reply_markup=main_menu_markup(user.id))

    # ---- majburiy obuna ----
    elif action == "fs":
        await show_force_sub(q)

    elif action == "fs_toggle":
        on = get_setting("force_sub", "1") == "1"
        set_setting("force_sub", "0" if on else "1")
        await show_force_sub(q)

    elif action == "chlist":
        chans = list_channels()
        text = "📋 <b>Kanallar ro'yxati</b>\n\n" + ("\n\n".join(channel_line(c) for c in chans) if chans else "Hozircha kanal yo'q.")
        await safe_edit(q, text, BACK_MARKUP)

    elif action == "addch":
        ud.clear()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🌐 Ommaviy kanal", callback_data="adm:addpub")],
            [InlineKeyboardButton("🔒 Maxfiy kanal", callback_data="adm:addpriv")],
            [InlineKeyboardButton("⬅️ Admin paneli", callback_data="adm:menu")],
        ])
        await safe_edit(q, "➕ <b>Kanal qo'shish</b>\n\nKanal turini tanlang:", kb)

    elif action == "addpub":
        ud.clear()
        ud["state"] = "addpub"
        await safe_edit(q, "🌐 Kanal havolasini yuboring.\nMasalan: <code>https://t.me/kanal_nomi</code> yoki <code>@kanal_nomi</code>\n\n"
                           "⚠️ Bot kanalda admin bo'lishi shart.\nBekor qilish uchun pastdagi ⬅️ tugmani bosing.", BACK_MARKUP)

    elif action == "addpriv":
        ud.clear()
        ud["state"] = "addpriv_fwd"
        await safe_edit(q, "🔒 Maxfiy kanaldan istalgan xabarni shu yerga <b>forward</b> qiling.\n\n"
                           "⚠️ Bot kanalda admin bo'lishi shart.\nBekor qilish uchun pastdagi ⬅️ tugmani bosing.", BACK_MARKUP)

    elif action == "pt":  # maxfiy kanal turi: 0 = oddiy taklif havolasi, 1 = so'rov orqali
        pend = ud.get("pending_ch")
        if not pend or "link" not in pend:
            await safe_edit(q, "❌ Jarayon topilmadi. Qaytadan boshlang.", BACK_MARKUP)
            return
        try:
            db_exec(
                "INSERT INTO channels(chat_id,title,link,kind,request_mode,added_at) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, link=excluded.link, "
                "kind=excluded.kind, request_mode=excluded.request_mode",
                (pend["chat_id"], pend["title"], pend["link"], "private", int(arg), now_iso()),
            )
        except sqlite3.Error as e:
            logger.exception("Kanalni saqlashda xato: %s", e)
            await safe_edit(q, "❌ Bazaga yozishda xatolik.", BACK_MARKUP)
            return
        ud.clear()
        await safe_edit(q, f"✅ Maxfiy kanal qo'shildi: <b>{esc(pend['title'])}</b>"
                           f"{' (so‘rov orqali)' if arg == '1' else ''}", BACK_MARKUP)

    elif action == "delch":
        chans = list_channels()
        if not chans:
            await safe_edit(q, "Hozircha kanal yo'q.", BACK_MARKUP)
            return
        rows = [[InlineKeyboardButton(f"🗑 {c['title']}"[:60], callback_data=f"adm:delch:{c['id']}")] for c in chans]
        rows.append([InlineKeyboardButton("⬅️ Admin paneli", callback_data="adm:menu")])
        await safe_edit(q, "➖ O'chiriladigan kanalni tanlang:", InlineKeyboardMarkup(rows))

    # ---- adminlar ----
    elif action == "addadm":
        if not is_main_admin(user.id):
            await safe_edit(q, "⛔️ Faqat asosiy admin boshqa adminlarni qo'sha oladi.", BACK_MARKUP)
            return
        ud.clear()
        ud["state"] = "addadm"
        await safe_edit(q, "👤 Yangi adminning Telegram ID raqamini yuboring yoki uning xabarini forward qiling.\n"
                           "Bekor qilish uchun pastdagi ⬅️ tugmani bosing.", BACK_MARKUP)

    elif action == "deladm":
        if not is_main_admin(user.id):
            await safe_edit(q, "⛔️ Faqat asosiy admin adminlarni o'chira oladi.", BACK_MARKUP)
            return
        admins = list_admins()
        if not admins:
            await safe_edit(q, "Qo'shimcha adminlar yo'q.", BACK_MARKUP)
            return
        rows = [[InlineKeyboardButton(f"🗑 {a['user_id']}", callback_data=f"adm:deladm:{a['user_id']}")] for a in admins]
        rows.append([InlineKeyboardButton("⬅️ Admin paneli", callback_data="adm:menu")])
        await safe_edit(q, "🗑 O'chiriladigan adminni tanlang:", InlineKeyboardMarkup(rows))

    # ---- reklama ----
    elif action == "bc":
        ud.clear()
        ud["state"] = "bc_wait"
        await safe_edit(q, "📢 <b>Reklama yuborish</b>\n\nReklama sifatida yuboriladigan xabarni shu yerga jo'nating "
                           "(matn, rasm, video, audio, ovozli xabar, hujjat, sticker, animatsiya va h.k.).\n"
                           "Bekor qilish uchun pastdagi ⬅️ tugmani bosing.", BACK_MARKUP)

    elif action == "bc_btn":
        if not ud.get("bc"):
            await safe_edit(q, "❌ Reklama topilmadi.", BACK_MARKUP)
            return
        ud["state"] = "bc_buttons"
        await q.message.reply_text(
            "🔘 Tugmalarni har qatorga bittadan yuboring:\n<code>Matn | https://havola.uz</code>\n\nBekor qilish uchun pastdagi ⬅️ tugmani bosing.",
            parse_mode=ParseMode.HTML, reply_markup=BACK_MARKUP,
        )

    elif action == "bc_no":
        ud.clear()
        await safe_edit(q, "❌ Reklama bekor qilindi.", BACK_MARKUP)

    elif action == "bc_ok":
        bc = ud.get("bc")
        if not bc:
            await safe_edit(q, "❌ Reklama topilmadi.", BACK_MARKUP)
            return
        if context.bot_data.get("bc_running"):
            await q.message.reply_text("⏳ Boshqa reklama yuborilmoqda. Tugashini kuting.")
            return
        ud.clear()
        await safe_edit(q, "🚀 Reklama yuborish boshlandi. Tugagach natija yuboriladi.")
        context.bot_data["bc_running"] = True
        asyncio.create_task(run_broadcast(context.bot, q.message.chat_id, bc, context.bot_data))

    # ---- statistika ----
    elif action == "users":
        total = db_one("SELECT COUNT(*) c FROM users")["c"]
        active = db_one("SELECT COUNT(*) c FROM users WHERE blocked=0")["c"]
        await safe_edit(q, f"👥 <b>Foydalanuvchilar soni:</b> {total}\n🟢 Faol: {active}\n🚫 Bloklagan: {total - active}", BACK_MARKUP)

    elif action == "stats":
        await safe_edit(q, build_stats(), BACK_MARKUP)


async def admin_callback_param(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """adm:delch:<id> va adm:deladm:<id> — parametrli amallar."""
    q = update.callback_query
    user = q.from_user
    if not is_admin(user.id):
        await q.answer("⛔️ Ruxsat yo'q.", show_alert=True)
        return
    await q.answer()
    _, action, arg = q.data.split(":")
    if action == "delch":
        ch = get_channel(int(arg))
        if ch:
            db_exec("DELETE FROM channels WHERE id=?", (ch["id"],))
            db_exec("DELETE FROM join_requests WHERE chat_id=?", (ch["chat_id"],))
            await safe_edit(q, f"✅ Kanal o'chirildi: <b>{esc(ch['title'])}</b>", BACK_MARKUP)
        else:
            await safe_edit(q, "❌ Kanal topilmadi.", BACK_MARKUP)
    elif action == "deladm":
        if not is_main_admin(user.id):
            await safe_edit(q, "⛔️ Faqat asosiy admin adminlarni o'chira oladi.", BACK_MARKUP)
            return
        target = int(arg)
        if target == MAIN_ADMIN_ID:
            await safe_edit(q, "⛔️ Asosiy adminni o'chirib bo'lmaydi.", BACK_MARKUP)
            return
        db_exec("DELETE FROM admins WHERE user_id=?", (target,))
        await safe_edit(q, f"✅ Admin o'chirildi: <code>{target}</code>", BACK_MARKUP)


def build_stats() -> str:
    now = datetime.now(TZ)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat(timespec="seconds")
    week_start = (now - timedelta(days=7)).isoformat(timespec="seconds")
    total = db_one("SELECT COUNT(*) c FROM users")["c"]
    blocked = db_one("SELECT COUNT(*) c FROM users WHERE blocked=1")["c"]
    today = db_one("SELECT COUNT(*) c FROM users WHERE joined_at>=?", (day_start,))["c"]
    week = db_one("SELECT COUNT(*) c FROM users WHERE joined_at>=?", (week_start,))["c"]
    chans = db_one("SELECT COUNT(*) c FROM channels")["c"]
    admins = len(list_admins()) + 1  # + asosiy admin
    text = (
        "📊 <b>Statistika</b>\n\n"
        f"👥 Jami foydalanuvchilar: <b>{total}</b>\n"
        f"🚫 Botni bloklaganlar: <b>{blocked}</b>\n"
        f"🆕 Bugun qo'shilganlar: <b>{today}</b>\n"
        f"📅 Oxirgi 7 kunda: <b>{week}</b>\n"
        f"📌 Majburiy obuna kanallari: <b>{chans}</b>\n"
        f"👮 Adminlar soni: <b>{admins}</b>\n\n"
    )
    hist = db_all("SELECT * FROM broadcasts ORDER BY id DESC LIMIT 5")
    if not hist:
        return text + "📢 Reklama tarixi: hozircha yo'q."
    last = hist[0]
    text += (f"📢 <b>Oxirgi reklama natijasi</b> (#{last['id']}):\n"
             f"✅ Yuborildi: {last['sent']}\n❌ Muvaffaqiyatsiz: {last['failed']}\n"
             f"🚫 Bloklagan: {last['blocked']}\n👥 Jami: {last['total']}\n\n"
             "🗂 <b>Reklama tarixi:</b>\n")
    for h in hist:
        text += f"#{h['id']} — {esc((h['created_at'] or '')[:16].replace('T', ' '))} — ✅{h['sent']} ❌{h['failed']} 🚫{h['blocked']}\n"
    return text


# ---------- Reklama yuborish mexanizmi ----------
def parse_buttons(text: str) -> Optional[InlineKeyboardMarkup]:
    rows = []
    for line in text.splitlines():
        if "|" not in line:
            continue
        label, url = [p.strip() for p in line.split("|", 1)]
        if label and re.match(r"^(https?://|tg://)", url):
            rows.append([InlineKeyboardButton(label[:60], url=url)])
    return InlineKeyboardMarkup(rows) if rows else None


async def send_one(bot, uid: int, bc: dict) -> str:
    """'ok' | 'blocked' | 'failed'. Vaqtinchalik xatolarda qayta urinadi."""
    for attempt in range(BROADCAST_MAX_RETRIES):
        try:
            await bot.copy_message(
                chat_id=uid, from_chat_id=bc["from_chat_id"], message_id=bc["message_id"],
                reply_markup=bc.get("markup"),
            )
            return "ok"
        except RetryAfter as e:
            wait = e.retry_after.total_seconds() if hasattr(e.retry_after, "total_seconds") else float(e.retry_after)
            logger.warning("Flood limit: %s soniya kutiladi", wait)
            await asyncio.sleep(wait + 1)
        except Forbidden:
            mark_blocked(uid)
            return "blocked"
        except (TimedOut, NetworkError):
            await asyncio.sleep(2 ** attempt)
        except BadRequest as e:
            low = str(e).lower()
            if "chat not found" in low or "deactivated" in low:
                mark_blocked(uid)
                return "blocked"
            logger.error("copy_message BadRequest uid=%s: %s", uid, e)
            return "failed"
        except TelegramError as e:
            logger.error("copy_message xatosi uid=%s: %s", uid, e)
            return "failed"
    return "failed"


async def run_broadcast(bot, admin_chat_id: int, bc: dict, bot_data: dict) -> None:
    try:
        ids = [r["user_id"] for r in db_all("SELECT user_id FROM users WHERE blocked=0")]
        bid = db_exec("INSERT INTO broadcasts(created_at,total) VALUES(?,?)", (now_iso(), len(ids)))
        sent = failed = blocked = 0
        for uid in ids:
            res = await send_one(bot, uid, bc)
            if res == "ok":
                sent += 1
            elif res == "blocked":
                blocked += 1
            else:
                failed += 1
            await asyncio.sleep(BROADCAST_DELAY)
        db_exec(
            "UPDATE broadcasts SET finished_at=?, sent=?, failed=?, blocked=? WHERE id=?",
            (now_iso(), sent, failed, blocked, bid),
        )
        await bot.send_message(
            admin_chat_id,
            f"✅ <b>Reklama yakunlandi (#{bid})</b>\n\n📨 Yuborildi: {sent}\n❌ Muvaffaqiyatsiz: {failed}\n"
            f"🚫 Bloklagan: {blocked}\n👥 Jami: {len(ids)}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.exception("Reklama yuborishda xato: %s", e)
        try:
            await bot.send_message(admin_chat_id, "❌ Reklama yuborishda xatolik yuz berdi. Loglarni tekshiring.")
        except TelegramError:
            pass
    finally:
        bot_data["bc_running"] = False


async def send_bc_preview(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bc = context.user_data["bc"]
    msg = update.effective_message
    try:
        await context.bot.copy_message(
            chat_id=msg.chat_id, from_chat_id=bc["from_chat_id"], message_id=bc["message_id"],
            reply_markup=bc.get("markup"),
        )
    except TelegramError as e:
        logger.error("Oldindan ko'rsatishda xato: %s", e)
        await msg.reply_text(f"❌ Bu xabarni nusxalab bo'lmadi: {esc(e)}")
        context.user_data.clear()
        return
    count = db_one("SELECT COUNT(*) c FROM users WHERE blocked=0")["c"]
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Yuborish", callback_data="adm:bc_ok"),
         InlineKeyboardButton("❌ Bekor qilish", callback_data="adm:bc_no")],
        [InlineKeyboardButton("🔘 Tugma qo'shish", callback_data="adm:bc_btn")],
    ])
    await msg.reply_text(f"👆 Reklama ko'rinishi shunday. Faol foydalanuvchilar: <b>{count}</b>\nYuborilsinmi?",
                         reply_markup=kb, parse_mode=ParseMode.HTML)


# ---------- Admin holatlari (matn/forward qabul qilish) ----------
async def admin_state_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """True qaytarsa — xabar admin jarayoniga tegishli edi va qayta ishlandi."""
    msg = update.effective_message
    user = update.effective_user
    ud = context.user_data
    state = ud.get("state")
    if not state or not is_admin(user.id):
        return False

    if state == "addpub":
        text = (msg.text or "").strip()
        m = re.search(r"(?:https?://)?t\.me/([A-Za-z0-9_]{4,})/?(?:\d+)?$", text) or re.match(r"^@([A-Za-z0-9_]{4,})$", text)
        if not m or text.lower().startswith(("https://t.me/+", "t.me/+")) or "joinchat" in text.lower():
            await msg.reply_text("❌ Noto'g'ri havola. Ommaviy kanal: https://t.me/kanal_nomi yoki @kanal_nomi")
            return True
        username = m.group(1)
        try:
            chat = await context.bot.get_chat(f"@{username}")
        except TelegramError as e:
            await msg.reply_text(f"❌ Kanal topilmadi: {esc(e)}", parse_mode=ParseMode.HTML)
            return True
        err = await verify_bot_admin(context.bot, chat.id)
        if err:
            await msg.reply_text(err, parse_mode=ParseMode.HTML)
            return True
        db_exec(
            "INSERT INTO channels(chat_id,title,link,kind,request_mode,added_at) VALUES(?,?,?,?,0,?) "
            "ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, link=excluded.link, kind='public', request_mode=0",
            (chat.id, chat.title, f"https://t.me/{username}", "public", now_iso()),
        )
        ud.clear()
        await msg.reply_text(f"✅ Ommaviy kanal qo'shildi: <b>{esc(chat.title)}</b>\n🔗 https://t.me/{esc(username)}",
                             reply_markup=BACK_MARKUP, parse_mode=ParseMode.HTML)
        return True

    if state == "addpriv_fwd":
        chat = forwarded_chat(msg)
        if not chat:
            await msg.reply_text("❌ Kanaldan forward qilingan xabar yuboring.\n"
                                 "(Agar kanal forward'ni yashirgan bo'lsa, boshqa kanal xabarini sinab ko'ring.)")
            return True
        err = await verify_bot_admin(context.bot, chat.id)
        if err:
            await msg.reply_text(err, parse_mode=ParseMode.HTML)
            return True
        ud["pending_ch"] = {"chat_id": chat.id, "title": chat.title or str(chat.id)}
        ud["state"] = "addpriv_link"
        await msg.reply_text(f"✅ Kanal aniqlandi: <b>{esc(chat.title)}</b>\n\nEndi shu kanalning <b>taklif havolasini</b> yuboring "
                             "(<code>https://t.me/+xxxx</code>).", parse_mode=ParseMode.HTML)
        return True

    if state == "addpriv_link":
        text = (msg.text or "").strip()
        if not re.match(r"^(https?://)?t\.me/(\+|joinchat/)[\w-]+/?$", text):
            await msg.reply_text("❌ Noto'g'ri taklif havolasi. Namuna: https://t.me/+AbCdEf123")
            return True
        if not text.startswith("http"):
            text = "https://" + text
        ud["pending_ch"]["link"] = text
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 Oddiy havola", callback_data="adm:pt:0")],
            [InlineKeyboardButton("📨 So'rov orqali qo'shiladigan", callback_data="adm:pt:1")],
        ])
        await msg.reply_text("Bu havola qanday turda?\n\n📨 <i>So'rov orqali</i> — foydalanuvchi so'rov yuborishi kifoya, "
                             "admin tasdiqlamasa ham botdan foydalana oladi.", reply_markup=kb, parse_mode=ParseMode.HTML)
        return True

    if state == "addadm":
        if not is_main_admin(user.id):
            ud.clear()
            await msg.reply_text("⛔️ Faqat asosiy admin adminlarni qo'sha oladi.")
            return True
        target = None
        fu = forwarded_user(msg)
        if fu:
            target = fu.id
        elif msg.text and msg.text.strip().lstrip("-").isdigit():
            target = int(msg.text.strip())
        if not target or target <= 0:
            await msg.reply_text("❌ To'g'ri Telegram ID yuboring (faqat raqam) yoki foydalanuvchi xabarini forward qiling.")
            return True
        if target == MAIN_ADMIN_ID or is_admin(target):
            await msg.reply_text("ℹ️ Bu foydalanuvchi allaqachon admin.")
            ud.clear()
            return True
        db_exec("INSERT OR IGNORE INTO admins(user_id,added_by,added_at) VALUES(?,?,?)", (target, user.id, now_iso()))
        ud.clear()
        await msg.reply_text(f"✅ Admin qo'shildi: <code>{target}</code>", reply_markup=BACK_MARKUP, parse_mode=ParseMode.HTML)
        return True

    if state == "bc_wait":
        ud["bc"] = {"from_chat_id": msg.chat_id, "message_id": msg.message_id, "markup": None}
        ud["state"] = "bc_preview"
        await send_bc_preview(update, context)
        return True

    if state == "bc_buttons":
        markup = parse_buttons(msg.text or "")
        if not markup:
            await msg.reply_text("❌ Format noto'g'ri. Namuna:\n<code>Matn | https://havola.uz</code>", parse_mode=ParseMode.HTML)
            return True
        ud["bc"]["markup"] = markup
        ud["state"] = "bc_preview"
        await send_bc_preview(update, context)
        return True

    return False


async def menu_action(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    msg = update.effective_message
    user = update.effective_user
    ud = context.user_data
    ud.clear()
    if text == BTN_SONG:
        ud["mode"] = "song"
        await msg.reply_text("🔎 Qo'shiq nomi, ijrochisi yoki janrini kiriting — men topaman.")
    elif text == BTN_ARTIST:
        ud["mode"] = "artist"
        await msg.reply_text("🎤 Ijrochi ismini kiriting:")
    elif text == BTN_IDENT:
        await msg.reply_text("🎧 Audio fayl, ovozli xabar yoki Instagram / TikTok / YouTube havolasini yuboring — "
                             "musiqasini aniqlayman.")
    elif text == BTN_CHARTS:
        await show_charts(msg, user.id)
    elif text == BTN_HELP:
        await msg.reply_text(HELP_TEXT.format(bot_username=esc(context.bot.username)), parse_mode=ParseMode.HTML)
    elif text == BTN_ADMIN:
        if is_admin(user.id):
            await msg.reply_text(ADMIN_TEXT, reply_markup=admin_menu_markup(), parse_mode=ParseMode.HTML)


async def private_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    text = (msg.text or "").strip()

    if text in MENU_BUTTONS:
        await menu_action(update, context, text)
        return
    if await admin_state_handler(update, context):
        return
    if msg.audio or msg.voice or msg.document:
        await handle_audio(update, context)
        return
    if not text:
        return

    url = extract_url(text)
    if url:
        if is_social_url(url):
            await handle_link(update, context, url)
        else:
            await msg.reply_text(ERR["link_unsupported"])
        return

    mode = context.user_data.pop("mode", "song")
    if mode == "artist":
        await search_artist(msg, user.id, text)
    else:
        await search_tracks(msg, user.id, text)


# ============================================================
# XATOLAR VA ISHGA TUSHIRISH
# ============================================================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update xatolik keltirdi: %s", context.error, exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(ERR["api_error"])
        except TelegramError:
            pass


async def post_init(application: Application) -> None:
    # Buyruqlar menyusi bo'sh: bot tugmalar va oddiy yozuv bilan ishlaydi
    await application.bot.delete_my_commands()
    logger.info("Bot ishga tushdi: @%s", application.bot.username)


def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # group -1: har bir update oldidan foydalanuvchini saqlash + majburiy obuna tekshiruvi
    app.add_handler(TypeHandler(Update, gate), group=-1)

    app.add_handler(CommandHandler("start", start_command, filters=filters.ChatType.PRIVATE))

    app.add_handler(CallbackQueryHandler(check_sub_callback, pattern=r"^chk$"))
    app.add_handler(CallbackQueryHandler(request_confirm_callback, pattern=r"^req:\d+$"))
    app.add_handler(CallbackQueryHandler(similar_callback, pattern=r"^sim:\d+$"))
    app.add_handler(CallbackQueryHandler(track_select_callback, pattern=r"^tr:\d+$"))
    app.add_handler(CallbackQueryHandler(itunes_select_callback, pattern=r"^it:\d+$"))
    app.add_handler(CallbackQueryHandler(admin_callback_param, pattern=r"^adm:(delch|deladm):-?\d+$"))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern=r"^adm:"))

    app.add_handler(ChatJoinRequestHandler(chat_join_request_handler))
    app.add_handler(InlineQueryHandler(inline_query))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, private_router))

    app.add_error_handler(error_handler)
    return app


def start_health_server() -> None:
    """Render 'web' xizmati port ochishni talab qiladi. PORT berilgan bo'lsagina ishga tushadi."""
    port = os.getenv("PORT")
    if not port:
        return
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class Health(BaseHTTPRequestHandler):
        def _ok(self, body: bool):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            if body:
                self.wfile.write(b"ok")

        def do_GET(self):
            self._ok(True)

        def do_HEAD(self):
            self._ok(False)

        def log_message(self, *args):
            pass

    server = HTTPServer(("0.0.0.0", int(port)), Health)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info("Health-server %s portda ishga tushdi", port)


def main() -> None:
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN topilmadi. .env faylida yoki muhit o'zgaruvchisida BOT_TOKEN=... deb bering.")
        sys.exit(1)
    db_init()
    start_health_server()
    logger.info("Baza: %s | Asosiy admin: %s", DB_PATH, MAIN_ADMIN_ID)
    app = build_app()
    app.run_polling(
        drop_pending_updates=False,
        allowed_updates=["message", "callback_query", "inline_query", "chat_join_request"],
    )


if __name__ == "__main__":
    main()
