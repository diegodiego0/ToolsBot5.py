#!/usr/bin/env python3
"""
MusicBot Pro v5
  ✅ SQLite DB  — /sdcard/Usuário bot Music/musicbot.db
  ✅ /settopic  — configura tópico do grupo
  ✅ Responde APENAS no tópico configurado
  ✅ Downloads rastreados no DB, arquivo apagado após envio
  ✅ Discografia completa com 3 fallbacks
  ✅ Paginação Álbuns ↔ Músicas ↔ Artista
  ✅ Concorrência total (concurrent_updates=True)
"""

import os, sys, subprocess, shutil, asyncio, logging, uuid, random, re, sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from datetime import datetime
from threading import Lock

# ══════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════
BOT_TOKEN     = "8618840827:AAHohLnNTWh_lkP4l9du6KJTaRQcPsNrwV8"
OWNER_ID      = 2061557102
AUDIO_QUALITY = "192"
MAX_BATCH     = 15
ALBUMS_PAGE   = 6
SONGS_PAGE    = 8
MAX_DL_SLOTS  = 8

_CFG: dict = {"dm_on": True}

BASE_DIR   = Path("/sdcard/Usuário bot Music")
DB_PATH    = BASE_DIR / "musicbot.db"
CACHE_DIR  = BASE_DIR / "cache"

_UAS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
]
_rua = lambda: random.choice(_UAS)

# ══════════════════════════════════════════════════════════════
#  BOOTSTRAP
# ══════════════════════════════════════════════════════════════
def _bootstrap():
    import importlib.util
    pkgs = {
        "telegram":   "python-telegram-bot>=20.7",
        "yt_dlp":     "yt-dlp",
        "mutagen":    "mutagen",
        "requests":   "requests",
        "rich":       "rich",
        "ytmusicapi": "ytmusicapi",
        "bs4":        "beautifulsoup4",
    }
    miss = [pip for mod, pip in pkgs.items() if not importlib.util.find_spec(mod)]
    if miss:
        print(f"📦 Instalando: {', '.join(miss)}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", *miss, "-q"])

_bootstrap()

from rich.logging import RichHandler
import requests as rq
import yt_dlp
from bs4 import BeautifulSoup
from ytmusicapi import YTMusic
from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand, InputMediaPhoto,
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from telegram.constants import ParseMode, ChatAction
from telegram.error import BadRequest, RetryAfter, TimedOut

logging.basicConfig(
    level=logging.INFO, format="%(message)s",
    handlers=[RichHandler(show_path=False, rich_tracebacks=True)],
)
log = logging.getLogger("musicbot")
for _n in ("httpx", "telegram", "urllib3", "asyncio"):
    logging.getLogger(_n).setLevel(logging.WARNING)

HAS_FFMPEG = bool(shutil.which("ffmpeg"))
HAS_ARIA2C = bool(shutil.which("aria2c"))
YTM        = YTMusic()

# ══════════════════════════════════════════════════════════════
#  THREAD POOLS SEPARADOS
# ══════════════════════════════════════════════════════════════
_SEARCH_POOL = ThreadPoolExecutor(max_workers=16, thread_name_prefix="search")
_DL_POOL     = ThreadPoolExecutor(max_workers=MAX_DL_SLOTS + 4, thread_name_prefix="dl")
_DL_SEM: asyncio.Semaphore | None = None

async def _run_search(fn, *args):
    return await asyncio.get_event_loop().run_in_executor(_SEARCH_POOL, fn, *args)

async def _run_dl(fn, *args):
    return await asyncio.get_event_loop().run_in_executor(_DL_POOL, fn, *args)

# ══════════════════════════════════════════════════════════════
#  BANCO DE DADOS SQLite
#  Arquivo: /sdcard/Usuário bot Music/musicbot.db
# ══════════════════════════════════════════════════════════════
_DB_LOCK = Lock()

def _db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=15, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # múltiplas leituras simultâneas
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def _db_init():
    """Cria todas as tabelas se não existirem."""
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with _DB_LOCK, _db_conn() as conn:
        conn.executescript("""
        -- ── Usuários ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS users (
            telegram_id  INTEGER PRIMARY KEY,
            full_name    TEXT    NOT NULL DEFAULT '',
            username     TEXT    NOT NULL DEFAULT '',
            first_seen   TEXT    NOT NULL,
            last_seen    TEXT    NOT NULL
        );

        -- ── Tópicos de grupo ──────────────────────────────────────
        -- Cada grupo pode ter UM tópico configurado.
        -- O bot só responde nesse tópico.
        CREATE TABLE IF NOT EXISTS topic_configs (
            chat_id   INTEGER PRIMARY KEY,
            topic_id  INTEGER NOT NULL,   -- message_thread_id do Telegram
            set_by    INTEGER NOT NULL,   -- telegram_id de quem configurou
            set_at    TEXT    NOT NULL
        );

        -- ── Artistas ──────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS artists (
            ytm_id      TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            picture_url TEXT DEFAULT '',
            cached_at   TEXT NOT NULL
        );

        -- ── Álbuns ────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS albums (
            ytm_id      TEXT PRIMARY KEY,
            artist_id   TEXT NOT NULL REFERENCES artists(ytm_id),
            title       TEXT NOT NULL,
            album_type  TEXT DEFAULT 'Album',
            year        TEXT DEFAULT '',
            cover_url   TEXT DEFAULT '',
            cached_at   TEXT NOT NULL
        );

        -- ── Músicas ───────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS tracks (
            video_id     TEXT PRIMARY KEY,
            album_id     TEXT REFERENCES albums(ytm_id),
            artist_id    TEXT REFERENCES artists(ytm_id),
            title        TEXT NOT NULL,
            track_number INTEGER DEFAULT 0,
            duration_sec INTEGER DEFAULT 0,
            cover_url    TEXT DEFAULT ''
        );

        -- ── Log de Downloads ──────────────────────────────────────
        -- Rastreia cada download solicitado por cada usuário.
        -- O arquivo físico é apagado após o envio.
        CREATE TABLE IF NOT EXISTS download_logs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER NOT NULL REFERENCES users(telegram_id),
            video_id     TEXT    DEFAULT '',
            title        TEXT    DEFAULT '',
            artist       TEXT    DEFAULT '',
            album        TEXT    DEFAULT '',
            requested_at TEXT    NOT NULL,
            sent_at      TEXT    DEFAULT '',
            success      INTEGER DEFAULT 0   -- 0=erro, 1=enviado
        );

        -- Índices para consultas frequentes
        CREATE INDEX IF NOT EXISTS idx_dl_user    ON download_logs(user_id);
        CREATE INDEX IF NOT EXISTS idx_dl_video   ON download_logs(video_id);
        CREATE INDEX IF NOT EXISTS idx_albums_art ON albums(artist_id);
        CREATE INDEX IF NOT EXISTS idx_tracks_alb ON tracks(album_id);
        """)
        conn.commit()
    log.info(f"🗄  DB inicializado: {DB_PATH}")

# ── Helpers do DB ──────────────────────────────────────────────
def _db_upsert_user(user) -> None:
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    uid  = user.id
    name = (user.full_name or "").strip()
    un   = (f"@{user.username}" if user.username else "")
    with _DB_LOCK, _db_conn() as conn:
        conn.execute("""
            INSERT INTO users(telegram_id, full_name, username, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                full_name = excluded.full_name,
                username  = excluded.username,
                last_seen = excluded.last_seen
        """, (uid, name, un, now, now))
        conn.commit()

def _db_set_topic(chat_id: int, topic_id: int, set_by: int) -> None:
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    with _DB_LOCK, _db_conn() as conn:
        conn.execute("""
            INSERT INTO topic_configs(chat_id, topic_id, set_by, set_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                topic_id = excluded.topic_id,
                set_by   = excluded.set_by,
                set_at   = excluded.set_at
        """, (chat_id, topic_id, set_by, now))
        conn.commit()

def _db_get_topic(chat_id: int) -> int | None:
    with _db_conn() as conn:
        row = conn.execute(
            "SELECT topic_id FROM topic_configs WHERE chat_id = ?", (chat_id,)
        ).fetchone()
        return row["topic_id"] if row else None

def _db_del_topic(chat_id: int) -> None:
    with _DB_LOCK, _db_conn() as conn:
        conn.execute("DELETE FROM topic_configs WHERE chat_id = ?", (chat_id,))
        conn.commit()

def _db_log_download(user_id: int, meta: dict | None) -> int:
    """Registra um download pendente. Retorna o ID do log."""
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    m   = meta or {}
    with _DB_LOCK, _db_conn() as conn:
        cur = conn.execute("""
            INSERT INTO download_logs
                (user_id, video_id, title, artist, album, requested_at, success)
            VALUES (?, ?, ?, ?, ?, ?, 0)
        """, (user_id, m.get("video_id",""), m.get("title",""),
              m.get("artist",""), m.get("album",""), now))
        conn.commit()
        return cur.lastrowid

def _db_log_sent(log_id: int) -> None:
    """Marca download como enviado com sucesso."""
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    with _DB_LOCK, _db_conn() as conn:
        conn.execute(
            "UPDATE download_logs SET success=1, sent_at=? WHERE id=?",
            (now, log_id))
        conn.commit()

def _db_cache_artist(artist_id: str, name: str, picture: str) -> None:
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    with _DB_LOCK, _db_conn() as conn:
        conn.execute("""
            INSERT INTO artists(ytm_id, name, picture_url, cached_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ytm_id) DO UPDATE SET
                name=excluded.name, picture_url=excluded.picture_url,
                cached_at=excluded.cached_at
        """, (artist_id, name, picture, now))
        conn.commit()

def _db_cache_album(ytm_id: str, artist_id: str, title: str,
                    atype: str, year: str, cover: str) -> None:
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    with _DB_LOCK, _db_conn() as conn:
        conn.execute("""
            INSERT INTO albums(ytm_id, artist_id, title, album_type, year, cover_url, cached_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ytm_id) DO UPDATE SET
                title=excluded.title, album_type=excluded.album_type,
                year=excluded.year, cover_url=excluded.cover_url,
                cached_at=excluded.cached_at
        """, (ytm_id, artist_id, title, atype, year, cover, now))
        conn.commit()

def _db_stats() -> dict:
    with _db_conn() as conn:
        users  = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        tracks = conn.execute("SELECT COUNT(*) FROM download_logs").fetchone()[0]
        ok     = conn.execute(
            "SELECT COUNT(*) FROM download_logs WHERE success=1").fetchone()[0]
        topics = conn.execute("SELECT COUNT(*) FROM topic_configs").fetchone()[0]
        artists = conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0]
        albums  = conn.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
    return {
        "users": users, "downloads": tracks, "sent": ok,
        "topics": topics, "artists": artists, "albums": albums
    }

# ══════════════════════════════════════════════════════════════
#  FILTRO DE TÓPICO
#  Garante que o bot só responde no tópico configurado no grupo
# ══════════════════════════════════════════════════════════════
async def _topic_allowed(update: Update) -> bool:
    """
    Retorna True se o update deve ser processado.
    - DM/canal sem tópico → sempre True
    - Grupo com tópico configurado → True somente se
      message.message_thread_id == topic_id configurado
    """
    chat = update.effective_chat
    if not chat or chat.type not in ("group", "supergroup"):
        return True   # DMs sempre liberadas

    msg       = update.effective_message
    thread_id = msg.message_thread_id if msg else None

    # Busca no DB (síncrono via executor pra não bloquear)
    configured = await _run_search(_db_get_topic, chat.id)

    if configured is None:
        # Sem tópico configurado → responde em qualquer lugar do grupo
        return True

    return thread_id == configured

def _thread_id(update: Update) -> int | None:
    """Retorna o message_thread_id do update atual."""
    msg = update.effective_message
    return msg.message_thread_id if msg else None

# ══════════════════════════════════════════════════════════════
#  PASTAS
# ══════════════════════════════════════════════════════════════
def _ensure_dirs():
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _work_dir() -> Path:
    d = CACHE_DIR / uuid.uuid4().hex
    d.mkdir(parents=True, exist_ok=True)
    return d

def _cleanup(path) -> None:
    try:
        p      = Path(path)
        parent = p.parent
        if parent != CACHE_DIR and parent.exists():
            shutil.rmtree(parent, ignore_errors=True)
        elif p.is_file():
            p.unlink(missing_ok=True)
    except Exception as e:
        log.debug(f"cleanup: {e}")

# ══════════════════════════════════════════════════════════════
#  SESSION CACHE  (chave curta → dados pesados)
# ══════════════════════════════════════════════════════════════
_SESSION: dict[str, dict] = {}

def cs(data: dict) -> str:
    k = uuid.uuid4().hex[:12]
    _SESSION[k] = data
    return k

def cg(k: str) -> dict | None:
    return _SESSION.get(k)

# ══════════════════════════════════════════════════════════════
#  HELPERS GERAIS
# ══════════════════════════════════════════════════════════════
def _thumb(thumbs: list, min_w: int = 300) -> str:
    if not thumbs: return ""
    pool = [t for t in thumbs if t.get("width", 0) >= min_w] or thumbs
    return max(pool, key=lambda t: t.get("width", 0)).get("url", "")

def _cut(t: str, n: int = 36) -> str:
    t = str(t or "")
    return t if len(t) <= n else t[:n-1] + "…"

def _sec(s) -> str:
    if not s: return "?:??"
    h, r = divmod(int(s), 3600); m, s = divmod(r, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

def _ico(tp: str) -> str:
    return {"single": "🎵", "ep": "📀"}.get(str(tp or "").lower(), "💿")

def _is_owner(u: Update) -> bool:
    return bool(u.effective_user and u.effective_user.id == OWNER_ID)

def _dm_ok(u: Update) -> bool:
    return True if _is_owner(u) else _CFG["dm_on"]

# ══════════════════════════════════════════════════════════════
#  RENDERIZADOR CENTRAL
#  Edita mensagem se edit=True, senão envia nova.
#  Passa message_thread_id quando necessário.
# ══════════════════════════════════════════════════════════════
async def _render(
    target,
    photo:     str,
    caption:   str,
    kb:        InlineKeyboardMarkup,
    edit:      bool = False,
    thread_id: int | None = None,
) -> None:
    # ── Editar mensagem existente ─────────────────────────
    if edit and hasattr(target, "chat"):
        try:
            if photo:
                await target.edit_media(
                    InputMediaPhoto(media=photo, caption=caption,
                                    parse_mode=ParseMode.HTML),
                    reply_markup=kb,
                )
                return
            try:
                await target.edit_caption(caption, reply_markup=kb,
                                          parse_mode=ParseMode.HTML)
                return
            except BadRequest:
                await target.edit_text(caption, reply_markup=kb,
                                       parse_mode=ParseMode.HTML)
                return
        except Exception as e:
            log.debug(f"_render edit falhou ({type(e).__name__})")

    # ── Enviar nova mensagem ──────────────────────────────
    extra = {}
    if thread_id:
        extra["message_thread_id"] = thread_id

    if hasattr(target, "reply_photo"):
        # target = Message  (reply herda o thread automaticamente)
        if photo:
            try:
                await target.reply_photo(photo=photo, caption=caption,
                                         reply_markup=kb, parse_mode=ParseMode.HTML)
                return
            except Exception: pass
        await target.reply_text(caption, reply_markup=kb, parse_mode=ParseMode.HTML)
    elif isinstance(target, tuple):
        # target = (bot, chat_id)
        bot, chat_id = target
        if photo:
            try:
                await bot.send_photo(chat_id, photo=photo, caption=caption,
                                     reply_markup=kb, parse_mode=ParseMode.HTML,
                                     **extra)
                return
            except Exception: pass
        await bot.send_message(chat_id, caption, reply_markup=kb,
                               parse_mode=ParseMode.HTML, **extra)

# ══════════════════════════════════════════════════════════════
#  YOUTUBE MUSIC  —  Busca completa de artista (3 fallbacks)
# ══════════════════════════════════════════════════════════════
def _fetch_artist_full(artist_id: str) -> dict:
    info     = YTM.get_artist(artist_id)
    releases: list[dict] = []
    seen:     set[str]   = set()

    def _add(items: list, tag: str):
        for item in (items or []):
            bid = (item.get("browseId") or item.get("playlistId") or "").strip()
            if bid and bid not in seen:
                seen.add(bid)
                r = dict(item)
                r["type"] = r.get("type") or tag
                releases.append(r)

    def _fetch_section(key: str, default_type: str):
        sec     = info.get(key) or {}
        browse  = (sec.get("browseId")   or "").strip()
        params  = (sec.get("params")     or "").strip()
        channel = (info.get("channelId") or "").strip()

        # Tentativa 1: browseId da seção + params
        if browse and params:
            try:
                r = YTM.get_artist_albums(browse, params) or []
                log.info(f"  [{key}] ✅ {len(r)} via browseId+params")
                _add(r, default_type); return
            except Exception as e:
                log.warning(f"  [{key}] browseId+params: {e}")

        # Tentativa 2: channelId principal + params da seção
        if channel and params:
            try:
                r = YTM.get_artist_albums(channel, params) or []
                log.info(f"  [{key}] ✅ {len(r)} via channelId+params")
                _add(r, default_type); return
            except Exception as e:
                log.warning(f"  [{key}] channelId+params: {e}")

        # Tentativa 3: results[] embutido (limitado, mas algo)
        results = sec.get("results") or []
        log.warning(f"  [{key}] ⚠ results[] fallback ({len(results)} itens)")
        _add(results, default_type)

    log.info(f"🔍 Discografia: {info.get('name', artist_id)}")
    _fetch_section("albums",  "Album")
    _fetch_section("singles", "Single")
    _fetch_section("eps",     "EP")

    songs   = (info.get("songs") or {}).get("results") or []
    name    = info.get("name", "?")
    picture = _thumb(info.get("thumbnails", []), 400)

    # Persiste no DB em background
    try:
        _db_cache_artist(artist_id, name, picture)
        for rel in releases:
            bid = rel.get("browseId", "")
            if bid:
                _db_cache_album(bid, artist_id, rel.get("title",""),
                                rel.get("type","Album"), rel.get("year",""),
                                _thumb(rel.get("thumbnails",[])))
    except Exception as e:
        log.debug(f"_db_cache: {e}")

    log.info(f"✅ {name} → {len(releases)} lançamentos | {len(songs)} músicas")
    return {
        "info":     info,
        "releases": releases,
        "songs":    songs,
        "picture":  picture,
        "name":     name,
    }

def _search(q: str) -> dict:
    out = {"artists": [], "albums": [], "songs": []}
    try: out["artists"] = YTM.search(q, filter="artists", limit=5)  or []
    except Exception: pass
    try: out["albums"]  = YTM.search(q, filter="albums",  limit=10) or []
    except Exception: pass
    try: out["songs"]   = YTM.search(q, filter="songs",   limit=10) or []
    except Exception: pass
    return out

def _get_album(bid: str) -> dict:
    return YTM.get_album(bid)

# ══════════════════════════════════════════════════════════════
#  HTML FALLBACK
# ══════════════════════════════════════════════════════════════
def _yt_html_search(q: str) -> str | None:
    try:
        r = rq.get(
            f"https://www.youtube.com/results"
            f"?search_query={rq.utils.quote(q)}&sp=EgIQAQ%3D%3D",
            headers={"User-Agent": _rua(), "Accept-Language": "en-US,en;q=0.9"},
            timeout=12,
        )
        if not r.ok: return None
        for s in BeautifulSoup(r.text, "html.parser").find_all("script"):
            m = re.search(r'"videoId"\s*:\s*"([a-zA-Z0-9_-]{11})"', s.string or "")
            if m: return f"https://www.youtube.com/watch?v={m.group(1)}"
    except Exception as e:
        log.warning(f"html_search: {e}")
    return None

# ══════════════════════════════════════════════════════════════
#  DOWNLOADER
# ══════════════════════════════════════════════════════════════
_STRATEGIES: list[tuple[list, str]] = [
    (["ios"],     "bestaudio/best"),
    (["ios"],     "best"),
    (["android"], "bestaudio/best"),
    (["android"], "best"),
    (["mweb"],    "bestaudio/best"),
    (["web"],     "bestaudio/best"),
    (["web"],     "best"),
]

def _build_opts(out_dir: str, fmt: str, clients: list) -> dict:
    o: dict = {
        "format":           fmt,
        "outtmpl":          str(Path(out_dir) / "%(id)s.%(ext)s"),
        "quiet":            True,
        "no_warnings":      True,
        "noplaylist":       True,
        "cachedir":         False,
        "age_limit":        99,
        "retries":          3,
        "fragment_retries": 5,
        "socket_timeout":   20,
        "extractor_args":   {
            "youtube": {"player_client": clients, "player_skip": ["webpage"]}
        },
        "http_headers": {"User-Agent": _rua(), "Accept-Language": "en-US,en;q=0.9"},
    }
    if HAS_ARIA2C:
        o["external_downloader"] = "aria2c"
        o["external_downloader_args"] = {
            "default": ["-x16", "-k1M", "--min-split-size=1M",
                        "-j16", "--quiet", "--no-conf"]
        }
    if HAS_FFMPEG:
        o["postprocessors"] = [{
            "key":              "FFmpegExtractAudio",
            "preferredcodec":   "mp3",
            "preferredquality": AUDIO_QUALITY,
        }]
    return o

def _locate_file(raw: str) -> str | None:
    stem = os.path.splitext(raw)[0]
    for ext in (".mp3", ".m4a", ".opus", ".webm", ".ogg", ".aac"):
        if os.path.isfile(stem + ext): return stem + ext
    return raw if os.path.isfile(raw) else None

def _try_download(target: str, out_dir: str) -> tuple[dict | None, str]:
    last_err  = "Erro desconhecido"
    captured: list[str] = []

    def _hook(d):
        if d["status"] == "finished":
            captured.append(d.get("filename", ""))

    for clients, fmt in _STRATEGIES:
        captured.clear()
        opts = _build_opts(out_dir, fmt, clients)
        opts["progress_hooks"] = [_hook]
        tag = f"{clients[0]}/{fmt[:12]}"
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info  = ydl.extract_info(target, download=True)
                if not info: last_err = "Sem resultado"; continue
                entry = info["entries"][0] if info.get("entries") else info
                raw   = captured[0] if captured else ydl.prepare_filename(entry)
                found = _locate_file(raw)
                if found:
                    log.info(f"✅ [{tag}] {Path(found).name}")
                    return entry, found
                last_err = "Arquivo não encontrado"
        except yt_dlp.utils.DownloadError as e:
            last_err = str(e).split("\n")[0][:180]
            log.warning(f"⚠ [{tag}] {last_err[:80]}")
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            log.warning(f"⚠ [{tag}] {last_err[:80]}")
    return None, last_err

def _dl(target: str, out_dir: str, meta: dict | None) -> dict:
    try:
        c = Path.home() / ".cache" / "yt-dlp"
        if c.exists(): shutil.rmtree(c, ignore_errors=True)
    except Exception: pass

    entry, result = _try_download(target, out_dir)

    if entry is None and target.startswith("https://"):
        t = (meta or {}).get("title",""); a = (meta or {}).get("artist","")
        if t:
            entry, result = _try_download(f"ytsearch1:{t} {a} audio".strip(), out_dir)

    if entry is None:
        t = (meta or {}).get("title",""); a = (meta or {}).get("artist","")
        if t:
            url = _yt_html_search(f"{t} {a} official audio")
            if url: entry, result = _try_download(url, out_dir)

    if entry is None:
        return {"ok": False, "error": result}

    if meta:
        try: _tag(result, meta)
        except Exception as e: log.warning(f"tag: {e}")

    return {
        "ok":     True,
        "file":   result,
        "title":  (meta or {}).get("title",  "") or entry.get("title",    ""),
        "artist": (meta or {}).get("artist", "") or "",
        "dur":    (meta or {}).get("dur",    0)  or entry.get("duration", 0),
    }

# ══════════════════════════════════════════════════════════════
#  TAGGER
# ══════════════════════════════════════════════════════════════
def _fetch_img(url: str) -> bytes | None:
    try:
        r = rq.get(url, timeout=8, headers={"User-Agent": _rua()})
        return r.content if r.ok else None
    except Exception: return None

def _tag(path: str, m: dict):
    ext   = Path(path).suffix.lower()
    cover = _fetch_img(m["cover"]) if m.get("cover") else None
    if   ext == ".mp3":           _tag_mp3(path, m, cover)
    elif ext in (".m4a", ".aac"): _tag_m4a(path, m, cover)
    elif ext == ".opus":          _tag_opus(path, m)

def _tag_mp3(path, m, cover):
    from mutagen.id3 import ID3, ID3NoHeaderError, TIT2, TPE1, TALB, TRCK, TDRC, APIC
    try:
        try:   t = ID3(path)
        except ID3NoHeaderError: t = ID3()
        if m.get("title"):  t["TIT2"] = TIT2(encoding=3, text=m["title"])
        if m.get("artist"): t["TPE1"] = TPE1(encoding=3, text=m["artist"])
        if m.get("album"):  t["TALB"] = TALB(encoding=3, text=m["album"])
        if m.get("track"):  t["TRCK"] = TRCK(encoding=3, text=str(m["track"]))
        if m.get("year"):   t["TDRC"] = TDRC(encoding=3, text=m["year"])
        if cover:
            t["APIC"] = APIC(encoding=3, mime="image/jpeg",
                             type=3, desc="Cover", data=cover)
        t.save(path, v2_version=3)
    except Exception as e: log.warning(f"tag_mp3: {e}")

def _tag_m4a(path, m, cover):
    from mutagen.mp4 import MP4, MP4Cover
    try:
        t = MP4(path)
        if m.get("title"):  t["\xa9nam"] = [m["title"]]
        if m.get("artist"): t["\xa9ART"] = [m["artist"]]
        if m.get("album"):  t["\xa9alb"] = [m["album"]]
        if m.get("year"):   t["\xa9day"] = [m["year"]]
        if m.get("track"):  t["trkn"]    = [(int(m["track"]), 0)]
        if cover:           t["covr"]    = [MP4Cover(cover, MP4Cover.FORMAT_JPEG)]
        t.save()
    except Exception as e: log.warning(f"tag_m4a: {e}")

def _tag_opus(path, m):
    from mutagen.oggopus import OggOpus
    try:
        t = OggOpus(path)
        for k, v in [("title", m.get("title")), ("artist", m.get("artist")),
                     ("album", m.get("album")),  ("date",   m.get("year")),
                     ("tracknumber", str(m.get("track", "")))]:
            if v: t[k] = [v]
        t.save()
    except Exception as e: log.warning(f"tag_opus: {e}")

# ══════════════════════════════════════════════════════════════
#  ENVIO DE ÁUDIO
#  Arquivo é SEMPRE apagado no finally (seja sucesso ou erro)
# ══════════════════════════════════════════════════════════════
async def _send_audio(
    bot, chat_id: int, res: dict,
    thread_id: int | None = None,
    log_id:    int | None = None,
) -> bool:
    path = res.get("file", "")
    if not path or not os.path.isfile(path):
        log.error(f"_send_audio: arquivo ausente → {path}")
        return False
    try:
        size_mb = os.path.getsize(path) / 1024 / 1024
        if size_mb > 49:
            await bot.send_message(
                chat_id,
                f"⚠️ <b>{_cut(res.get('title',''))}</b> — "
                f"arquivo muito grande ({size_mb:.1f} MB).",
                parse_mode=ParseMode.HTML,
                **( {"message_thread_id": thread_id} if thread_id else {} ),
            )
            return False
        await bot.send_chat_action(chat_id, ChatAction.UPLOAD_VOICE)
        with open(path, "rb") as f:
            await bot.send_audio(
                chat_id, audio=f,
                title=res.get("title", ""),
                performer=res.get("artist", ""),
                duration=int(res.get("dur") or 0),
                read_timeout=180, write_timeout=180, connect_timeout=30,
                **( {"message_thread_id": thread_id} if thread_id else {} ),
            )
        # Marca como enviado no DB
        if log_id:
            await _run_search(_db_log_sent, log_id)
        return True
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
        return await _send_audio(bot, chat_id, res, thread_id, log_id)
    except TimedOut:
        log.warning("send_audio: timeout")
        return False
    finally:
        _cleanup(path)   # ← arquivo SEMPRE apagado

# ══════════════════════════════════════════════════════════════
#  DOWNLOAD COM SEMÁFORO
# ══════════════════════════════════════════════════════════════
async def _dl_task(target: str, meta: dict | None) -> dict:
    assert _DL_SEM is not None
    async with _DL_SEM:
        work = _work_dir()
        try:
            return await _run_dl(_dl, target, str(work), meta)
        except Exception as e:
            _cleanup(work)
            return {"ok": False, "error": str(e)}

# ══════════════════════════════════════════════════════════════
#  BATCH PARALELO
# ══════════════════════════════════════════════════════════════
async def _batch(
    bot, chat_id: int, status_msg,
    items: list[tuple], label: str,
    user_id: int = 0,
    thread_id: int | None = None,
):
    total = len(items)
    ok = err = 0
    tasks = [asyncio.create_task(_dl_task(url, meta)) for url, _, meta in items]

    for i, (task, (_, display, meta)) in enumerate(zip(tasks, items), 1):
        bar = "█" * round((i-1)/total*10) + "░" * (10 - round((i-1)/total*10))
        try:
            await status_msg.edit_text(
                f"⏬  <b>{_cut(label, 40)}</b>\n\n"
                f"🎵  <code>{_cut(display, 46)}</code>\n"
                f"<code>[{bar}]</code>  {i}/{total}  ({round((i-1)/total*100)}%)",
                parse_mode=ParseMode.HTML,
            )
        except Exception: pass

        log_id = 0
        if user_id:
            log_id = await _run_search(_db_log_download, user_id, meta)

        await bot.send_chat_action(chat_id, ChatAction.UPLOAD_VOICE)
        res = await task

        if res["ok"]:
            sent = await _send_audio(bot, chat_id, res, thread_id, log_id)
            ok  += sent; err += (not sent)
        else:
            err += 1
            try:
                await bot.send_message(
                    chat_id,
                    f"❌  <code>{_cut(display)}</code>\n"
                    f"<i>{_cut(res['error'], 120)}</i>",
                    parse_mode=ParseMode.HTML,
                    **( {"message_thread_id": thread_id} if thread_id else {} ),
                )
            except Exception: pass

    icon = "✅" if not err else ("⚠️" if ok else "❌")
    try:
        await status_msg.edit_text(
            f"{icon}  <b>{_cut(label, 40)}</b>\n"
            f"✅ {ok} enviada(s)" + (f"   ❌ {err} erro(s)" if err else ""),
            parse_mode=ParseMode.HTML,
        )
    except Exception: pass

# ══════════════════════════════════════════════════════════════
#  TECLADOS
# ══════════════════════════════════════════════════════════════
def _kb_settings() -> InlineKeyboardMarkup:
    lbl = "🟢  Buscas ATIVAS" if _CFG["dm_on"] else "🔴  Buscas INATIVAS"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(lbl, callback_data="CFG:dm")],
        [InlineKeyboardButton("✖️  Fechar", callback_data="DEL")],
    ])

def _kb_search_results(results: dict) -> InlineKeyboardMarkup:
    rows = []
    for al in results.get("albums", [])[:4]:
        bid = al.get("browseId", "")
        if not bid: continue
        k = cs({"bid": bid, "cover": _thumb(al.get("thumbnails", []))})
        year = al.get("year", "")
        rows.append([InlineKeyboardButton(
            f"{_ico(al.get('type','Album'))}  "
            f"{_cut(al.get('title','?'), 28)}"
            + (f"  [{year}]" if year else ""),
            callback_data=f"SA:{k}",
        )])
    for sg in results.get("songs", [])[:4]:
        vid = sg.get("videoId", "")
        if not vid: continue
        title  = sg.get("title", "?")
        artist = (sg.get("artists") or [{}])[0].get("name", "")
        k = cs({
            "video_id": vid, "title": title, "artist": artist,
            "album":    (sg.get("album") or {}).get("name", ""),
            "cover":    _thumb(sg.get("thumbnails", [])),
            "dur":      sg.get("duration_seconds", 0),
            "track": "", "year": "",
        })
        rows.append([InlineKeyboardButton(
            f"🎵  {_cut(title, 26)}  —  {_cut(artist, 16)}",
            callback_data=f"SD:{k}",
        )])
    rows.append([InlineKeyboardButton("✖️  Fechar", callback_data="DEL")])
    return InlineKeyboardMarkup(rows)

def _kb_artist_card(artist_key: str, n_rel: int, n_songs: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"💿  Álbuns  ({n_rel})",
                                 callback_data=f"AL:{artist_key}:0"),
            InlineKeyboardButton(f"🎵  Músicas  ({n_songs})",
                                 callback_data=f"SG:{artist_key}:0"),
        ],
        [InlineKeyboardButton("✖️  Fechar", callback_data="DEL")],
    ])

def _kb_albums(
    page_items: list, artist_key: str,
    page: int, total: int, n_songs: int,
) -> InlineKeyboardMarkup:
    rows        = []
    total_pages = max(1, -(-total // ALBUMS_PAGE))
    for rel in page_items:
        bid = rel.get("browseId", "")
        if not bid: continue
        k = cs({"album_id": bid, "cover": _thumb(rel.get("thumbnails", []), 400),
                "artist_key": artist_key, "page": page})
        year  = rel.get("year", "????")
        rtype = rel.get("type", "Album")
        rows.append([InlineKeyboardButton(
            f"{_ico(rtype)}  {_cut(rel.get('title','?'), 28)}  [{year}]",
            callback_data=f"TR:{k}",
        )])
    # Paginação
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"AL:{artist_key}:{page-1}"))
    nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="NOP"))
    if (page + 1) * ALBUMS_PAGE < total:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"AL:{artist_key}:{page+1}"))
    rows.append(nav)
    # Navegação entre sections
    rows.append([
        InlineKeyboardButton("🔙  Artista",       callback_data=f"AK:{artist_key}"),
        InlineKeyboardButton(f"🎵  Músicas ({n_songs})",
                             callback_data=f"SG:{artist_key}:0"),
        InlineKeyboardButton("✖️  Fechar",         callback_data="DEL"),
    ])
    return InlineKeyboardMarkup(rows)

def _kb_songs(
    page_items: list, artist_key: str,
    page: int, total: int,
    artist_name: str, cover: str, n_rel: int,
) -> InlineKeyboardMarkup:
    rows        = []
    total_pages = max(1, -(-total // SONGS_PAGE))
    for sg in page_items:
        vid = sg.get("videoId", "")
        if not vid: continue
        title = sg.get("title", "?")
        dur   = _sec(sg.get("duration_seconds", 0) or sg.get("duration", 0))
        k = cs({
            "video_id": vid, "title": title, "artist": artist_name,
            "album":    (sg.get("album") or {}).get("name", ""),
            "cover":    cover,
            "dur":      sg.get("duration_seconds", 0),
            "track": "", "year": "",
        })
        rows.append([InlineKeyboardButton(
            f"🎵  {_cut(title, 28)}  [{dur}]",
            callback_data=f"DL:{k}",
        )])
    # Paginação
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"SG:{artist_key}:{page-1}"))
    nav.append(InlineKeyboardButton(f"📄 {page+1}/{total_pages}", callback_data="NOP"))
    if (page + 1) * SONGS_PAGE < total:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"SG:{artist_key}:{page+1}"))
    rows.append(nav)
    # Navegação entre sections
    rows.append([
        InlineKeyboardButton("🔙  Artista",      callback_data=f"AK:{artist_key}"),
        InlineKeyboardButton(f"💿  Álbuns ({n_rel})",
                             callback_data=f"AL:{artist_key}:0"),
        InlineKeyboardButton("✖️  Fechar",        callback_data="DEL"),
    ])
    return InlineKeyboardMarkup(rows)

def _kb_tracks(album_info: dict, album_key: str) -> InlineKeyboardMarkup:
    data        = cg(album_key) or {}
    artist_key  = data.get("artist_key", "")
    page        = data.get("page", 0)
    artist_name = (album_info.get("artists") or [{}])[0].get("name", "")
    album_title = album_info.get("title", "")
    year        = album_info.get("year", "")
    cover       = _thumb(album_info.get("thumbnails", []))
    tracks      = album_info.get("tracks") or []
    valid       = [t for t in tracks
                   if t.get("videoId") and t.get("isAvailable", True)]

    rows = []
    all_t = [
        {"video_id": t["videoId"], "title": t.get("title",""),
         "artist":   artist_name,  "album": album_title,
         "track":    t.get("trackNumber") or i+1,
         "year":     year, "cover": cover,
         "dur":      t.get("duration_seconds", 0)}
        for i, t in enumerate(valid)
    ]
    ak = cs({"tracks": all_t, "label": f"💿  {album_title}"})
    rows.append([InlineKeyboardButton(
        f"⬇️  Baixar álbum completo  ({len(valid)} faixas)",
        callback_data=f"DAL:{ak}",
    )])
    rows.append([InlineKeyboardButton("─" * 22, callback_data="NOP")])

    for i, t in enumerate(valid):
        pos = t.get("trackNumber") or i + 1
        dur = t.get("duration") or _sec(t.get("duration_seconds", 0))
        k   = cs({
            "video_id": t["videoId"], "title": t.get("title",""),
            "artist":   artist_name,  "album": album_title,
            "track":    pos, "year": year, "cover": cover,
            "dur":      t.get("duration_seconds", 0),
        })
        rows.append([InlineKeyboardButton(
            f"🎵  {pos:02d}. {_cut(t.get('title',''), 26)}  [{dur}]",
            callback_data=f"DL:{k}",
        )])

    nav = []
    if artist_key:
        nav.append(InlineKeyboardButton("🔙  Álbuns",
                                        callback_data=f"AL:{artist_key}:{page}"))
    nav.append(InlineKeyboardButton("✖️  Fechar", callback_data="DEL"))
    rows.append(nav)
    return InlineKeyboardMarkup(rows)

# ══════════════════════════════════════════════════════════════
#  LOAD ARTISTA (lazy)
# ══════════════════════════════════════════════════════════════
async def _load_artist(artist_key: str) -> dict | None:
    data = cg(artist_key)
    if not data: return None
    if data.get("_loaded"): return data
    try:
        full = await _run_search(_fetch_artist_full, data["artist_id"])
        data.update({
            "_loaded":     True,
            "_releases":   full["releases"],
            "_songs":      full["songs"],
            "_info":       full["info"],
            "picture":     data.get("picture") or full["picture"],
            "artist_name": data.get("artist_name") or full["name"],
        })
        _SESSION[artist_key] = data
        return data
    except Exception as e:
        log.error(f"_load_artist: {e}"); return None

# ══════════════════════════════════════════════════════════════
#  SHOW FUNCTIONS
# ══════════════════════════════════════════════════════════════
async def _show_artist_card(target, artist_key: str,
                            edit: bool = False, thread_id: int | None = None):
    data = await _load_artist(artist_key)
    if not data:
        if hasattr(target, "reply_text"):
            await target.reply_text("❌ Sessão expirada. Busque novamente.")
        return
    releases   = data.get("_releases", [])
    songs      = data.get("_songs", [])
    info       = data.get("_info", {})
    picture    = data.get("picture", "")
    name       = data.get("artist_name", "?")
    desc       = (info.get("description") or "")[:220]
    views      = info.get("views", "")
    n_albums   = sum(1 for r in releases if r.get("type","").lower() == "album")
    n_singles  = sum(1 for r in releases if r.get("type","").lower() == "single")
    n_eps      = sum(1 for r in releases if r.get("type","").lower() == "ep")
    lines = [f"🎤  <b>{name}</b>"]
    if views: lines.append(f"👁  {views}")
    parts = []
    if n_albums:  parts.append(f"💿 {n_albums} álbum(ns)")
    if n_eps:     parts.append(f"📀 {n_eps} EP(s)")
    if n_singles: parts.append(f"🎵 {n_singles} single(s)")
    if parts: lines.append("  ·  ".join(parts))
    if desc:  lines.append(f"\n<i>{desc}…</i>")
    lines.append("\n\nEscolha uma seção:")
    kb = _kb_artist_card(artist_key, len(releases), len(songs))
    await _render(target, picture, "\n".join(lines), kb, edit=edit, thread_id=thread_id)

async def _show_albums(target, artist_key: str, page: int,
                       edit: bool = True, thread_id: int | None = None):
    data = await _load_artist(artist_key)
    if not data:
        if hasattr(target, "reply_text"): await target.reply_text("❌ Sessão expirada.")
        return
    releases    = data.get("_releases", [])
    songs       = data.get("_songs", [])
    total       = len(releases)
    page_items  = releases[page*ALBUMS_PAGE:(page+1)*ALBUMS_PAGE]
    total_pages = max(1, -(-total // ALBUMS_PAGE))
    picture     = data.get("picture", "")
    name        = data.get("artist_name", "?")
    n_albums    = sum(1 for r in releases if r.get("type","").lower() == "album")
    n_singles   = sum(1 for r in releases if r.get("type","").lower() == "single")
    n_eps       = sum(1 for r in releases if r.get("type","").lower() == "ep")
    caption = (
        f"🎤  <b>{name}</b>  —  Discografia\n\n"
        f"💿 {n_albums} álbum(ns)  ·  📀 {n_eps} EP(s)  ·  🎵 {n_singles} single(s)\n"
        f"<i>{total} lançamentos  ·  pág. {page+1}/{total_pages}</i>\n\n"
        f"Selecione para ver as faixas:"
    )
    kb = _kb_albums(page_items, artist_key, page, total, len(songs))
    await _render(target, picture, caption, kb, edit=edit, thread_id=thread_id)

async def _show_songs(target, artist_key: str, page: int,
                      edit: bool = True, thread_id: int | None = None):
    data = await _load_artist(artist_key)
    if not data:
        if hasattr(target, "reply_text"): await target.reply_text("❌ Sessão expirada.")
        return
    songs       = data.get("_songs", [])
    releases    = data.get("_releases", [])
    total       = len(songs)
    page_items  = songs[page*SONGS_PAGE:(page+1)*SONGS_PAGE]
    total_pages = max(1, -(-total // SONGS_PAGE))
    picture     = data.get("picture", "")
    name        = data.get("artist_name", "?")
    if not songs:
        caption = (
            f"🎤  <b>{name}</b>  —  Músicas\n\n"
            f"😕  Nenhuma música disponível via API.\n"
            f"Tente buscar diretamente pelo nome da música."
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙  Artista",        callback_data=f"AK:{artist_key}"),
            InlineKeyboardButton(f"💿  Álbuns ({len(releases)})",
                                 callback_data=f"AL:{artist_key}:0"),
            InlineKeyboardButton("✖️  Fechar",          callback_data="DEL"),
        ]])
        await _render(target, picture, caption, kb, edit=edit, thread_id=thread_id)
        return
    caption = (
        f"🎤  <b>{name}</b>  —  Músicas\n\n"
        f"<i>{total} faixas  ·  pág. {page+1}/{total_pages}</i>\n\n"
        f"Toque em uma música para baixar:"
    )
    kb = _kb_songs(page_items, artist_key, page, total, name, picture, len(releases))
    await _render(target, picture, caption, kb, edit=edit, thread_id=thread_id)

async def _show_tracks(target, album_id: str, cover_fb: str,
                       artist_key: str, page: int,
                       edit: bool = True, thread_id: int | None = None):
    try:
        info = await _run_search(_get_album, album_id)
    except Exception as e:
        if hasattr(target, "reply_text"):
            await target.reply_text(f"❌ Erro ao carregar álbum: {e}")
        return
    tracks = info.get("tracks") or []
    if not tracks:
        if hasattr(target, "reply_text"):
            await target.reply_text("😕 Sem faixas disponíveis.")
        return
    atype       = info.get("type", "Álbum")
    artist_name = (info.get("artists") or [{}])[0].get("name", "")
    title       = info.get("title", "")
    year        = info.get("year", "")
    duration    = info.get("duration", "")
    tc          = info.get("trackCount") or len(tracks)
    cover       = _thumb(info.get("thumbnails", [])) or cover_fb
    valid       = [t for t in tracks
                   if t.get("videoId") and t.get("isAvailable", True)]
    unavail     = len(tracks) - len(valid)
    album_key   = cs({"album_id": album_id, "cover": cover,
                      "artist_key": artist_key, "page": page})
    caption = (
        f"{_ico(atype)}  <b>{title}</b>\n\n"
        f"🎤  {artist_name}\n"
        f"🗓  {year}   ·   {atype}\n"
        f"🎵  {tc} faixas"
        + (f"  ·  ⏱ {duration}" if duration else "")
        + (f"\n⚠️  {unavail} faixa(s) indisponível(is)" if unavail else "")
        + f"\n\n✅ {len(valid)} disponíveis para download:"
    )
    kb = _kb_tracks(info, album_key)
    await _render(target, cover, caption, kb, edit=edit, thread_id=thread_id)

# ══════════════════════════════════════════════════════════════
#  BUSCA
# ══════════════════════════════════════════════════════════════
async def _do_search(message, query: str, thread_id: int | None = None):
    status  = await message.reply_text(
        f"🔍  Buscando  <b>{_cut(query, 40)}</b>…",
        parse_mode=ParseMode.HTML)
    results = await _run_search(_search, query)
    try: await status.delete()
    except Exception: pass

    artists = results.get("artists", [])
    albums  = results.get("albums",  [])
    songs   = results.get("songs",   [])

    if not any([artists, albums, songs]):
        await message.reply_text("😕 Nada encontrado. Tente outro termo.")
        return

    if artists:
        a = artists[0]
        k = cs({
            "artist_id":   a["browseId"],
            "artist_name": a.get("artist") or a.get("name") or "?",
            "picture":     _thumb(a.get("thumbnails", [])),
        })
        await _show_artist_card(message, k, edit=False, thread_id=thread_id)
        return

    if albums and not songs:
        al = albums[0]; bid = al.get("browseId", "")
        if bid:
            await _show_tracks(message, bid, _thumb(al.get("thumbnails",[])),
                               "", 0, edit=False, thread_id=thread_id)
            return

    await message.reply_text(
        f"🔍  <b>{_cut(query, 40)}</b>\n\n"
        f"💿 = álbum/EP   🎵 = música\n\nSelecione:",
        reply_markup=_kb_search_results(results),
        parse_mode=ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════════════
#  COMANDOS
# ══════════════════════════════════════════════════════════════
async def cmd_start(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _topic_allowed(u): return
    if u.effective_user:
        await _run_search(_db_upsert_user, u.effective_user)
    name = (u.effective_user.first_name or "você") if u.effective_user else "você"
    await u.message.reply_text(
        f"🎵  Olá, <b>{name}</b>!\n\n"
        f"Digite para buscar:\n"
        f"  🎤  artista  →  card com Álbuns e Músicas\n"
        f"  💿  álbum / EP  →  faixas\n"
        f"  🎵  música  →  download\n\n"
        f"<b>Múltiplas músicas</b> — separe por vírgula:\n"
        f"<code>música 1, música 2</code>\n\n"
        f"<code>/buscar nome</code>  →  busca direta",
        parse_mode=ParseMode.HTML,
    )

async def cmd_buscar(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await _topic_allowed(u): return
    if u.effective_user: await _run_search(_db_upsert_user, u.effective_user)
    if not _dm_ok(u):
        await u.message.reply_text("❌ Buscas desativadas."); return
    q = " ".join(ctx.args).strip() if ctx.args else ""
    if not q:
        await u.message.reply_text(
            "ℹ️  Use: <code>/buscar nome</code>", parse_mode=ParseMode.HTML)
        return
    await _do_search(u.message, q, _thread_id(u))

# ══════════════════════════════════════════════════════════════
#  /settopic — Configura o tópico do grupo
# ══════════════════════════════════════════════════════════════
async def cmd_settopic(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Use dentro de um tópico do grupo.
    O bot passará a responder APENAS nesse tópico.
    """
    if not u.message or not u.effective_chat or not u.effective_user:
        return

    chat = u.effective_chat
    user = u.effective_user
    msg  = u.message

    # Só funciona em grupos
    if chat.type not in ("group", "supergroup"):
        await msg.reply_text(
            "❌ Este comando só funciona em <b>grupos com tópicos</b> habilitados.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Verifica se é admin ou owner do bot
    is_admin = False
    try:
        member    = await chat.get_member(user.id)
        is_admin  = member.status in ("administrator", "creator")
    except Exception: pass

    if not is_admin and not _is_owner(u):
        await msg.reply_text("❌ Apenas <b>administradores</b> podem configurar o tópico.",
                             parse_mode=ParseMode.HTML)
        return

    # Pega o thread_id da mensagem atual
    thread_id = msg.message_thread_id

    # Subcomando: /settopic clear → remove configuração
    args = ctx.args or []
    if args and args[0].lower() == "clear":
        await _run_search(_db_del_topic, chat.id)
        await msg.reply_text(
            "🗑  Configuração de tópico <b>removida</b>.\n"
            "O bot voltará a responder em qualquer lugar do grupo.",
            parse_mode=ParseMode.HTML,
        )
        return

    if not thread_id:
        await msg.reply_text(
            "❌ Você precisa usar este comando <b>dentro de um tópico</b>.\n\n"
            "Como fazer:\n"
            "1. Abra o tópico desejado\n"
            "2. Digite <code>/settopic</code> nesse tópico\n\n"
            "Para remover: <code>/settopic clear</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    # Salva no DB
    await _run_search(_db_set_topic, chat.id, thread_id, user.id)

    await msg.reply_text(
        f"✅  <b>Tópico configurado com sucesso!</b>\n\n"
        f"🔒  O bot responderá <b>apenas neste tópico</b>.\n"
        f"📌  ID do tópico: <code>{thread_id}</code>\n"
        f"🗓  Configurado por: {user.full_name}\n\n"
        f"Para alterar: use <code>/settopic</code> em outro tópico.\n"
        f"Para remover: <code>/settopic clear</code>",
        parse_mode=ParseMode.HTML,
    )
    log.info(f"📌 Tópico configurado: chat={chat.id} topic={thread_id} by={user.id}")

async def cmd_settings(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(u):
        await u.message.reply_text("❌ Sem permissão."); return
    stats = await _run_search(_db_stats)
    dm    = "🟢 Ativo" if _CFG["dm_on"] else "🔴 Inativo"
    await u.message.reply_text(
        "⚙️  <b>Configurações  —  MusicBot Pro v5</b>\n"
        f"{'─'*30}\n"
        f"💬  Buscas        :  {dm}\n"
        f"{'─'*30}\n"
        f"👥  Usuários      :  {stats['users']}\n"
        f"⬇️  Downloads     :  {stats['downloads']}  (✅ {stats['sent']} enviados)\n"
        f"🎤  Artistas DB   :  {stats['artists']}\n"
        f"💿  Álbuns DB     :  {stats['albums']}\n"
        f"📌  Tópicos conf. :  {stats['topics']}\n"
        f"{'─'*30}\n"
        f"🎚  Qualidade     :  {AUDIO_QUALITY} kbps\n"
        f"🔧  ffmpeg        :  {'✅' if HAS_FFMPEG else '❌'}\n"
        f"⚡  aria2c        :  {'✅ 16x' if HAS_ARIA2C else '❌'}\n"
        f"🔄  Estratégias   :  {len(_STRATEGIES)}\n"
        f"⚡  DL Slots      :  {MAX_DL_SLOTS}\n"
        f"🧵  Search Pool   :  {_SEARCH_POOL._max_workers}w\n"
        f"🧵  DL Pool       :  {_DL_POOL._max_workers}w\n"
        f"📂  DB            :  <code>{DB_PATH}</code>\n"
        f"📁  Cache         :  <code>{CACHE_DIR}</code>",
        reply_markup=_kb_settings(),
        parse_mode=ParseMode.HTML,
    )

# ══════════════════════════════════════════════════════════════
#  HANDLER DE MENSAGENS
# ══════════════════════════════════════════════════════════════
async def handle_msg(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not u.effective_chat or not u.message: return
    if not await _topic_allowed(u): return        # ← filtro de tópico
    if u.effective_user: await _run_search(_db_upsert_user, u.effective_user)
    if not _dm_ok(u):
        await u.message.reply_text("❌ Buscas desativadas."); return

    query = (u.message.text or "").strip()
    if not query: return

    tid     = _thread_id(u)
    user_id = u.effective_user.id if u.effective_user else 0

    # Múltiplas músicas separadas por vírgula
    parts = [p.strip() for p in query.split(",") if p.strip()]
    if len(parts) > 1:
        parts  = parts[:MAX_BATCH]
        status = await u.message.reply_text(
            f"⚡  <b>{len(parts)}</b> músicas  —  baixando em paralelo…",
            parse_mode=ParseMode.HTML)
        items  = [(f"ytsearch1:{p}", p, None) for p in parts]
        await _batch(ctx.bot, u.effective_chat.id, status, items,
                     f"{len(parts)} músicas", user_id, tid)
        return

    await _do_search(u.message, query, tid)

# ══════════════════════════════════════════════════════════════
#  CALLBACK HANDLER
# ══════════════════════════════════════════════════════════════
async def handle_cb(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = u.callback_query
    data = (q.data or "").strip()
    await q.answer()

    chat_id   = q.message.chat.id if q.message else q.from_user.id
    msg       = q.message
    user_id   = q.from_user.id if q.from_user else 0
    tid       = msg.message_thread_id if msg else None

    async def _new(text: str):
        return await ctx.bot.send_message(
            chat_id, text, parse_mode=ParseMode.HTML,
            **( {"message_thread_id": tid} if tid else {} ),
        )

    if data == "NOP": return
    if data == "DEL":
        try: await msg.delete()
        except Exception: pass
        return

    if data.startswith("CFG:"):
        if q.from_user.id != OWNER_ID:
            await q.answer("❌ Sem permissão.", show_alert=True); return
        k = data.split(":")[1]
        _CFG[f"{k}_on"] = not _CFG.get(f"{k}_on", True)
        st = "🟢 ativadas" if _CFG[f"{k}_on"] else "🔴 desativadas"
        await q.answer(f"Buscas {st}")
        try: await msg.edit_reply_markup(_kb_settings())
        except Exception: pass
        return

    if data.startswith("AK:"):
        await _show_artist_card(msg, data[3:], edit=True)
        return

    if data.startswith("AL:"):
        _, key, pg = data.split(":", 2)
        await _show_albums(msg, key, int(pg), edit=True)
        return

    if data.startswith("SG:"):
        _, key, pg = data.split(":", 2)
        await _show_songs(msg, key, int(pg), edit=True)
        return

    if data.startswith("SA:"):
        d = cg(data[3:])
        if not d: await q.answer("Sessão expirada.", show_alert=True); return
        await _show_tracks(msg, d["bid"], d.get("cover",""), "", 0, edit=True)
        return

    if data.startswith("TR:"):
        d = cg(data[3:])
        if not d: await q.answer("Sessão expirada.", show_alert=True); return
        await _show_tracks(msg, d["album_id"], d.get("cover",""),
                           d.get("artist_key",""), d.get("page",0), edit=True)
        return

    if data.startswith("DL:") or data.startswith("SD:"):
        meta  = cg(data[3:])
        if not meta: await q.answer("Sessão expirada.", show_alert=True); return
        title  = meta.get("title",""); artist = meta.get("artist","")
        smsg   = await _new(
            f"⏬  <b>{_cut(title)}</b>"
            + (f"  —  {artist}" if artist else "") + "\n<i>Baixando…</i>"
        )
        # Registra no DB
        log_id = await _run_search(_db_log_download, user_id, meta)
        vid    = meta.get("video_id","")
        target = (f"https://music.youtube.com/watch?v={vid}"
                  if vid else f"ytsearch1:{title} {artist}")
        res = await _dl_task(target, meta)
        if res["ok"]:
            sent = await _send_audio(ctx.bot, chat_id, res, tid, log_id)
            try: await smsg.delete()
            except Exception: pass
            if not sent:
                await _new("⚠️  Arquivo muito grande (> 50 MB).")
        else:
            try:
                await smsg.edit_text(
                    f"❌  <b>{_cut(title)}</b>\n"
                    f"<i>{_cut(res['error'], 200)}</i>",
                    parse_mode=ParseMode.HTML)
            except Exception: pass
        return

    if data.startswith("DAL:"):
        d = cg(data[4:])
        if not d: await q.answer("Sessão expirada.", show_alert=True); return
        tracks = d.get("tracks",[]); label = d.get("label","Álbum")
        smsg   = await _new(
            f"⚡  <b>{_cut(label, 40)}</b>\n"
            f"<i>Iniciando {len(tracks)} downloads em paralelo…</i>"
        )
        items = []
        for t in tracks:
            vid = t.get("video_id","")
            url = (f"https://music.youtube.com/watch?v={vid}" if vid
                   else f"ytsearch1:{t.get('title','')} {t.get('artist','')}")
            items.append((url, t.get("title",""), t))
        await _batch(ctx.bot, chat_id, smsg, items, label, user_id, tid)
        return

# ══════════════════════════════════════════════════════════════
#  ERROR HANDLER
# ══════════════════════════════════════════════════════════════
async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    log.error("Erro não tratado:", exc_info=ctx.error)

# ══════════════════════════════════════════════════════════════
#  INICIALIZAÇÃO
# ══════════════════════════════════════════════════════════════
async def _on_init(app) -> None:
    global _DL_SEM
    _DL_SEM = asyncio.Semaphore(MAX_DL_SLOTS)
    await _run_search(_db_init)
    await app.bot.set_my_commands([
        BotCommand("start",    "🎵  Início"),
        BotCommand("buscar",   "🔍  Buscar artista, álbum ou música"),
        BotCommand("settopic", "📌  Configurar tópico do grupo"),
        BotCommand("settings", "⚙️  Configurações (owner)"),
    ])
    log.info(
        f"\n✅  MusicBot Pro v5\n"
        f"   DB          : {DB_PATH}\n"
        f"   cache       : {CACHE_DIR}\n"
        f"   ffmpeg      : {HAS_FFMPEG}\n"
        f"   aria2c      : {HAS_ARIA2C}\n"
        f"   qualidade   : {AUDIO_QUALITY} kbps\n"
        f"   estratégias : {len(_STRATEGIES)}\n"
        f"   DL slots    : {MAX_DL_SLOTS}\n"
        f"   search pool : {_SEARCH_POOL._max_workers}w\n"
        f"   dl pool     : {_DL_POOL._max_workers}w"
    )

def main():
    if BOT_TOKEN == "SEU_TOKEN_AQUI":
        sys.exit("❌ Configure BOT_TOKEN no topo do arquivo.")

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .read_timeout(30)
        .write_timeout(180)
        .connect_timeout(30)
        .concurrent_updates(True)
        .post_init(_on_init)
        .build()
    )
    app.add_handler(CommandHandler("start",    cmd_start))
    app.add_handler(CommandHandler("buscar",   cmd_buscar))
    app.add_handler(CommandHandler("settopic", cmd_settopic))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CallbackQueryHandler(handle_cb))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, handle_msg))
    app.add_error_handler(error_handler)
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()


#cp ~/storage/downloads/ToolsBot4.py ~/
#ls -la ~/ToolsBot4.py
#python ToolsBot4.py


# instalar ffmpeg (para conversão .mp3)
# pkg install ffmpeg        # Termux
# sudo apt install ffmpeg   # Ubuntu


#┌─────────────────────────────────────────────────────┐
#│  1. Abra o tópico desejado no grupo                 │
#│  2. Digite /settopic (sendo admin)                  │
#│  3. Bot confirma com o ID do tópico                 │
#│  4. Bot ignora QUALQUER mensagem fora do tópico     │
#│                                                     │
#│  Para alterar → /settopic em outro tópico           │
#│  Para remover → /settopic clear                     │
#└─────────────────────────────────────────────────────┘
