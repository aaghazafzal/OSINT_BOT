import os
import re
import io
import json
import time
import logging
import asyncio
import hashlib
import threading
from pathlib import Path
from collections import defaultdict
import concurrent.futures as _cf
import encodings.idna  # Fix: Werkzeug IDNA encoding issue

import duckdb
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes
)
from telegram.constants import ParseMode
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from flask import Flask

# ============================================================
# ⚙️ CONFIG
# ============================================================
BOT_TOKEN        = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS        = list(map(int, filter(None, os.environ.get("ADMIN_IDS", "0").split(","))))
DRIVE_FOLDER_ID  = os.environ.get("DRIVE_FOLDER_ID", "")   # Merged_Prefix_DB folder ID
GDRIVE_CREDS     = os.environ.get("GDRIVE_CREDENTIALS", "")
MINI_APP_URL     = os.environ.get("MINI_APP_URL", "")      # Set when mini app is ready

SEARCH_STICKER   = "CAACAgIAAxkBAAER8OxqtUbJd5JUeIcWIz3w6VOxZSD2BwAC4xAAAqch4EmHTbt5tPu_Xz0E"

MAX_REQ_PER_MIN  = 10
CACHE_DIR        = Path("/tmp/prefix_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
INDEX_CACHE_FILE = Path("/tmp/drive_index.json")

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ============================================================
# 📡 KEYBOARDS (Colored Buttons via PTB 22.8+)
# ============================================================
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import KeyboardButtonStyle

def btn(text: str, *, cd: str = None, url: str = None, web_app_url: str = None, switch_inline: str = None, style=None) -> InlineKeyboardButton:
    """Helper to create InlineKeyboardButton with styles"""
    kwargs = {"text": text}
    if cd is not None: kwargs["callback_data"] = cd
    if url is not None: kwargs["url"] = url
    if web_app_url is not None: kwargs["web_app"] = {"url": web_app_url}
    if switch_inline is not None: kwargs["switch_inline_query_current_chat"] = switch_inline
    if style is not None: kwargs["style"] = style
    return InlineKeyboardButton(**kwargs)

def _build_result_kb(found=True):
    """Colored keyboard - green for found, red for not found"""
    btn_status = btn("✅ Match Found!", cd="cb_noop", style=KeyboardButtonStyle.SUCCESS) if found else btn("❌ No Record", cd="cb_noop", style=KeyboardButtonStyle.DANGER)
    
    keyboard = [
        [
            btn("🔍 Search Another Number", switch_inline="", style=KeyboardButtonStyle.SUCCESS),
            btn_status
        ]
    ]
    if MINI_APP_URL:
        keyboard.append([btn("🎮 Play Mini Game", web_app_url=MINI_APP_URL)])
    else:
        keyboard.append([btn("📊 Database Status", cd="cb_status", style=KeyboardButtonStyle.PRIMARY)])
        
    return InlineKeyboardMarkup(keyboard)

def _build_searching_kb():
    """Keyboard shown while searching"""
    if MINI_APP_URL:
        return InlineKeyboardMarkup([[btn("🎮 Play While Searching...", web_app_url=MINI_APP_URL)]])
    return InlineKeyboardMarkup([[btn("⏳ Searching...", cd="cb_noop", style=KeyboardButtonStyle.PRIMARY)]])


# ============================================================
# 🔑 GOOGLE DRIVE
# ============================================================
def _make_service():
    """Thread-safe: har call pe naya service (google-api-python-client thread issue)"""
    creds_dict = json.loads(GDRIVE_CREDS)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

_shared_service = None
_svc_lock = threading.Lock()

def get_drive_service():
    global _shared_service
    with _svc_lock:
        if _shared_service is None:
            try:
                _shared_service = _make_service()
                log.info("✅ Google Drive connected!")
            except Exception as e:
                log.error(f"❌ Drive error: {e}")
        return _shared_service

# ============================================================
# 📂 SIMPLE INDEX: prefix → file_id  (1 file per prefix!)
# ============================================================
_prefix_index: dict = {}   # { "789": "DRIVE_FILE_ID" }
_index_built = False

def build_drive_index():
    """Merged_Prefix_DB folder me prefix_XXX.parquet files list karo"""
    global _prefix_index, _index_built

    # Cache check
    if INDEX_CACHE_FILE.exists():
        try:
            _prefix_index = json.loads(INDEX_CACHE_FILE.read_text())
            _index_built = True
            log.info(f"⚡ Index from cache: {len(_prefix_index)} prefixes")
            return
        except Exception as e:
            log.warning(f"Cache invalid, rebuilding: {e}")

    log.info("🔍 Building index from Merged_Prefix_DB...")
    start = time.time()
    try:
        service = get_drive_service()
        if not service:
            return

        index = {}
        page_token = None
        while True:
            resp = service.files().list(
                q=f"'{DRIVE_FOLDER_ID}' in parents and name contains 'prefix_' and trashed=false",
                fields="nextPageToken, files(id, name)",
                pageSize=1000,
                pageToken=page_token
            ).execute()

            for f in resp.get("files", []):
                # name = "prefix_789.parquet" → key = "789"
                name = f["name"]
                if name.startswith("prefix_") and name.endswith(".parquet"):
                    prefix = name[7:-8]   # strip "prefix_" and ".parquet"
                    index[prefix] = f["id"]

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        _prefix_index = index
        _index_built = True
        log.info(f"✅ Index built: {len(_prefix_index)} prefixes in {time.time()-start:.1f}s")

        # Save cache
        INDEX_CACHE_FILE.write_text(json.dumps(_prefix_index))
        log.info("💾 Index cached")

    except Exception as e:
        log.error(f"❌ Index build failed: {e}")

# ============================================================
# ⬇️ DOWNLOADER  (1 file per prefix — much faster!)
# ============================================================
_dl_locks: dict = {}
_dl_lock_master = threading.Lock()

def _get_dl_lock(prefix):
    with _dl_lock_master:
        if prefix not in _dl_locks:
            _dl_locks[prefix] = threading.Lock()
        return _dl_locks[prefix]

def get_prefix_file(prefix: str):
    """Download 1 merged parquet file for this prefix. Return local path."""
    cache_path = CACHE_DIR / f"prefix_{prefix}.parquet"

    if cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path

    lock = _get_dl_lock(prefix)
    with lock:
        if cache_path.exists() and cache_path.stat().st_size > 0:
            return cache_path

        file_id = _prefix_index.get(prefix)
        if not file_id:
            log.warning(f"prefix={prefix} not in index")
            return None

        log.info(f"⬇️ Downloading prefix_{prefix}.parquet ...")
        start = time.time()
        try:
            svc = _make_service()
            req = svc.files().get_media(fileId=file_id)
            with open(cache_path, 'wb') as f:
                dl = MediaIoBaseDownload(f, req, chunksize=8 * 1024 * 1024)
                done = False
                while not done:
                    _, done = dl.next_chunk()
            size_mb = cache_path.stat().st_size / (1024 * 1024)
            log.info(f"✅ prefix_{prefix}: {size_mb:.1f}MB in {time.time()-start:.1f}s")
            return cache_path
        except Exception as e:
            log.error(f"❌ Download failed prefix={prefix}: {e}")
            if cache_path.exists():
                cache_path.unlink()
            return None

# ============================================================
# 🔢 NUMBER NORMALIZER
# ============================================================
def normalize(raw: str):
    digits = re.sub(r"[^\d]", "", raw.strip())
    if len(digits) == 10: return digits
    if len(digits) == 11 and digits.startswith("0"): return digits[1:]
    if len(digits) == 12 and digits.startswith("91"): return digits[2:]
    if len(digits) == 13 and digits.startswith("091"): return digits[3:]
    return None

# ============================================================
# 🔍 SEARCH
# ============================================================
_db_lock = threading.Lock()
_db_conn = None

def get_conn():
    global _db_conn
    with _db_lock:
        if _db_conn is None:
            _db_conn = duckdb.connect()
            _db_conn.execute("PRAGMA threads=2")
            _db_conn.execute("PRAGMA memory_limit='200MB'")
        return _db_conn

def search_mobile(mobile: str):
    prefix = mobile[:3]
    start = time.time()

    local_file = get_prefix_file(prefix)
    if not local_file:
        return [], time.time() - start

    try:
        con = get_conn()
        rows = con.execute(f"""
            SELECT * FROM read_parquet('{local_file}')
            WHERE CAST(mobile AS VARCHAR) = '{mobile}'
        """).fetchall()
        cols = [d[0] for d in con.description]

        seen, results = set(), []
        for row in rows:
            rd = dict(zip(cols, row))
            fp = hashlib.md5(str(rd).encode()).hexdigest()
            if fp not in seen:
                seen.add(fp)
                results.append(rd)

        return results, time.time() - start
    except Exception as e:
        log.error(f"Search error: {e}")
        return [], time.time() - start

# ============================================================
# 🚦 RATE LIMITER
# ============================================================
_user_reqs = defaultdict(list)

def is_limited(uid: int):
    now = time.time()
    _user_reqs[uid] = [t for t in _user_reqs[uid] if now - t < 60]
    if len(_user_reqs[uid]) >= MAX_REQ_PER_MIN:
        wait = int(60 - (now - _user_reqs[uid][0])) + 1
        return True, wait
    _user_reqs[uid].append(now)
    return False, 0

# ============================================================
# 📨 FORMATTER
# ============================================================
EMOJI = {
    'mobile':'📱','name':'👤','fname':'👨','address':'🏠',
    'alt':'📞','email':'📧','circle':'🌐','id':'🪪'
}

def fmt_row(row: dict, i: int, total: int) -> str:
    lines = []
    if total > 1:
        lines += [f"📌 <b>Record {i}/{total}</b>", "─"*28]
    for col, em in EMOJI.items():
        val = str(row.get(col) or "").strip()
        if val and val.lower() not in ["nan","none","null",""]:
            if col == "address":
                val = re.sub(r"[!]+", ", ", val).strip(", ")
                val = re.sub(r",\s*,", ",", val)
            val = val.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            lines.append(f"{em} <b>{col.upper()}:</b> <code>{val}</code>")
    return "\n".join(lines)

# ============================================================
# 🤖 HANDLERS
# ============================================================
def main_menu_kb():
    """Main menu inline keyboard"""
    return InlineKeyboardMarkup([
        [
            btn("🔍 Search Number", switch_inline="", style=KeyboardButtonStyle.SUCCESS),
            btn("📊 Status", cd="cb_status", style=KeyboardButtonStyle.PRIMARY),
        ],
        [
            btn("ℹ️ Help", cd="cb_help", style=KeyboardButtonStyle.PRIMARY),
            btn("⚡ Speed Info", cd="cb_speed", style=KeyboardButtonStyle.PRIMARY),
        ]
    ])

def not_found_kb():
    """Keyboard shown when number not found"""
    return InlineKeyboardMarkup([
        [
            btn("🔍 Try Another", switch_inline="", style=KeyboardButtonStyle.SUCCESS),
            btn("ℹ️ Help", cd="cb_help", style=KeyboardButtonStyle.PRIMARY),
        ]
    ])

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    name = user.first_name
    name = name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") if name else "User"
    uid = user.id
    
    status = "✅ Ready — Send a number!" if _index_built else "⏳ Database loading, please wait..."
    text = (
        f"👋 <b>Welcome, <a href='tg://user?id={uid}'>{name}</a>!</b>\n\n"
        "🔍 <b><a href='https://t.me/OSINT_UNIVORABOT'>OSINT BOT [UNIVORA]</a></b>\n"
        "A powerful intelligence tool to analyze and verify telecom records instantly.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "📲 <b>Supported formats:</b>\n"
        "<code>9876543210</code>\n"
        "<code>+919876543210</code>\n"
        "<code>919876543210</code>\n\n"
        f"⚡ <b>Status:</b> {status}\n"
        "━━━━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(), disable_web_page_preview=True)

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cached = list(CACHE_DIR.glob("prefix_*.parquet"))
    size_mb = sum(f.stat().st_size for f in cached) / (1024 * 1024)
    if _index_built:
        msg = (
            "✅ <b>Database Status: ONLINE</b>\n\n"
            f"🗂 Total Prefixes Indexed: <code>{len(_prefix_index)}</code>\n"
            f"💾 Cached Prefixes: <code>{len(cached)}</code>\n"
            f"💿 Cache Size: <code>{size_mb:.0f} MB</code>\n\n"
            "🟢 <b>Cached prefixes:</b> Instant search (&lt;0.5s)\n"
            "🟡 <b>New prefix:</b> First download (5-30s), then instant"
        )
    else:
        msg = "🔴 <b>Database Status: Loading...</b>\n\nPlease wait a moment and try again."
    
    kb = InlineKeyboardMarkup([
        [
            btn("🔍 Search Now", switch_inline="", style=KeyboardButtonStyle.SUCCESS),
            btn("🔄 Refresh", cd="cb_status", style=KeyboardButtonStyle.PRIMARY)
        ]
    ])
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=kb)

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [
            btn("🔍 Search Number", switch_inline="", style=KeyboardButtonStyle.SUCCESS),
            btn("📊 Status", cd="cb_status", style=KeyboardButtonStyle.PRIMARY)
        ]
    ])
    msg = (
        "📖 <b>How to Use</b>\n\n"
        "<b>Commands:</b>\n"
        "/start — Welcome screen\n"
        "/status — Database status\n"
        "/help — This guide\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "📲 <b>Send a mobile number:</b>\n"
        "<code>9876543210</code> — 10 digits\n"
        "<code>+919876543210</code> — with +91\n"
        "<code>09876543210</code> — with leading 0\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ <b>Speed:</b>\n"
        "🟢 Cached prefix → <code>&lt;0.5s</code> instant\n"
        "🟡 New prefix → <code>5-30s</code> first time, then instant\n\n"
        "📚 <b>Library:</b> python-telegram-bot v22.8"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=kb)


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    cached = list(CACHE_DIR.glob("prefix_*.parquet"))
    size_mb = sum(f.stat().st_size for f in cached) / (1024 * 1024)
    await update.message.reply_text(
        f"🛠 *Admin Stats*\n\n"
        f"📋 Index: `{len(_prefix_index)}` prefixes\n"
        f"💾 Cached: `{len(cached)}` / `{len(_prefix_index)}`\n"
        f"💿 Cache size: `{size_mb:.1f} MB`\n"
        f"✅ Index ready: `{_index_built}`",
        parse_mode=ParseMode.MARKDOWN
    )

async def cmd_clearcache(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    import shutil
    shutil.rmtree(CACHE_DIR, ignore_errors=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    global _db_conn
    _db_conn = None
    await update.message.reply_text("✅ Cache cleared successfully!")

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle inline button presses"""
    q = update.callback_query
    await q.answer()
    data = q.data

    if data == "cb_noop":
        return

    chat_id = q.message.chat.id
    msg_id = q.message.message_id

    if data == "cb_status":
        cached = list(CACHE_DIR.glob("prefix_*.parquet"))
        size_mb = sum(f.stat().st_size for f in cached) / (1024 * 1024)
        if _index_built:
            msg = (
                "✅ <b>Database Status: ONLINE</b>\n\n"
                f"🗂 Total Prefixes Indexed: <code>{len(_prefix_index)}</code>\n"
                f"💾 Cached Prefixes: <code>{len(cached)}</code>\n"
                f"💿 Cache Size: <code>{size_mb:.0f} MB</code>\n\n"
                "🟢 <b>Cached:</b> Instant  |  🟡 <b>New:</b> 5-30s first time"
            )
        else:
            msg = "🔴 <b>Database Status: Loading...</b>\n\nWait a moment and try again."
        kb = InlineKeyboardMarkup([
            [
                btn("🔍 Search Now", switch_inline="", style=KeyboardButtonStyle.SUCCESS),
                btn("🔄 Refresh", cd="cb_status", style=KeyboardButtonStyle.PRIMARY)
            ]
        ])
        await q.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=kb)

    elif data == "cb_help":
        kb = InlineKeyboardMarkup([
            [
                btn("🔍 Search Number", switch_inline="", style=KeyboardButtonStyle.SUCCESS),
                btn("📊 Status", cd="cb_status", style=KeyboardButtonStyle.PRIMARY)
            ]
        ])
        msg = (
            "📖 <b>How to Use</b>\n\n"
            "Just send any Indian mobile number:\n"
            "<code>9876543210</code> | <code>+919876543210</code>\n\n"
            "🟢 Cached → Instant\n"
            "🟡 New prefix → 5-30s first time\n\n"
            "📚 Library: python-telegram-bot v22.8"
        )
        await q.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=kb)

    elif data == "cb_speed":
        kb = InlineKeyboardMarkup([
            [
                btn("◀️ Back", cd="cb_help", style=KeyboardButtonStyle.PRIMARY)
            ]
        ])
        msg = (
            "⚡ <b>Speed Information</b>\n\n"
            "🟢 <b>Cached prefix:</b> <code>&lt; 0.5 seconds</code>\n"
            "   Numbers you've searched before\n\n"
            "🟡 <b>New prefix (first time):</b> <code>5-30 seconds</code>\n"
            "   Downloads data from Google Drive\n\n"
            "🔄 <b>After restart:</b> Index rebuilds in ~30s\n"
            "   Cached files persist during session"
        )
        await q.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=kb)

async def handle_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid     = update.effective_user.id
    chat_id = update.effective_chat.id
    text    = update.message.text.strip()

    limited, wait = is_limited(uid)
    if limited:
        await update.message.reply_text(
            f"⏳ *Rate limit reached!*\nPlease wait `{wait}s` before searching again.",
            parse_mode=ParseMode.MARKDOWN)
        return

    mobile = normalize(text)
    if not mobile:
        if update.message.chat.type == "private":
            await update.message.reply_text(
                "⚠️ *Invalid Number Format!*\n\n"
                "Please send a valid Indian mobile number:\n"
                "`9876543210`\n`+919876543210`\n`919876543210`",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("ℹ️ Help", callback_data="cb_help")
                ]])
            )
        return

    if not _index_built:
        await update.message.reply_text(
            "🔴 *Database is loading...*\n\n"
            "Please wait a moment and try again.\n"
            "_Usually ready within 1-2 minutes after restart._",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Check Status", callback_data="cb_status")
            ]])
        )
        return

    # ── Step 1: Send sticker (plays while searching) ──
    sticker_msg = await update.message.reply_sticker(SEARCH_STICKER)

    # ── Step 2: Send engaging search message with mini app button ──
    search_text = (
        f"🔍 <b>Searching Database...</b>\n\n"
        f"📱 <b>Number:</b> <code>{mobile}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ Scanning <b>230GB+</b> telecom records\n"
        "🛰 Querying across <b>87 data chunks</b>\n"
        "🔐 Secure encrypted lookup in progress\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>🕐 New numbers take 5-30s to fetch.</i>\n"
        "<i>Previously searched numbers are instant!</i>"
    )
    search_msg = await update.message.reply_text(
        search_text,
        reply_markup=_build_searching_kb(),
        parse_mode=ParseMode.HTML,
        reply_to_message_id=update.message.message_id
    )

    # ── Step 3: Run search in background ──
    loop = asyncio.get_event_loop()
    results, elapsed = await loop.run_in_executor(None, search_mobile, mobile)

    # ── Step 4: Delete sticker ──
    await sticker_msg.delete()

    # ── Step 5: Show result with colored buttons ──
    if not results:
        not_found = (
            f"❌ <b>No Records Found</b>\n\n"
            f"📱 <code>{mobile}</code> is not in our database.\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱ Completed in <code>{elapsed:.2f}s</code>\n"
            "Try a different number or check the format."
        )
        await search_msg.edit_text(not_found, reply_markup=_build_result_kb(found=False), parse_mode=ParseMode.HTML)
        return

    total  = len(results)
    header = (
        f"✅ <b>{total} Record{'s' if total > 1 else ''} Found!</b>\n"
        f"⚡ <code>{elapsed:.2f}s</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
    )
    
    # Cap results to avoid exceeding Telegram's 4096 char limit
    max_res = min(total, 5)
    blocks = []
    for i, row in enumerate(results[:max_res], 1):
        blocks.append(fmt_row(row, i, total))
    
    body = "\n━━━━━━━━━━━━━━━━━━━━━\n".join(blocks)
    if total > 5:
        body += f"\n━━━━━━━━━━━━━━━━━━━━━\n⚠️ <i>Showing 5 of {total} records.</i>"
        
    final_text = header + body

    await search_msg.edit_text(final_text, reply_markup=_build_result_kb(found=True), parse_mode=ParseMode.HTML)



# ============================================================
# 🚀 FLASK + BOT + STARTUP
# ============================================================
flask_app = Flask(__name__)

@flask_app.route("/")
def health():
    cached = len(list(CACHE_DIR.glob("prefix_*.parquet")))
    return {
        "status": "ok",
        "index_built": _index_built,
        "prefixes": len(_prefix_index),
        "cached": cached
    }, 200

@flask_app.route("/ping")
def ping():
    return "pong", 200

def self_ping_loop():
    import urllib.request
    url = os.environ.get("WEBHOOK_URL", "").rstrip("/")
    if not url:
        return
    log.info(f"🏓 Self-ping: {url}/ping every 5 min")
    while True:
        time.sleep(300)
        try:
            urllib.request.urlopen(f"{url}/ping", timeout=10)
            log.info("🏓 Ping OK")
        except Exception as e:
            log.warning(f"🏓 Ping failed: {e}")

def run_bot():
    log.info("🤖 Starting bot...")
    async def _run():
        from telegram.ext import CallbackQueryHandler
        app = Application.builder().token(BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(CommandHandler("help", cmd_help))
        app.add_handler(CommandHandler("stats", cmd_stats))
        app.add_handler(CommandHandler("clearcache", cmd_clearcache))
        app.add_handler(CallbackQueryHandler(handle_callback))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))
        await app.initialize()
        await app.start()
        await app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=["message", "callback_query"]
        )
        log.info("✅ Bot polling started!")
        while True:
            await asyncio.sleep(60)

        log.info("✅ Bot polling started!")
        while True:
            await asyncio.sleep(60)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_run())
    except Exception as e:
        log.error(f"❌ Bot error: {e}")

def startup():
    log.info("🚀 Starting OSINT Bot...")
    threading.Thread(target=build_drive_index, daemon=True, name="DriveIndex").start()
    threading.Thread(target=run_bot, daemon=True, name="TelegramBot").start()
    threading.Thread(target=self_ping_loop, daemon=True, name="SelfPing").start()
    log.info("✅ All threads started!")

startup()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host="0.0.0.0", port=port)
