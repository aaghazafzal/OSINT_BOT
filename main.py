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
FORCE_SUB_CHANNEL = int(os.environ.get("FORCE_SUB_CHANNEL", "-1002657096509"))
FORCE_SUB_URL     = os.environ.get("FORCE_SUB_URL", "https://t.me/UnivoraOfficial")
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

def _build_result_kb(found=True, is_admin=False):
    """Colored keyboard - green for found, red for not found"""
    btn_status = btn("✅ Match Found!", cd="cb_noop", style=KeyboardButtonStyle.SUCCESS) if found else btn("❌ No Record", cd="cb_noop", style=KeyboardButtonStyle.DANGER)
    
    keyboard = [
        [
            btn("🔍 Search Another Number", switch_inline="", style=KeyboardButtonStyle.SUCCESS),
            btn_status
        ]
    ]
    
    if is_admin:
        keyboard.append([btn("📊 Database Status", cd="cb_status", style=KeyboardButtonStyle.PRIMARY)])
        
    return InlineKeyboardMarkup(keyboard)

def _build_searching_kb():
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

async def check_force_sub(uid: int, bot) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=FORCE_SUB_CHANNEL, user_id=uid)
        return member.status in ['member', 'administrator', 'creator', 'restricted']
    except Exception as e:
        if "user not found" in str(e).lower():
            return False
        log.warning(f"Force Sub Error: {e}")
        return False

async def enforce_sub(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    uid = update.effective_user.id
    if uid in ADMIN_IDS:
        return True
    
    is_subbed = await check_force_sub(uid, context.bot)
    if not is_subbed:
        kb = InlineKeyboardMarkup([
            [btn("📢 Join UNIVORA", url=FORCE_SUB_URL, style=KeyboardButtonStyle.PRIMARY)],
            [btn("🔄 Check", cd="cb_check_sub", style=KeyboardButtonStyle.SUCCESS)]
        ])
        text = "⚠️ <b>Access Denied!</b>\n\nYou must join our official channel to use this bot.\nPlease join and click <b>Check</b>."
        if update.callback_query:
            await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        elif update.message:
            await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return False
    return True


ABOUT_TEXT = (
    "🚀 <b><a href='https://t.me/OSINT_UNIVORABOT'>OSINT BOT [UNIVORA]</a></b>\n"
    "An advanced intelligence tool designed for ultra-fast telecom record lookup.\n\n"
    "⚙️ <b>Tech Stack:</b>\n"
    "• <b>Language:</b> <a href='https://www.python.org/'>Python 3</a>\n"
    "• <b>Library:</b> <a href='https://python-telegram-bot.org/'>python-telegram-bot</a>\n"
    "• <b>Engine:</b> <a href='https://duckdb.org/'>DuckDB</a>\n\n"
    "🛡 <b>Powered by <a href='https://univora.website'>Univora Platform</a></b>\n"
    "A next-generation ecosystem for intelligence and automation.\n\n"
    "👨‍💻 <b>Developer:</b> <a href='https://t.me/ROLEX_SIIR'>@ROLEX_SIIR</a>"
)

def about_kb():
    return InlineKeyboardMarkup([
        [
            btn("🌐 Univora Website", url="https://univora.website", style=KeyboardButtonStyle.PRIMARY),
            btn("👨‍💻 Developer", url="https://t.me/ROLEX_SIIR", style=KeyboardButtonStyle.PRIMARY)
        ],
        [
            btn("🔙 Back", cd="cb_start", style=KeyboardButtonStyle.PRIMARY)
        ]
    ])

def main_menu_kb(is_admin=False):
    """Main menu inline keyboard"""
    row1 = [btn("🔍 Search Number", switch_inline="", style=KeyboardButtonStyle.SUCCESS)]
    if is_admin:
        row1.append(btn("📊 Status", cd="cb_status", style=KeyboardButtonStyle.PRIMARY))
    
    row2 = [
        btn("ℹ️ Help", cd="cb_help", style=KeyboardButtonStyle.PRIMARY),
        btn("⚡ Speed Info", cd="cb_speed", style=KeyboardButtonStyle.PRIMARY),
    ]
    row3 = [btn("🚀 About Univora", cd="cb_about", style=KeyboardButtonStyle.PRIMARY)]
    return InlineKeyboardMarkup([row1, row2, row3])

def not_found_kb(is_admin=False):
    """Keyboard shown when number not found"""
    row1 = [btn("🔍 Try Another", switch_inline="", style=KeyboardButtonStyle.SUCCESS)]
    if is_admin:
        row1.append(btn("📊 Status", cd="cb_status", style=KeyboardButtonStyle.PRIMARY))
    
    row2 = [btn("ℹ️ Help", cd="cb_help", style=KeyboardButtonStyle.PRIMARY)]
    return InlineKeyboardMarkup([row1, row2])

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await enforce_sub(update, ctx):
        return
    user = update.effective_user
    name = user.first_name
    name = name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") if name else "User"
    uid = user.id
    is_admin = uid in ADMIN_IDS
    
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
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(is_admin), disable_web_page_preview=True)

async def cmd_about(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await enforce_sub(update, ctx):
        return
    await update.message.reply_text(ABOUT_TEXT, parse_mode=ParseMode.HTML, reply_markup=about_kb(), disable_web_page_preview=True)

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
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
    is_admin = update.effective_user.id in ADMIN_IDS
    row1 = [btn("🔍 Search Number", switch_inline="", style=KeyboardButtonStyle.SUCCESS)]
    if is_admin:
        row1.append(btn("📊 Status", cd="cb_status", style=KeyboardButtonStyle.PRIMARY))
    
    kb = InlineKeyboardMarkup([row1])
    msg = (
        "📖 <b>How to Use</b>\n\n"
        "<b>Commands:</b>\n"
        "/start — Welcome screen\n"
        "/help — This guide\n"
        + ("/status — Database status\n" if is_admin else "") +
        "\n━━━━━━━━━━━━━━━━━━━━━\n"
        "📲 <b>Send a mobile number:</b>\n"
        "<code>9876543210</code> — 10 digits\n"
        "<code>+919876543210</code> — with +91\n"
        "<code>09876543210</code> — with leading 0\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ <b>Speed:</b>\n"
        "🟢 Fast lookup → <code>&lt;0.5s</code>\n"
        "🟡 Deep search → <code>5-30s</code>\n\n"
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
    uid = update.effective_user.id
    is_admin = uid in ADMIN_IDS

    if data == "cb_check_sub":
        if await check_force_sub(uid, ctx.bot):
            await q.answer("✅ Thank you for joining!", show_alert=True)
            await q.message.delete()
            status = "✅ Ready — Send a number!" if _index_built else "⏳ Database loading..."
            text = (
                f"👋 <b>Welcome, <a href='tg://user?id={uid}'>User</a>!</b>\n\n"
                "🔍 <b><a href='https://t.me/OSINT_UNIVORABOT'>OSINT BOT [UNIVORA]</a></b>\n"
                "A powerful intelligence tool to analyze and verify telecom records instantly.\n\n"
                "┏━━━━━━━━━━━━━━━━━━━━\n"
                "📲 <b>Supported formats:</b>\n"
                "<code>9876543210</code>\n"
                "<code>+919876543210</code>\n"
                "<code>919876543210</code>\n\n"
                f"⚡ <b>Status:</b> {status}\n"
                "┗━━━━━━━━━━━━━━━━━━━━"
            )
            await q.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(is_admin), disable_web_page_preview=True)
        else:
            await q.answer("❌ You haven't joined the channel yet!", show_alert=True)
        return

    if not await enforce_sub(update, ctx):
        return

    if data == "cb_status":
        if not is_admin:
            await q.answer("⚠️ This button is restricted to Admins only.", show_alert=True)
            return
            
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
        row1 = [btn("🔍 Search Number", switch_inline="", style=KeyboardButtonStyle.SUCCESS)]
        if is_admin:
            row1.append(btn("📊 Status", cd="cb_status", style=KeyboardButtonStyle.PRIMARY))
            
        kb = InlineKeyboardMarkup([row1])
        msg = (
            "📖 <b>How to Use</b>\n\n"
            "Just send any Indian mobile number:\n"
            "<code>9876543210</code> | <code>+919876543210</code>\n\n"
            "🟢 Fast lookup → Instant\n"
            "🟡 Deep search → 5-30s first time\n\n"
            "📚 Library: python-telegram-bot v22.8"
        )
        await q.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=kb)

    elif data == "cb_about":
        await q.edit_message_text(ABOUT_TEXT, parse_mode=ParseMode.HTML, reply_markup=about_kb(), disable_web_page_preview=True)

    elif data == "cb_start":
        user = update.effective_user
        name = user.first_name
        name = name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") if name else "User"
        status = "✅ Ready — Send a number!" if _index_built else "⏳ Database loading, please wait..."
        text = (
            f"👋 <b>Welcome, <a href='tg://user?id={uid}'>{name}</a>!</b>\n\n"
            "🔍 <b><a href='https://t.me/OSINT_UNIVORABOT'>OSINT BOT [UNIVORA]</a></b>\n"
            "A powerful intelligence tool to analyze and verify telecom records instantly.\n\n"
            "┏━━━━━━━━━━━━━━━━━━━━\n"
            "📲 <b>Supported formats:</b>\n"
            "<code>9876543210</code>\n"
            "<code>+919876543210</code>\n"
            "<code>919876543210</code>\n\n"
            f"⚡ <b>Status:</b> {status}\n"
            "┗━━━━━━━━━━━━━━━━━━━━"
        )
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(is_admin), disable_web_page_preview=True)

    elif data == "cb_speed":
        kb = InlineKeyboardMarkup([
            [
                btn("◀️ Back", cd="cb_help", style=KeyboardButtonStyle.PRIMARY)
            ]
        ])
        msg = (
            "⚡ <b>Speed Information</b>\n\n"
            "🟢 <b>Fast Lookup:</b> <code>&lt; 0.5 seconds</code>\n"
            "   For frequently searched networks.\n\n"
            "🟡 <b>Deep Search:</b> <code>5-30 seconds</code>\n"
            "   For querying fresh records."
        )
        await q.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=kb)

async def handle_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await enforce_sub(update, ctx):
        return
    uid     = update.effective_user.id
    chat_id = update.effective_chat.id
    text    = update.message.text.strip()
    is_admin = uid in ADMIN_IDS

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
                "⚠️ <b>Invalid Number Format!</b>\n\n"
                "Please send a valid Indian mobile number:\n"
                "<code>9876543210</code>\n<code>+919876543210</code>\n<code>919876543210</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=not_found_kb(is_admin)
            )
        return

    if not _index_built:
        await update.message.reply_text(
            "🔴 <b>Database is loading...</b>\n\n"
            "Please wait a moment and try again.\n"
            "<i>Usually ready within 1-2 minutes after restart.</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                btn("🔄 Check Status", cd="cb_status", style=KeyboardButtonStyle.PRIMARY)
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
        "⚡ Initializing secure lookup...\n"
        "🔐 Querying intelligence network...\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>🕐 Please wait while we fetch the records.</i>"
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
        await search_msg.edit_text(not_found, reply_markup=_build_result_kb(found=False, is_admin=is_admin), parse_mode=ParseMode.HTML)
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

    await search_msg.edit_text(final_text, reply_markup=_build_result_kb(found=True, is_admin=is_admin), parse_mode=ParseMode.HTML)



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
        from telegram import BotCommand
        app = Application.builder().token(BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(CommandHandler("help", cmd_help))
        app.add_handler(CommandHandler("about", cmd_about))
        app.add_handler(CommandHandler("stats", cmd_stats))
        app.add_handler(CommandHandler("clearcache", cmd_clearcache))
        app.add_handler(CallbackQueryHandler(handle_callback))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))
        
        await app.initialize()
        await app.start()
        
        # Set the command menu
        try:
            await app.bot.set_my_commands([
                BotCommand("start", "Restart the bot and show main menu"),
                BotCommand("status", "Check database index and cache status"),
                BotCommand("help", "Show guide and formatting rules"),
                BotCommand("about", "About Univora Bot and Developer")
            ])
            log.info("✅ Bot menu commands updated!")
        except Exception as e:
            log.warning(f"⚠️ Failed to set bot commands: {e}")
            
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
