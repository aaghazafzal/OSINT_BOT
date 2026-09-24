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

MAX_REQ_PER_MIN  = 10
CACHE_DIR        = Path("/tmp/prefix_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
INDEX_CACHE_FILE = Path("/tmp/drive_index.json")

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

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
        lines += [f"📌 *Record {i}/{total}*", "─"*28]
    for col, em in EMOJI.items():
        val = str(row.get(col) or "").strip()
        if val and val.lower() not in ["nan","none","null",""]:
            if col == "address":
                val = re.sub(r"[!]+", ", ", val).strip(", ")
                val = re.sub(r",\s*,", ",", val)
            lines.append(f"{em} *{col.upper()}:* `{val}`")
    return "\n".join(lines)

# ============================================================
# 🤖 HANDLERS
# ============================================================
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    name = update.effective_user.first_name
    status = "✅ Ready" if _index_built else "⏳ Loading..."
    await update.message.reply_text(
        f"👋 *Namaste {name}!*\n\n"
        "🔍 *OSINT Search Bot*\n"
        "230GB+ Indian database!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "📲 *Number bhejo:*\n"
        "`9876543210` | `+919876543210`\n\n"
        f"⚡ *Status:* {status}\n"
        "━━━━━━━━━━━━━━━━━━━━━",
        parse_mode=ParseMode.MARKDOWN
    )

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cached = list(CACHE_DIR.glob("prefix_*.parquet"))
    size_mb = sum(f.stat().st_size for f in cached) / (1024*1024)
    if _index_built:
        msg = (
            f"✅ *Database Ready!*\n\n"
            f"🗂 Total Prefixes: `{len(_prefix_index)}`\n"
            f"💾 Cached: `{len(cached)}` files (`{size_mb:.0f} MB`)\n"
            f"⚡ Cached prefixes: instant search!"
        )
    else:
        msg = "⏳ *Index loading...* Thodi der baad /status check karo."
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Help*\n\n"
        "/start - Bot shuru\n"
        "/status - Database status\n\n"
        "📲 *Number formats:*\n"
        "`9876543210` | `+919876543210`\n"
        "`919876543210` | `09876543210`\n\n"
        "⚡ *Speed:*\n"
        "• First search: 5-15s (download)\n"
        "• Repeat search: <0.5s (cached)",
        parse_mode=ParseMode.MARKDOWN
    )

async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    cached = list(CACHE_DIR.glob("prefix_*.parquet"))
    size_mb = sum(f.stat().st_size for f in cached) / (1024*1024)
    await update.message.reply_text(
        f"📊 *Admin Stats*\n\n"
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
    await update.message.reply_text("✅ Cache cleared!")

async def handle_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text.strip()

    limited, wait = is_limited(uid)
    if limited:
        await update.message.reply_text(
            f"⏳ Ruko! `{wait}s` baad try karo.", parse_mode=ParseMode.MARKDOWN)
        return

    mobile = normalize(text)
    if not mobile:
        if update.message.chat.type == "private":
            await update.message.reply_text(
                "⚠️ *Sahi number format nahi!*\nExample: `9876543210`",
                parse_mode=ParseMode.MARKDOWN)
        return

    if not _index_built:
        await update.message.reply_text(
            "⏳ *Database load ho raha hai...*\nThodi der baad try karo!",
            parse_mode=ParseMode.MARKDOWN)
        return

    status_msg = await update.message.reply_text(
        f"🔍 Searching `{mobile}`...", parse_mode=ParseMode.MARKDOWN)

    loop = asyncio.get_event_loop()
    results, elapsed = await loop.run_in_executor(None, search_mobile, mobile)

    if not results:
        await status_msg.edit_text(
            f"❌ *Not Found*\n\n`{mobile}` database mein nahi hai.\n_({elapsed:.2f}s)_",
            parse_mode=ParseMode.MARKDOWN)
        return

    total = len(results)
    header = f"✅ *{total} Record{'s' if total>1 else ''} Found!* ⚡`{elapsed:.2f}s`\n━━━━━━━━━━━━━━━━━━━━━\n"
    await status_msg.edit_text(
        header + fmt_row(results[0], 1, total), parse_mode=ParseMode.MARKDOWN)

    for i, row in enumerate(results[1:], 2):
        await update.message.reply_text(
            "━━━━━━━━━━━━━━━━━━━━━\n" + fmt_row(row, i, total),
            parse_mode=ParseMode.MARKDOWN)
        await asyncio.sleep(0.3)

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
        app = Application.builder().token(BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(CommandHandler("help", cmd_help))
        app.add_handler(CommandHandler("stats", cmd_stats))
        app.add_handler(CommandHandler("clearcache", cmd_clearcache))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))
        await app.initialize()
        await app.start()
        await app.updater.start_polling(
            drop_pending_updates=True, allowed_updates=["message"])
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
