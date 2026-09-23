import os
import re
import io
import json
import time
import glob
import logging
import asyncio
import hashlib
import threading
from pathlib import Path
from collections import defaultdict

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
# ⚙️ CONFIG - Render Environment Variables
# ============================================================
BOT_TOKEN       = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS       = list(map(int, filter(None, os.environ.get("ADMIN_IDS", "0").split(","))))
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")
GDRIVE_CREDS    = os.environ.get("GDRIVE_CREDENTIALS", "")

MAX_REQ_PER_MIN = 5
CACHE_DIR = Path("/tmp/prefix_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# 📝 LOGGING
# ============================================================
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ============================================================
# 🔑 GOOGLE DRIVE
# ============================================================
_drive_service = None

def get_drive_service():
    global _drive_service
    if _drive_service:
        return _drive_service
    try:
        creds_dict = json.loads(GDRIVE_CREDS)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
        log.info("✅ Google Drive connected!")
        return _drive_service
    except Exception as e:
        log.error(f"❌ Drive error: {e}")
        return None

# ============================================================
# 📂 DRIVE FILE INDEX
# ============================================================
_prefix_index: dict = defaultdict(list)
_index_built = False

def build_drive_index():
    global _prefix_index, _index_built
    service = get_drive_service()
    if not service:
        log.error("Drive unavailable, index not built")
        return

    log.info("🔍 Building Drive index...")
    start = time.time()
    try:
        # Get all chunk folders
        chunk_folders = {}
        page_token = None
        while True:
            resp = service.files().list(
                q=f"'{DRIVE_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="nextPageToken, files(id, name)", pageSize=100, pageToken=page_token
            ).execute()
            for f in resp.get("files", []):
                if f["name"].startswith("chunk_"):
                    try:
                        chunk_folders[f["id"]] = int(f["name"].split("_")[1])
                    except: pass
            page_token = resp.get("nextPageToken")
            if not page_token: break

        log.info(f"Found {len(chunk_folders)} chunk folders")

        # Get prefix folders inside each chunk
        for chunk_id, chunk_num in chunk_folders.items():
            page_token = None
            while True:
                resp = service.files().list(
                    q=f"'{chunk_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                    fields="nextPageToken, files(id, name)", pageSize=200, pageToken=page_token
                ).execute()
                for pf in resp.get("files", []):
                    if pf["name"].startswith("prefix="):
                        prefix_val = pf["name"].split("=")[1]
                        # Get parquet files in this prefix folder
                        presp = service.files().list(
                            q=f"'{pf['id']}' in parents and name contains '.parquet' and trashed=false",
                            fields="files(id)", pageSize=50
                        ).execute()
                        for pf2 in presp.get("files", []):
                            _prefix_index[prefix_val].append((chunk_num, pf2["id"]))
                page_token = resp.get("nextPageToken")
                if not page_token: break

        _index_built = True
        total = sum(len(v) for v in _prefix_index.values())
        log.info(f"✅ Index ready: {len(_prefix_index)} prefixes, {total} files ({time.time()-start:.1f}s)")
    except Exception as e:
        log.error(f"❌ Index build failed: {e}")

# ============================================================
# ⬇️ DOWNLOADER
# ============================================================
_dl_locks = {}
_dl_lock_master = threading.Lock()

def get_dl_lock(prefix):
    with _dl_lock_master:
        if prefix not in _dl_locks:
            _dl_locks[prefix] = threading.Lock()
        return _dl_locks[prefix]

def get_prefix_files_sync(prefix: str) -> list:
    cache_path = CACHE_DIR / f"p_{prefix}"
    if cache_path.exists():
        files = list(cache_path.glob("*.parquet"))
        if files:
            return files

    lock = get_dl_lock(prefix)
    with lock:
        if cache_path.exists():
            files = list(cache_path.glob("*.parquet"))
            if files: return files

        cache_path.mkdir(parents=True, exist_ok=True)
        service = get_drive_service()
        if not service: return []

        entries = _prefix_index.get(prefix, [])
        if not entries: return []

        log.info(f"⬇️ Downloading {len(entries)} files for prefix={prefix}")
        downloaded = []
        for idx, (chunk_num, file_id) in enumerate(entries):
            local_path = cache_path / f"c{chunk_num}_{idx}.parquet"
            try:
                req = service.files().get_media(fileId=file_id)
                buf = io.BytesIO()
                dl = MediaIoBaseDownload(buf, req, chunksize=8*1024*1024)
                done = False
                while not done: _, done = dl.next_chunk()
                local_path.write_bytes(buf.getvalue())
                downloaded.append(local_path)
            except Exception as e:
                log.warning(f"Download failed {file_id}: {e}")
        log.info(f"✅ {len(downloaded)} files downloaded for prefix={prefix}")
        return downloaded

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
_db_conn = None
_db_lock = threading.Lock()

def get_conn():
    global _db_conn
    with _db_lock:
        if _db_conn is None:
            _db_conn = duckdb.connect()
            _db_conn.execute("PRAGMA threads=2")
            _db_conn.execute("PRAGMA memory_limit='350MB'")
        return _db_conn

def search_mobile(mobile: str):
    prefix = mobile[:3]
    start = time.time()
    files = get_prefix_files_sync(prefix)
    if not files:
        return [], time.time() - start

    file_list = str([str(p) for p in files])
    try:
        con = get_conn()
        rows = con.execute(f"""
            SELECT * FROM read_parquet({file_list}, union_by_name=true)
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
    await update.message.reply_text(
        f"👋 *Namaste {name}!*\n\n"
        "🔍 *OSINT Search Bot*\n"
        "230GB+ Indian database se instant details!\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "📲 *Bas number bhejo:*\n"
        "`9876543210`\n`+919876543210`\n`91-9876-543210`\n\n"
        f"⚡ *Status:* {'✅ Ready' if _index_built else '⏳ Index loading...'}\n"
        "━━━━━━━━━━━━━━━━━━━━━",
        parse_mode=ParseMode.MARKDOWN
    )

async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    cached = list(CACHE_DIR.iterdir())
    files = sum(len(list(d.glob("*.parquet"))) for d in cached if d.is_dir())
    size = sum(f.stat().st_size for d in cached if d.is_dir() for f in d.glob("*.parquet"))
    await update.message.reply_text(
        f"📊 *Stats*\n\n"
        f"🗂 Indexed Prefixes: `{len(_prefix_index)}`\n"
        f"💾 Cached Prefixes: `{len(cached)}`\n"
        f"📁 Cached Files: `{files}`\n"
        f"💿 Cache Size: `{size/1024/1024:.1f} MB`\n"
        f"✅ Index Ready: `{_index_built}`",
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
        await update.message.reply_text(f"⏳ Ruko! `{wait}s` baad try karo.", parse_mode=ParseMode.MARKDOWN)
        return

    mobile = normalize(text)
    if not mobile:
        if update.message.chat.type == "private":
            await update.message.reply_text("⚠️ *Sahi number format nahi!*\nExample: `9876543210`", parse_mode=ParseMode.MARKDOWN)
        return

    if not _index_built:
        await update.message.reply_text("⏳ *Database load ho raha hai...*\n1-2 minute me ready hoga. Phir try karo!", parse_mode=ParseMode.MARKDOWN)
        return

    status = await update.message.reply_text(f"🔍 Searching `{mobile}`...", parse_mode=ParseMode.MARKDOWN)

    loop = asyncio.get_event_loop()
    results, elapsed = await loop.run_in_executor(None, search_mobile, mobile)

    if not results:
        await status.edit_text(
            f"❌ *Not Found*\n\n`{mobile}` database mein nahi hai.\n_({elapsed:.2f}s)_",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    total = len(results)
    header = f"✅ *{total} Record{'s' if total>1 else ''} Found!* ⚡`{elapsed:.2f}s`\n━━━━━━━━━━━━━━━━━━━━━\n"
    await status.edit_text(header + fmt_row(results[0], 1, total), parse_mode=ParseMode.MARKDOWN)

    for i, row in enumerate(results[1:], 2):
        await update.message.reply_text("━━━━━━━━━━━━━━━━━━━━━\n" + fmt_row(row, i, total), parse_mode=ParseMode.MARKDOWN)
        await asyncio.sleep(0.3)

# ============================================================
# 🚀 STARTUP - Polling in background thread
# ============================================================
flask_app = Flask(__name__)

@flask_app.route("/")
def health():
    return {"status": "ok", "index_built": _index_built, "prefixes": len(_prefix_index)}, 200

def run_bot():
    """Bot polling - background thread me chalta hai (no signal handlers)"""
    log.info("🤖 Starting bot polling...")

    async def _run():
        app = Application.builder().token(BOT_TOKEN).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("stats", cmd_stats))
        app.add_handler(CommandHandler("clearcache", cmd_clearcache))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_msg))

        await app.initialize()
        await app.start()
        # run_polling() ki jagah direct updater use karo (signal handlers nahi lagate)
        await app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=["message"]
        )
        log.info("✅ Bot polling started successfully!")
        # Forever run karo
        while True:
            await asyncio.sleep(60)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_run())
    except Exception as e:
        log.error(f"❌ Bot crashed: {e}")


def startup():
    """Gunicorn import hone par automatically chalta hai"""
    log.info("🚀 Initializing OSINT Bot...")
    # Drive index background thread
    threading.Thread(target=build_drive_index, daemon=True, name="DriveIndex").start()
    # Bot polling background thread
    threading.Thread(target=run_bot, daemon=True, name="TelegramBot").start()
    log.info("✅ All background threads started!")

# Gunicorn jab bhi main.py import kare, startup chalega
startup()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    flask_app.run(host="0.0.0.0", port=port)
