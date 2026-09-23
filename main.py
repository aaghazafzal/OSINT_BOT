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
from datetime import datetime, timedelta
from collections import defaultdict, OrderedDict

import duckdb
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    filters, ContextTypes, CallbackQueryHandler
)
from telegram.constants import ParseMode
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from flask import Flask, request

# ============================================================
# ⚙️ CONFIGURATION - Render Environment Variables se aayega
# ============================================================
BOT_TOKEN       = os.environ.get("BOT_TOKEN", "")
ADMIN_IDS       = list(map(int, os.environ.get("ADMIN_IDS", "0").split(",")))
WEBHOOK_URL     = os.environ.get("WEBHOOK_URL", "")        # e.g. https://yourapp.onrender.com
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")   # Final_Partitioned_DB folder ID
GDRIVE_CREDS    = os.environ.get("GDRIVE_CREDENTIALS", "") # Service Account JSON (string)

# Rate limiting config
MAX_REQUESTS_PER_MIN = 5
CACHE_DIR = Path("/tmp/prefix_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# 📝 LOGGING
# ============================================================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# ============================================================
# 🔑 GOOGLE DRIVE SERVICE
# ============================================================
_drive_service = None

def get_drive_service():
    global _drive_service
    if _drive_service:
        return _drive_service
    try:
        creds_dict = json.loads(GDRIVE_CREDS)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
        log.info("✅ Google Drive service connected")
        return _drive_service
    except Exception as e:
        log.error(f"❌ Drive service error: {e}")
        return None

# ============================================================
# 📂 DRIVE FILE INDEX (Startup par build hoga)
# ============================================================
# prefix -> list of (chunk_num, file_id)
_prefix_index: dict[str, list[tuple[int, str]]] = defaultdict(list)
_index_built = False
_index_lock = threading.Lock()

def build_drive_index():
    """Google Drive se saare parquet files ka index banata hai"""
    global _prefix_index, _index_built
    service = get_drive_service()
    if not service:
        log.error("Drive service unavailable, cannot build index")
        return

    log.info("🔍 Building Drive file index...")
    start = time.time()

    try:
        # Final_Partitioned_DB ke andar ke saare chunk folders dhundho
        page_token = None
        chunk_folders = {}  # folder_id -> chunk_num

        while True:
            resp = service.files().list(
                q=f"'{DRIVE_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="nextPageToken, files(id, name)",
                pageSize=100,
                pageToken=page_token
            ).execute()

            for f in resp.get("files", []):
                name = f["name"]  # e.g. "chunk_1"
                if name.startswith("chunk_"):
                    try:
                        num = int(name.split("_")[1])
                        chunk_folders[f["id"]] = num
                    except:
                        pass

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        log.info(f"Found {len(chunk_folders)} chunk folders")

        # Har chunk ke andar prefix folders dhundho
        for chunk_folder_id, chunk_num in chunk_folders.items():
            page_token = None
            while True:
                resp = service.files().list(
                    q=f"'{chunk_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                    fields="nextPageToken, files(id, name)",
                    pageSize=200,
                    pageToken=page_token
                ).execute()

                for pf in resp.get("files", []):
                    pname = pf["name"]  # e.g. "prefix=943"
                    if pname.startswith("prefix="):
                        prefix_val = pname.split("=")[1]
                        prefix_folder_id = pf["id"]

                        # Is prefix folder ke andar .parquet files dhundho
                        presp = service.files().list(
                            q=f"'{prefix_folder_id}' in parents and name contains '.parquet' and trashed=false",
                            fields="files(id, name)",
                            pageSize=50
                        ).execute()

                        for pf2 in presp.get("files", []):
                            _prefix_index[prefix_val].append((chunk_num, pf2["id"]))

                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

        _index_built = True
        elapsed = time.time() - start
        total_files = sum(len(v) for v in _prefix_index.values())
        log.info(f"✅ Index built: {len(_prefix_index)} prefixes, {total_files} parquet files ({elapsed:.1f}s)")

    except Exception as e:
        log.error(f"❌ Index build failed: {e}")


# ============================================================
# ⬇️ SMART PREFIX DOWNLOADER WITH CACHE
# ============================================================
_download_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

async def get_prefix_files(prefix: str) -> list[Path]:
    """
    Prefix ke liye local parquet files return karta hai.
    Agar cache me nahi hain toh Drive se download karta hai.
    """
    cache_path = CACHE_DIR / f"prefix_{prefix}"

    # Cache hit - already downloaded files
    if cache_path.exists():
        files = list(cache_path.glob("*.parquet"))
        if files:
            log.info(f"⚡ Cache hit for prefix={prefix} ({len(files)} files)")
            return files

    # Download lock - ek hi baar download hoga concurrent requests me bhi
    async with _download_locks[prefix]:
        # Double-check after lock
        if cache_path.exists():
            files = list(cache_path.glob("*.parquet"))
            if files:
                return files

        cache_path.mkdir(parents=True, exist_ok=True)
        service = get_drive_service()
        if not service:
            return []

        file_entries = _prefix_index.get(prefix, [])
        if not file_entries:
            return []

        log.info(f"⬇️ Downloading {len(file_entries)} files for prefix={prefix}")
        downloaded = []

        for idx, (chunk_num, file_id) in enumerate(file_entries):
            local_path = cache_path / f"chunk{chunk_num}_{idx}.parquet"
            try:
                req = service.files().get_media(fileId=file_id)
                buf = io.BytesIO()
                downloader = MediaIoBaseDownload(buf, req, chunksize=8*1024*1024)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                local_path.write_bytes(buf.getvalue())
                downloaded.append(local_path)
            except Exception as e:
                log.warning(f"Download failed for {file_id}: {e}")

        log.info(f"✅ Downloaded {len(downloaded)} files for prefix={prefix}")
        return downloaded


# ============================================================
# 🔢 NUMBER NORMALIZER
# ============================================================
def normalize_number(raw: str) -> str | None:
    """
    Kisi bhi format me number dalo, clean 10-digit return karega.
    +919431655276 → 9431655276
    91-9431-655276 → 9431655276
    09431655276 → 9431655276
    """
    # Sirf digits rakho
    digits = re.sub(r"[^\d]", "", raw.strip())

    if len(digits) == 10:
        return digits
    elif len(digits) == 11 and digits.startswith("0"):
        return digits[1:]
    elif len(digits) == 12 and digits.startswith("91"):
        return digits[2:]
    elif len(digits) == 13 and digits.startswith("091"):
        return digits[3:]
    elif len(digits) == 10:
        return digits
    return None


# ============================================================
# 🔍 CORE SEARCH ENGINE
# ============================================================
_duckdb_conn = None
_conn_lock = threading.Lock()

def get_db_conn():
    global _duckdb_conn
    with _conn_lock:
        if _duckdb_conn is None:
            _duckdb_conn = duckdb.connect()
            _duckdb_conn.execute("PRAGMA threads=2")
            _duckdb_conn.execute("PRAGMA memory_limit='400MB'")
        return _duckdb_conn


async def search_number(mobile: str) -> tuple[list[dict], float]:
    """Returns (results_list, time_taken_seconds)"""
    prefix = mobile[:3]
    start = time.time()

    parquet_files = await get_prefix_files(prefix)
    if not parquet_files:
        return [], time.time() - start

    file_list_str = str([str(p) for p in parquet_files])

    try:
        con = get_db_conn()
        rows = con.execute(f"""
            SELECT *
            FROM read_parquet({file_list_str}, union_by_name=true)
            WHERE CAST(mobile AS VARCHAR) = '{mobile}'
        """).fetchall()

        if not rows:
            return [], time.time() - start

        # Column names
        col_names = [desc[0] for desc in con.description]
        results = []
        seen = set()

        for row in rows:
            row_dict = dict(zip(col_names, row))
            # Deduplicate
            fingerprint = hashlib.md5(str(row_dict).encode()).hexdigest()
            if fingerprint not in seen:
                seen.add(fingerprint)
                results.append(row_dict)

        return results, time.time() - start

    except Exception as e:
        log.error(f"Search error: {e}")
        return [], time.time() - start


# ============================================================
# 🚦 RATE LIMITER
# ============================================================
_user_requests: dict[int, list[float]] = defaultdict(list)

def is_rate_limited(user_id: int) -> tuple[bool, int]:
    """Returns (is_limited, seconds_to_wait)"""
    now = time.time()
    window = 60  # 1 minute window
    # Purane requests hata do
    _user_requests[user_id] = [t for t in _user_requests[user_id] if now - t < window]

    if len(_user_requests[user_id]) >= MAX_REQUESTS_PER_MIN:
        oldest = _user_requests[user_id][0]
        wait = int(window - (now - oldest)) + 1
        return True, wait

    _user_requests[user_id].append(now)
    return False, 0


# ============================================================
# 📨 MESSAGE FORMATTER
# ============================================================
def format_result(row: dict, idx: int, total: int) -> str:
    """Ek record ko sundar Telegram message me convert karta hai"""
    lines = []

    if total > 1:
        lines.append(f"📌 *Record {idx} of {total}*")
        lines.append("─" * 30)

    field_map = {
        "mobile":  ("📱", "Mobile"),
        "name":    ("👤", "Name"),
        "fname":   ("👨", "Father/Husband"),
        "address": ("🏠", "Address"),
        "alt":     ("📞", "Alt Number"),
        "email":   ("📧", "Email"),
        "circle":  ("🌐", "Operator/Circle"),
        "id":      ("🔑", "ID/Aadhaar"),
    }

    for col, (emoji, label) in field_map.items():
        val = str(row.get(col, "") or "").strip()
        if val and val.lower() not in ["nan", "none", "null", ""]:
            # Address ko clean karo (!! → newline)
            if col == "address":
                val = val.replace("!!", " ").replace("!", ", ").strip(", ")
                val = re.sub(r",\s*,", ",", val)
            lines.append(f"{emoji} *{label}:* `{val}`")

    return "\n".join(lines)


def format_not_found(mobile: str, elapsed: float) -> str:
    return (
        f"❌ *Not Found*\n\n"
        f"Number `{mobile}` hamare database mein nahi hai.\n\n"
        f"_Searched in {elapsed:.2f}s_"
    )


# ============================================================
# 🤖 BOT HANDLERS
# ============================================================
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = (
        f"👋 *Namaste {user.first_name}!*\n\n"
        "🔍 *OSINT Search Bot*\n"
        "Kisi bhi Indian mobile number ki details instantly pao!\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "📲 *Kaise use karein:*\n"
        "Bas number bhejo!\n\n"
        "✅ *Supported Formats:*\n"
        "`9431655276`\n"
        "`+919431655276`\n"
        "`91-9431-655276`\n"
        "`09431655276`\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ Powered by 230GB+ Database"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "📖 *Help & Commands*\n\n"
        "/start - Bot shuru karo\n"
        "/help - Ye menu\n"
        "/stats - Database stats (Admin)\n"
        "/clearcache - Cache saaf karo (Admin)\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🔢 *Number formats supported:*\n"
        "• 10 digit: `9876543210`\n"
        "• With +91: `+919876543210`\n"
        "• With 91: `919876543210`\n"
        "• With 0: `09876543210`\n"
        "• With spaces/dashes: `+91-9876-543210`\n\n"
        "⚡ *Speed:*\n"
        "• First search: 5-15 sec (downloading)\n"
        "• Repeat search: 0.1-0.5 sec (cached)\n\n"
        f"📊 Rate limit: {MAX_REQUESTS_PER_MIN} searches/minute"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only command!")
        return

    cached_prefixes = [d for d in CACHE_DIR.iterdir() if d.is_dir()]
    total_files = sum(len(list(d.glob("*.parquet"))) for d in cached_prefixes)
    total_size = sum(f.stat().st_size for d in cached_prefixes for f in d.glob("*.parquet"))

    text = (
        "📊 *Bot Statistics*\n\n"
        f"🗂 Index Prefixes: `{len(_prefix_index)}`\n"
        f"💾 Cached Prefixes: `{len(cached_prefixes)}`\n"
        f"📁 Cached Files: `{total_files}`\n"
        f"💿 Cache Size: `{total_size / 1024 / 1024:.1f} MB`\n"
        f"🕐 Index Built: `{'Yes' if _index_built else 'No'}`\n"
        f"📡 Drive Connected: `{'Yes' if _drive_service else 'No'}`"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)


async def cmd_clearcache(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only command!")
        return

    import shutil
    shutil.rmtree(CACHE_DIR, ignore_errors=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    global _duckdb_conn
    _duckdb_conn = None

    await update.message.reply_text("✅ Cache cleared successfully!")


async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Main message handler - number search"""
    user_id = update.effective_user.id
    raw_text = update.message.text.strip()

    # Rate limit check
    limited, wait_sec = is_rate_limited(user_id)
    if limited:
        await update.message.reply_text(
            f"⏳ *Thoda ruko!*\n`{wait_sec}` seconds baad try karo.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Number normalize karo
    mobile = normalize_number(raw_text)
    if not mobile:
        # Number nahi hai toh ignore (groups me bhi sahi rahega)
        if update.message.chat.type == "private":
            await update.message.reply_text(
                "⚠️ *Sahi number format nahi hai!*\n\nExample: `9876543210`",
                parse_mode=ParseMode.MARKDOWN
            )
        return

    # Index ready hai?
    if not _index_built:
        msg = await update.message.reply_text(
            "⏳ *Database index bana raha hun...*\n_Pehli baar 30-60 sec lag sakte hain._",
            parse_mode=ParseMode.MARKDOWN
        )
        # Wait for index
        timeout = 120
        waited = 0
        while not _index_built and waited < timeout:
            await asyncio.sleep(2)
            waited += 2

        if not _index_built:
            await msg.edit_text("❌ Database load nahi hua. Thodi der baad try karo.")
            return
        await msg.delete()

    # Search status message
    status_msg = await update.message.reply_text(
        f"🔍 *Searching...*\n`{mobile}` dhoondh raha hun...",
        parse_mode=ParseMode.MARKDOWN
    )

    try:
        results, elapsed = await search_number(mobile)

        if not results:
            await status_msg.edit_text(
                format_not_found(mobile, elapsed),
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # Results dikhao
        total = len(results)
        header = (
            f"✅ *Found {total} Record{'s' if total > 1 else ''}!*\n"
            f"⚡ `{elapsed:.2f}s`\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
        )

        # Pehla result status message me edit karo
        first_result_text = header + format_result(results[0], 1, total)
        await status_msg.edit_text(first_result_text, parse_mode=ParseMode.MARKDOWN)

        # Baki results alag messages me bhejo
        for i, row in enumerate(results[1:], start=2):
            text = "━━━━━━━━━━━━━━━━━━━━\n" + format_result(row, i, total)
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
            await asyncio.sleep(0.3)  # Telegram flood control

    except Exception as e:
        log.error(f"Search handler error: {e}")
        await status_msg.edit_text(
            "⚠️ *Kuch gadbad ho gayi!* Dobara try karo.",
            parse_mode=ParseMode.MARKDOWN
        )


# ============================================================
# 🚀 MAIN - BOT + WEBHOOK SETUP
# ============================================================
flask_app = Flask(__name__)
_bot_app = None


@flask_app.route("/")
def health():
    return {"status": "ok", "index_built": _index_built, "prefixes": len(_prefix_index)}, 200


@flask_app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook():
    """Telegram webhook endpoint"""
    global _bot_app
    if _bot_app is None:
        return "Bot not ready", 503

    data = request.get_json(force=True)
    asyncio.run(_bot_app.process_update(Update.de_json(data, _bot_app.bot)))
    return "OK", 200


def start_index_thread():
    """Background thread me Drive index banao"""
    thread = threading.Thread(target=build_drive_index, daemon=True)
    thread.start()


async def setup_bot():
    global _bot_app
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    # Handlers register karo
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("clearcache", cmd_clearcache))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Webhook set karo
    await app.initialize()
    await app.bot.set_webhook(
        url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
        allowed_updates=["message", "callback_query"]
    )

    _bot_app = app
    log.info(f"✅ Webhook set: {WEBHOOK_URL}/{BOT_TOKEN}")
    return app


def main():
    # Drive index background me banao
    start_index_thread()

    # Bot setup
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(setup_bot())

    # Flask server start karo (Render ke liye)
    port = int(os.environ.get("PORT", 8080))
    log.info(f"🚀 Starting Flask server on port {port}")
    flask_app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    main()
