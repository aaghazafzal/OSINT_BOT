# 🤖 OSINT Search Bot

Advanced Telegram bot for searching 230GB+ OSINT database stored on Google Drive.

## Files
- `main.py` - Main bot code
- `requirements.txt` - Python dependencies
- `render.yaml` - Render deployment config

## 🚀 Deployment Steps

### Step 1: GitHub par upload karo
1. GitHub.com par naya repository banao (Private!)
2. Teen files upload karo: `main.py`, `requirements.txt`, `render.yaml`
3. **IMPORTANT:** `credentials.json` ko GitHub par KABHI mat daalo! (Environment variable me dalenge)

### Step 2: Drive Folder ID nikalo
1. Google Drive kholo
2. `Final_Partitioned_DB` folder kholo
3. URL me se ID copy karo: `drive.google.com/drive/folders/`**`1ABC...xyz`**
4. Ye ID note kar lo

### Step 3: Service Account JSON ko string me convert karo
Colab me ye run karo:
```python
import json
with open('credentials.json', 'r') as f:
    data = json.load(f)
print(json.dumps(data))  # Ye puri string copy karo
```

### Step 4: Render par deploy karo
1. [render.com](https://render.com) par signup karo
2. **New** → **Web Service** → GitHub repo connect karo
3. Environment Variables add karo:

| Variable | Value |
|---|---|
| `BOT_TOKEN` | Naya Telegram bot token (BotFather se reset karo) |
| `ADMIN_IDS` | Tumhara Telegram user ID ([@userinfobot](https://t.me/userinfobot) se pata karo) |
| `WEBHOOK_URL` | `https://YOUR-APP-NAME.onrender.com` (deploy ke baad milega) |
| `DRIVE_FOLDER_ID` | Step 2 me nikala hua ID |
| `GDRIVE_CREDENTIALS` | Step 3 me nikali hui JSON string |

4. **Create Web Service** click karo
5. Deploy hone ke baad URL copy karo aur `WEBHOOK_URL` variable me daalo
6. **Redeploy** karo

## ⚡ Speed Information
- **First search** (new prefix): 5-15 seconds (Google Drive se download)
- **Repeat search** (same prefix): 0.1-0.5 seconds (local cache)
- Cache Render restart par reset hoti hai

## 🔍 Number Formats Supported
- `9876543210` (plain 10-digit)
- `+919876543210` (with +91)
- `919876543210` (with 91)
- `09876543210` (with leading 0)
- `+91-9876-543210` (with dashes/spaces)
