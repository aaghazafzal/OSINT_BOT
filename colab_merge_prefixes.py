### ⚡ OSINT Bot - Prefix Merger Script
### Google Colab me chalao - ek baar hi karna hai
### Ye sabhi 87 chunks ko prefix-wise merge karega → 1 file per prefix

# ============================================================
# STEP 1: Drive Mount + Setup
# ============================================================
from google.colab import drive
drive.mount('/content/drive')

import duckdb
import os
import time
from pathlib import Path

SOURCE_DIR = "/content/drive/MyDrive/database/Final_Partitioned_DB"
OUTPUT_DIR = "/content/drive/MyDrive/database/Merged_Prefix_DB"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# STEP 2: Sabhi Unique Prefixes Find Karo
# ============================================================
print("🔍 Finding all prefixes...")
all_prefixes = set()

for chunk_dir in sorted(Path(SOURCE_DIR).glob("chunk_*")):
    for prefix_dir in chunk_dir.glob("prefix=*"):
        prefix = prefix_dir.name.split("=")[1]
        all_prefixes.add(prefix)

all_prefixes = sorted(all_prefixes)
print(f"✅ Found {len(all_prefixes)} unique prefixes")

# ============================================================
# STEP 3: Har Prefix Ko Merge Karo
# ============================================================
con = duckdb.connect()
con.execute("PRAGMA threads=4")
con.execute("PRAGMA memory_limit='8GB'")

done = 0
failed = 0
start_total = time.time()

for prefix in all_prefixes:
    output_file = f"{OUTPUT_DIR}/prefix_{prefix}.parquet"

    # Already merged? Skip
    if os.path.exists(output_file):
        done += 1
        if done % 50 == 0:
            print(f"⏭ Skipped (already done): {done}/{len(all_prefixes)}")
        continue

    # Sabhi chunk files dhundo
    pattern_files = []
    for chunk_dir in sorted(Path(SOURCE_DIR).glob("chunk_*")):
        prefix_dir = chunk_dir / f"prefix={prefix}"
        if prefix_dir.exists():
            files = list(prefix_dir.glob("*.parquet"))
            pattern_files.extend([str(f) for f in files])

    if not pattern_files:
        continue

    try:
        file_list = str(pattern_files)
        con.execute(f"""
            COPY (
                SELECT DISTINCT *
                FROM read_parquet({file_list}, union_by_name=true)
            )
            TO '{output_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)
        """)
        done += 1

        if done % 50 == 0:
            elapsed = time.time() - start_total
            rate = done / elapsed * 60
            remaining = (len(all_prefixes) - done) / rate if rate > 0 else 0
            print(f"✅ {done}/{len(all_prefixes)} | {rate:.0f}/min | ETA: {remaining:.0f} min")

    except Exception as e:
        print(f"❌ Prefix {prefix}: {e}")
        failed += 1

elapsed = time.time() - start_total
print(f"\n🎉 DONE! {done} merged, {failed} failed in {elapsed/60:.1f} min")

# ============================================================
# STEP 4: Verify + Get Folder ID
# ============================================================
merged_files = list(Path(OUTPUT_DIR).glob("*.parquet"))
total_size = sum(f.stat().st_size for f in merged_files) / (1024**3)
print(f"📊 Total files: {len(merged_files)}")
print(f"💿 Total size: {total_size:.2f} GB")

# Folder ID nikalo (DRIVE_FOLDER_ID ke liye)
from google.colab import drive
import subprocess
result = subprocess.run(['find', '/content/drive/MyDrive/database/Merged_Prefix_DB',
                        '-maxdepth', '0', '-printf', '%f\n'],
                       capture_output=True, text=True)
print(f"\n📁 Ab Google Drive me Merged_Prefix_DB folder open karo")
print(f"📋 URL se folder ID copy karo aur bot ke DRIVE_FOLDER_ID me daalo")
