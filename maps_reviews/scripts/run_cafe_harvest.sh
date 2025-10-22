#!/usr/bin/env bash
set -euo pipefail

# === 參數設定 ===
AREAS_FILE="data/areas/areas_tw.csv"   # 你的行政區檔（建議至少 20–25 行）
LANG="zh-TW"
PAGES=25                               # 每區最多翻頁（Nearby 其實最多 3 頁，但我們放大以保險）
OUTDIR="data/raw/places"

# 也可以把密集區半徑設大一點（在 CSV 裡調整），例如 2000~3000
# radius_m 欄位是以「公尺」計算

# === 封裝：跑一次收集 ===
run_job() {
  local label="$1"      # 顯示用（要寫進檔名）
  local keyword="$2"    # 關鍵字（可空字串）
  local gtype="$3"      # Google Places type（可空字串）

  echo "=============================="
  echo "  Harvest: $label"
  echo "  keyword: $keyword"
  echo "     type: $gtype"
  echo "=============================="

  # 組參數
  args=( -m src.collectors.gmaps_places --areas "$AREAS_FILE" --max_pages "$PAGES" --lang "$LANG" --outdir "$OUTDIR" --also_csv )
  if [[ -n "$keyword" ]]; then
    args+=( --keyword "$keyword" )
  fi
  if [[ -n "$gtype" ]]; then
    args+=( --type "$gtype" )
  fi

  python "${args[@]}"
}

# === 1) 基礎市場面：一般咖啡店 ===
# 用 type=cafe + 關鍵字（中/英）一起抓，涵蓋率高很多
run_job "all_cafes" "咖啡 OR cafe OR coffee" "cafe"

# === 2) 連鎖補抓：品牌關鍵字 ===
# 星巴克
run_job "starbucks" "星巴克 OR Starbucks" ""
# 路易莎
run_job "louisa" "路易莎 OR Louisa" ""
# 85度C
run_job "85c" "85度C OR 85°C OR 85C" ""

# (選用) 你也可自行加碼其他品牌：
# run_job "cama" "cama OR Cama OR 咖碼" ""
# run_job "mrbrown" "伯朗咖啡 OR Mr. Brown" ""

# === 3) 合併＆前處理（重複去除 / 欄位清洗 / 打標 is_chain）===
python -m src.processing.prepare_analysis \
  --places data/processed/places_all.parquet \
  --out data/processed/analysis_ready.parquet

echo "All done ✅"