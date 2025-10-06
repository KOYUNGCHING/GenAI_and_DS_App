# 全台咖啡廳評價與連鎖店比較分析

## 專題簡介
本專案使用 **Google Maps API** 蒐集全台各縣市咖啡廳資料，  
比較 **連鎖品牌（如 Starbucks、85°C、路易莎）** 與 **在地咖啡廳** 在評價、價格層級與人氣上的差異。  

分析聚焦於以下幾個問題：
1. 各縣市咖啡廳的平均評價是否存在明顯差異？  
2. 連鎖品牌是否普遍獲得更高評價？  
3. 價格層級與評分之間是否呈現正相關？  
4. 哪些縣市的高價咖啡廳評價最高？
   
## 使用步驟（簡版）
1. `git checkout -b feature/maps-reviews`
2. `cd maps_reviews && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
3. `cp .env.example .env` 並填入 Key
4. 抓咖啡廳資料：`python -m src.collectors.gmaps_places --areas data/areas/areas_tw.csv --max_pages 3 --lang zh-TW`
5. 轉換欄位並清理（價格、品牌、城市等）: `python -m src.processing.prepare_analysis \
  --places data/processed/places_all.parquet \
  --out data/processed/analysis_ready.parquet`

