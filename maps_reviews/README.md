# Google Maps 餐廳評價與地區差異分析（Starter）

## 使用步驟（簡版）
1. `git checkout -b feature/maps-reviews`
2. `cd maps_reviews && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
3. `cp .env.example .env` 並填入 Key
4. 抓餐廳清單：`python -m src.collectors.gmaps_places --areas data/areas/areas_tw.csv --max_pages 2 --lang zh-TW`
5. 抓評論（可選）：`python -m src.collectors.gmaps_details --in data/processed/places_all.parquet --out data/raw/reviews/reviews.parquet --lang zh-TW --max_reviews 5`
6. 前處理：`python -m src.processing.prepare_analysis --places data/processed/places_all.parquet --reviews data/raw/reviews/reviews.parquet --out data/processed/analysis_ready.parquet`
7. 分析：  
   - `python -m src.analysis.anova_area_price --in data/processed/analysis_ready.parquet --metric rating --out reports/anova_rating.csv`  
   - `python -m src.analysis.anova_area_price --in data/processed/analysis_ready.parquet --metric popularity --out reports/anova_popularity.csv`  
   - `python -m src.analysis.area_stats --in data/processed/analysis_ready.parquet --out reports/area_summary.csv --plot`  
   - `python -m src.analysis.sentiment_summary --in data/processed/analysis_ready.parquet --out reports/sentiment_by_area.csv`
