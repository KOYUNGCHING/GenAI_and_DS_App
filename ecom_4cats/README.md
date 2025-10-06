# 電商平台四大品類銷售分析（Shopee × PChome）

本專案以 **Shopee** 與 **PChome** 的公開搜尋 JSON 介面為資料來源，蒐集四大品類（家用電器、食品、服飾、3C 產品）的商品資訊，
比較平台內部的**銷售指標**（Shopee: `historical_sold`；PChome: `review_count` 作為 proxy），並產出統計圖表。

> 僅存取公開搜尋結果，低頻請求、不登入、不繞反爬。請遵守各站條款。

## 快速開始
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Shopee
python -m src.collectors.shopee_search --category 家用電器 --query "掃地機器人 電鍋 吸塵器" --pages 2
python -m src.collectors.shopee_search --category 食品     --query "餅乾 零食 咖啡" --pages 2
python -m src.collectors.shopee_search --category 服飾     --query "T恤 外套 襯衫" --pages 2
python -m src.collectors.shopee_search --category 3C產品   --query "手機 筆電 耳機" --pages 2

# PChome
python -m src.collectors.pchome_search --category 家用電器 --query "掃地機器人 電鍋 吸塵器" --pages 2
python -m src.collectors.pchome_search --category 食品     --query "餅乾 零食 咖啡" --pages 2
python -m src.collectors.pchome_search --category 服飾     --query "T恤 外套 襯衫" --pages 2
python -m src.collectors.pchome_search --category 3C產品   --query "手機 筆電 耳機" --pages 2

# 合併清理
python -m src.processing.merge_clean --raw data/raw --out data/processed/all_items.parquet

# 分析與圖表
python -m src.analysis.category_sales_summary --in data/processed/all_items.parquet --out data/processed/category_summary.csv --plot
```

輸出：
- `data/processed/category_summary.csv`
- `reports/category_sales_bar_shopee.png`
- `reports/category_sales_bar_pchome.png`

## 欄位
- `platform`: shopee/pchome
- `category`: 家用電器/食品/服飾/3C產品
- `title`, `price`, `rating`, `review_count`, `sales`(Shopee)
- `sales_proxy`(PChome = review_count)
- `url`, `query`, `scraped_at`
