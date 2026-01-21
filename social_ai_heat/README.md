
# 社群平台 AI 討論熱度（Social AI Heat）

這個資料夾提供一個可執行的最小專案骨架，幫你從 Twitter/X、Reddit、知乎 收集以「#ChatGPT / #AIart / AI」為主題的討論，
產生每日量化指標（貼文數、情感正/負比例），並輸出時間序列圖表與高峰偵測結果。

> ⚠️ **法規 / 服務條款提醒**：請謹慎遵守各平台的使用條款與 robots.txt。Twitter/X 與知乎對非官方 API 的請求可能會封鎖或視為違規；
> Reddit 官方 API 需要 OAuth 憑證。此專案預設使用：
> - **Twitter/X**：`snscrape` 做公開關鍵字搜尋（可能隨平台變化而失效）。
> - **Reddit**：`praw`（官方 API）。
> - **知乎**：以 requests 搭配 **自行提供**登入後 cookie 的方式呼叫 zhihu 網頁層 API（風險較高，僅做教學示範）。

---

## 快速開始

1. 建議 Python 3.10+ 。
2. 建立虛擬環境並安裝套件：
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Windows 用 .venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. 複製 `.env.example` 為 `.env`，填入 Reddit 憑證與（可選）知乎 Cookie：
   ```bash
   cp .env.example .env
   # 編輯 .env：
   # REDDIT_CLIENT_ID=...
   # REDDIT_CLIENT_SECRET=...
   # REDDIT_USER_AGENT=yourapp:1.0 (by u/your_reddit_username)
   # ZHIHU_COOKIE=__zse_ck=...; z_c0=...  # 需自行從瀏覽器複製，風險自負
   ```

4. 回填歷史資料（範例：2025-07-01 至今天）：
   ```bash
   python -m src.collectors.x_snscrape --hashtags "#ChatGPT,#AIart" --since 2025-07-01 --until 2025-10-06
   python -m src.collectors.reddit_praw --query "ChatGPT OR AI art OR 生成式AI" --start 2025-07-01 --end 2025-10-06
   python -m src.collectors.zhihu_search --query "ChatGPT 人工智慧 生成式AI" --pages 3
   ```

5. 正規化欄位並合併：
   ```bash
   python -m src.utils.normalize data/raw data/bronze
   ```

6. 執行情感分析與時間序列：
   ```bash
   python -m src.analysis.sentiment --in data/bronze/all_posts.parquet --out data/silver/sentiment.parquet
   python -m src.analysis.make_timeseries --in data/silver/sentiment.parquet --out data/gold/daily_metrics.parquet
   python -m src.analysis.make_timeseries --plot --in data/gold/daily_metrics.parquet --out reports/daily_charts
   ```

7. （選）載入新聞事件清單並對齊高峰：
   ```bash
   python -m src.analysis.events_join --events data/events/events_sample.csv --metrics data/gold/daily_metrics.parquet --out reports/peaks_with_events.csv
   ```

---

## 資料表與欄位（統一 Schema）

- `post_id`：平台原始 ID
- `platform`：`x` / `reddit` / `zhihu`
- `created_at`：UTC ISO 時間
- `author`：作者名稱或 ID（可為空）
- `text`：內文（或標題）
- `hashtags`：以逗號分隔
- `url`：原文連結
- `score`：like / upvote / vote 數（能取到才填）
- `comments`：留言數（能取到才填)
- `lang`：語言（偵測後）

情感輸出（silver）：
- `sentiment`：`pos`/`neg`/`neu`
- `sent_score`：[-1, 1] 範圍之分數（不同模型會近似映射）

時間序列（gold）：每日彙總：
- `date`、`platform`、`hashtag`
- `post_count`、`pos_ratio`、`neg_ratio`
- `zscore_7d`：相對 7 日平均的尖峰指標

---

## 目錄結構

```
social_ai_heat/
  data/
    raw/         # 各資料源原始匯出
    bronze/      # 正規化後合併
    silver/      # 加上情感標註
    gold/        # 指標與時間序列
    events/
      events_sample.csv
  reports/
  src/
    collectors/
      x_snscrape.py
      reddit_praw.py
      zhihu_search.py
    analysis/
      sentiment.py
      make_timeseries.py
      events_join.py
    utils/
      normalize.py
      io.py
  .env.example
  requirements.txt
  README.md
```

---

## 授權與責任
此範例碼僅供研究教學使用。請自行評估法規風險、遵守各平台條款與隱私規範。
