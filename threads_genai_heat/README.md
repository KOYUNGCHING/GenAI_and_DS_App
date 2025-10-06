# Threads 上的生成式 AI 討論熱度分析（Threads-only）

本專案專注於 **Threads** 平台，追蹤與「生成式人工智慧」（Generative AI）相關的主題／hashtag（如：`#ChatGPT`、`#AIart`、`#GenerativeAI`、`#Midjourney`、`#Sora`），
並產出：每日貼文數、互動加權情感、Hashtag 共現網路、使用者集中度、與高峰摘要（不依賴外部新聞）。

> 合規提醒：本專案預設使用 **Threads 官方 API（Keyword Search）**。請在 `.env` 中設定 `THREADS_ACCESS_TOKEN`（開發者測試或正式核准之 token）。

---

## 快速開始

### 1) 建立虛擬環境並安裝套件

```bash
python3 -m venv .venv
source .venv/bin/activate     # Windows 用 .venv\Scripts\activate
pip install -r requirements.txt
```

### 2) 設定環境變數
```bash
cp .env.example .env
# 編輯 .env，填入：THREADS_ACCESS_TOKEN=...
```

### 3) 收集資料（範例：2025-10-06 之前一週）
> 半開區間：要涵蓋 10/06，end 請給 10/07。

```bash
python -m src.collectors.threads_keyword --topics "#ChatGPT,#AIart,#GenerativeAI,#Midjourney,#Sora"   --start 2025-10-01 --end 2025-10-07 --out data/raw/threads/threads_2025-10-01_2025-10-07.parquet
```

### 4) 正規化 → 情感分析 → 特徵工程 → 時間序列與圖表
```bash
python -m src.utils.normalize data/raw data/bronze
python -m src.analysis.sentiment --in data/bronze/all_posts.parquet --out data/silver/sentiment.parquet
python -m src.features.build_features --in data/silver/sentiment.parquet --out data/silver/with_features.parquet
python -m src.analysis.make_timeseries --in data/silver/with_features.parquet --out data/gold/daily_metrics.parquet --plot
```

### 5) 進階分析
```bash
# Hashtag 共現網路（輸出 .gexf，Gephi 可開）
python -m src.analysis.hashtag_graph --in data/silver/with_features.parquet --out reports/hashtag_graph.gexf

# 使用者集中度（Gini 與 Top1%/Top5% 互動占比）
python -m src.analysis.user_concentration --in data/silver/with_features.parquet --out reports/user_concentration.json

# 高峰摘要（zscore_7d ≥ 2 當尖峰，輸出當日前後代表貼文）
python -m src.analysis.peaks_summarize --metrics data/gold/daily_metrics.parquet --posts data/silver/with_features.parquet --out reports/threads_peaks.csv
```

---

## 目錄結構
```
threads_genai_heat/
  data/
    raw/threads/
    bronze/
    silver/
    gold/
    events/
  reports/
  src/
    collectors/
      threads_keyword.py
    features/
      build_features.py
    analysis/
      sentiment.py
      make_timeseries.py
      hashtag_graph.py
      user_concentration.py
      peaks_summarize.py
    utils/
      io.py
      normalize.py
  .env.example
  requirements.txt
  README.md
```

---

## 欄位 Schema（統一格式）
- post_id, platform, created_at (UTC ISO), author, text, hashtags, url
- score（like 數）, comments（留言數）, lang
- sentiment（pos/neg/neu）, sent_score ∈ [-1,1]
- engagement = like + 2*comment（可在 `build_features.py` 調權重）

---

## 授權與責任
本範例僅供研究教學。請遵守 Threads API 與 Meta 開發者條款。
