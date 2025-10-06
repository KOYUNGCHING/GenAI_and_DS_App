import requests, urllib.parse, pandas as pd, time, os, random
from tqdm import tqdm
from pathlib import Path
from datetime import datetime
from requests.adapters import HTTPAdapter, Retry

# ==========================================================
# Helper: 建立 Session（有重試機制）
# ==========================================================
def build_session():
    sess = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.8,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=5, pool_maxsize=5)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

SESSION = build_session()

# ==========================================================
# Shopee API 抓取（增加 headers 防 403）
# ==========================================================
UA_POOL = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
]

def fetch_page(keyword, newest):
    url = "https://shopee.tw/api/v4/search/search_items"
    params = {
        "by": "relevancy",
        "keyword": keyword,
        "limit": 60,
        "newest": newest,
        "order": "desc",
        "page_type": "search",
        "scenario": "PAGE_GLOBAL_SEARCH",
        "version": 2,
    }

    ua = random.choice(UA_POOL)
    headers = {
        "User-Agent": ua,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-TW,zh;q=0.9",
        "Referer": f"https://shopee.tw/search?keyword={urllib.parse.quote(keyword)}",
        "Origin": "https://shopee.tw",
        "Connection": "keep-alive",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }

    for attempt in range(3):
        r = SESSION.get(url, params=params, headers=headers, timeout=20)
        if r.status_code in (403, 429):
            wait = (attempt + 1) * (1.5 + random.random())
            print(f"Got {r.status_code}, backing off {wait:.1f}s ...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()


# ==========================================================
# 主程式入口
# ==========================================================
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", required=True, help="分類名稱，如 3C產品 / 食品 / 家用電器")
    parser.add_argument("--query", required=True, help="搜尋關鍵字，多個用空白分開")
    parser.add_argument("--pages", type=int, default=1, help="每個關鍵字要抓幾頁")
    args = parser.parse_args()

    all_rows = []
    for kw in args.query.split():
        print(f"🔍 抓取關鍵字：{kw}")
        for p in tqdm(range(args.pages)):
            newest = p * 60
            js = fetch_page(kw, newest)
            items = js.get("items", [])
            for it in items:
                basic = it.get("item_basic", {})
                row = {
                    "category": args.category,
                    "keyword": kw,
                    "itemid": basic.get("itemid"),
                    "shopid": basic.get("shopid"),
                    "name": basic.get("name"),
                    "price": basic.get("price") / 100000 if basic.get("price") else None,
                    "historical_sold": basic.get("historical_sold"),
                    "liked_count": basic.get("liked_count"),
                    "rating_star": basic.get("item_rating", {}).get("rating_star"),
                    "shop_location": basic.get("shop_location"),
                    "ctime": datetime.utcfromtimestamp(basic.get("ctime", 0)).isoformat(),
                }
                all_rows.append(row)
            # 節流
            time.sleep(1.2 + random.random() * 0.8)

    df = pd.DataFrame(all_rows)
    out_dir = Path("data/raw/shopee")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"shopee_{args.category}.parquet"
    df.to_parquet(out_file)
    print(f"✅ Saved {out_file}, rows={len(df)}")


# ==========================================================
# 直接執行時進入 main()
# ==========================================================
if __name__ == "__main__":
    main()