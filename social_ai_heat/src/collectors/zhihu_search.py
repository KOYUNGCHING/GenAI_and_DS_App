
import argparse, os, time, datetime as dt, pandas as pd, requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed
from langdetect import detect, LangDetectException
from src.utils.io import to_parquet, ensure_dir

SEARCH_API = "https://www.zhihu.com/api/v4/search_v3"

def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def zhihu_search(query: str, page: int, cookie: str):
    params = {
        "t": "general",
        "q": query,
        "correction": "1",
        "offset": page * 20
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Cookie": cookie
    }
    resp = requests.get(SEARCH_API, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", type=str, default="ChatGPT 人工智慧 生成式AI")
    ap.add_argument("--pages", type=int, default=2)
    ap.add_argument("--outdir", type=str, default="data/raw/zhihu")
    args = ap.parse_args()

    load_dotenv()
    cookie = os.getenv("ZHIHU_COOKIE")
    if not cookie:
        raise SystemExit("需要 ZHIHU_COOKIE（自行從瀏覽器複製登入後 cookie），並遵守平台條款。")

    ensure_dir(args.outdir)
    rows = []
    for p in range(args.pages):
        data = zhihu_search(args.query, p, cookie)
        for item in data.get("data", []):
            # 只取常見的 question/answer 類型
            target = item.get("object", {})
            title = target.get("title", "") or target.get("question", {}).get("name", "")
            excerpt = target.get("excerpt", "") or target.get("description", "") or ""
            url = target.get("url", "") or target.get("question", {}).get("url", "")
            created = target.get("created_time") or target.get("updated_time")
            if created:
                created_iso = dt.datetime.utcfromtimestamp(int(created)).isoformat()
            else:
                created_iso = dt.datetime.utcnow().isoformat()

            text = (title + "\n" + excerpt).strip()
            rows.append({
                "post_id": str(target.get("id") or target.get("question", {}).get("id") or ""),
                "platform": "zhihu",
                "created_at": created_iso,
                "author": None,
                "text": text,
                "hashtags": "",
                "url": url,
                "score": None,
                "comments": None,
                "lang": detect_lang(text) if text else "zh",
            })
        time.sleep(1)

    if not rows:
        print("No data.")
        return

    df = pd.DataFrame(rows)
    outpath = os.path.join(args.outdir, f"zhihu_{dt.date.today().isoformat()}.parquet")
    to_parquet(df, outpath)
    print(f"Saved: {outpath}, rows={len(df)}")

if __name__ == "__main__":
    main()
