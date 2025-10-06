import argparse, os, time, datetime as dt, requests, pandas as pd
from dotenv import load_dotenv
from langdetect import detect, LangDetectException

BASE = "https://graph.threads.net/v1.0"  # 官方 Threads API base（實際版本以你的 app 設定為準）

def detect_lang(text: str) -> str:
    try:
        return detect(text) if text else "unknown"
    except LangDetectException:
        return "unknown"

def t_get(url, params=None, headers=None):
    r = requests.get(url, params=params, headers=headers, timeout=20)
    if not r.ok:
        try:
            print("Error:", r.status_code, r.text[:500])
        except Exception:
            print("Error:", r.status_code)
        r.raise_for_status()
    return r.json()

def fetch_keyword(topic: str, start_ts: int, end_ts: int, token: str, limit: int = 50):
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{BASE}/keyword_search"
    params = {"q": topic, "since": start_ts, "until": end_ts, "limit": limit}
    while True:
        data = t_get(url, params=params, headers=headers)
        items = data.get("data", [])
        for it in items:
            yield it
        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break
        url, params = next_url, None
        time.sleep(0.2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topics", required=True, help="#ChatGPT,#AIart,#GenerativeAI (逗號分隔，可含 #)")
    ap.add_argument("--start", required=True)  # YYYY-MM-DD
    ap.add_argument("--end", required=True)    # YYYY-MM-DD (exclusive)
    ap.add_argument("--out", default="data/raw/threads/threads.parquet")
    args = ap.parse_args()

    load_dotenv()
    token = os.getenv("THREADS_ACCESS_TOKEN")
    if not token:
        raise SystemExit("缺少 THREADS_ACCESS_TOKEN，請在 .env 設定。")

    start_ts = int(dt.datetime.fromisoformat(args.start).timestamp())
    end_ts = int(dt.datetime.fromisoformat(args.end).timestamp())

    topics = [t.strip().lstrip("#") for t in args.topics.split(",") if t.strip()]
    rows = []
    for tp in topics:
        for m in fetch_keyword(tp, start_ts, end_ts, token):
            text = (m.get("caption") or m.get("text") or "").strip()
            created = m.get("timestamp") or m.get("created_time") or m.get("created_at")
            author = m.get("username") or (m.get("from") or {}).get("name") or m.get("author")
            url = m.get("permalink") or m.get("url")
            likes = m.get("like_count") or m.get("likes_count") or m.get("likeCount") or 0
            comments = m.get("comments_count") or m.get("reply_count") or m.get("commentCount") or 0

            rows.append({
                "post_id": m.get("id"),
                "platform": "threads",
                "created_at": created,
                "author": author,
                "text": text,
                "hashtags": f"#{tp}",
                "url": url,
                "score": likes,
                "comments": comments,
                "lang": detect_lang(text),
            })

    if not rows:
        print("No data.")
        return

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_parquet(args.out, index=False)
    print(f"Saved: {args.out}, rows={len(df)}")

if __name__ == "__main__":
    main()
