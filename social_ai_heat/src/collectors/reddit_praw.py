
import argparse, os, datetime as dt, time
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
from langdetect import detect, LangDetectException
from src.utils.io import to_parquet, ensure_dir
import praw

SUBREDDITS = ["MachineLearning", "artificial", "ChatGPT", "StableDiffusion", "DataScience"]

def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", type=str, default="ChatGPT OR AI art OR 生成式AI")
    ap.add_argument("--start", type=str, required=True)  # YYYY-MM-DD
    ap.add_argument("--end", type=str, required=True)    # YYYY-MM-DD
    ap.add_argument("--outdir", type=str, default="data/raw/reddit")
    ap.add_argument("--limit", type=int, default=2000)
    args = ap.parse_args()

    load_dotenv()
    cid = os.getenv("REDDIT_CLIENT_ID")
    secret = os.getenv("REDDIT_CLIENT_SECRET")
    ua = os.getenv("REDDIT_USER_AGENT")
    if not (cid and secret and ua):
        raise SystemExit("缺少 Reddit API 憑證（REDDIT_CLIENT_ID/SECRET/USER_AGENT）")

    reddit = praw.Reddit(
        client_id=cid, client_secret=secret, user_agent=ua, ratelimit_seconds=5
    )

    start_ts = int(dt.datetime.fromisoformat(args.start).timestamp())
    end_ts = int(dt.datetime.fromisoformat(args.end).timestamp())

    ensure_dir(args.outdir)
    rows = []
    for sub in SUBREDDITS:
        sr = reddit.subreddit(sub)
        # reddit 官方搜尋沒有原生時間範圍，這裡用 'time_filter="year"' + 事後過濾
        for post in tqdm(sr.search(args.query, sort="new", time_filter="year", limit=args.limit), desc=f"r/{sub}"):
            created = int(post.created_utc)
            if created < start_ts or created >= end_ts:
                continue
            text = (post.title or "") + "\n" + (post.selftext or "")
            rows.append({
                "post_id": post.id,
                "platform": "reddit",
                "created_at": dt.datetime.utcfromtimestamp(created).isoformat(),
                "author": str(post.author) if post.author else None,
                "text": text.strip(),
                "hashtags": "",  # reddit 無明確 hashtag，這裡留空
                "url": f"https://www.reddit.com{post.permalink}",
                "score": int(post.score or 0),
                "comments": int(post.num_comments or 0),
                "lang": detect_lang(text),
            })
            # 簡單節流
            time.sleep(0.05)

    if not rows:
        print("No data.")
        return

    df = pd.DataFrame(rows)
    outpath = os.path.join(args.outdir, f"reddit_{args.start}_{args.end}.parquet")
    to_parquet(df, outpath)
    print(f"Saved: {outpath}, rows={len(df)}")

if __name__ == "__main__":
    main()
