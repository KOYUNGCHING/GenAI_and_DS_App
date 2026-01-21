
import argparse, datetime as dt, pandas as pd
from tqdm import tqdm
from langdetect import detect, LangDetectException
from src.utils.io import to_parquet, ensure_dir
import os

# snscrape 的 Twitter 模組仍以 "twitter" 命名，實際對應 X
try:
    import snscrape.modules.twitter as sntwitter
except Exception as e:
    raise SystemExit("請先安裝 snscrape：pip install snscrape\n或 requirements.txt 中的 git 版本。") from e

def detect_lang(text: str) -> str:
    try:
        return detect(text)
    except LangDetectException:
        return "unknown"

def iter_tweets(query: str):
    # sntwitter.TwitterSearchScraper(query).get_items()
    scraper = sntwitter.TwitterSearchScraper(query)
    for tw in scraper.get_items():
        yield tw

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hashtags", type=str, default="#ChatGPT,#AIart")
    ap.add_argument("--since", type=str, required=True)   # YYYY-MM-DD
    ap.add_argument("--until", type=str, required=True)   # YYYY-MM-DD (不含當日)
    ap.add_argument("--outdir", type=str, default="data/raw/x")
    args = ap.parse_args()

    tags = [t.strip() for t in args.hashtags.split(",") if t.strip()]
    since = dt.date.fromisoformat(args.since)
    until = dt.date.fromisoformat(args.until)

    ensure_dir(args.outdir)

    all_rows = []
    for tag in tags:
        query = f'({tag}) since:{since} until:{until}'
        print(f"[X] Query: {query}")
        for tw in tqdm(iter_tweets(query), desc=f"scrape {tag}"):
            text = tw.content or ""
            all_rows.append({
                "post_id": tw.id,
                "platform": "x",
                "created_at": tw.date.isoformat(),
                "author": getattr(tw.user, "username", None),
                "text": text,
                "hashtags": ",".join([h for h in tw.hashtags or []]),
                "url": tw.url,
                "score": getattr(tw, "likeCount", None),
                "comments": getattr(tw, "replyCount", None),
                "lang": detect_lang(text),
            })

    if not all_rows:
        print("No data.")
        return

    df = pd.DataFrame(all_rows)
    outpath = os.path.join(args.outdir, f"x_{since}_{until}.parquet")
    to_parquet(df, outpath)
    print(f"Saved: {outpath}, rows={len(df)}")

if __name__ == "__main__":
    main()
