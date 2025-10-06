import argparse, pandas as pd
from src.utils.io import read_parquet

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics", required=True)  # data/gold/daily_metrics.parquet
    ap.add_argument("--posts", required=True)    # data/silver/with_features.parquet
    ap.add_argument("--out", required=True)      # CSV
    ap.add_argument("--z", type=float, default=2.0)
    ap.add_argument("--window", type=int, default=0, help="0=當日；>0 代表前後天數窗口")
    ap.add_argument("--topk", type=int, default=5)
    args = ap.parse_args()

    daily = read_parquet(args.metrics).copy()
    posts = read_parquet(args.posts).copy()

    daily["date"] = pd.to_datetime(daily["date"]).dt.date
    posts["date"] = pd.to_datetime(posts["created_at"]).dt.date

    peaks = daily[daily["zscore_7d"] >= args.z].copy()

    rows = []
    for _, p in peaks.iterrows():
        d = p["date"]; tag = p["hashtag"]; plat = p["platform"]
        mask = (posts["date"] >= (pd.to_datetime(d) - pd.Timedelta(days=args.window)).date()) & \
               (posts["date"] <= (pd.to_datetime(d) + pd.Timedelta(days=args.window)).date())
        sub = posts[mask & posts["platform"].eq(plat)].copy()
        sub = sub[sub["hashtags"].fillna("").str.contains(tag.replace("#",""))]
        top = sub.sort_values("engagement", ascending=False).head(args.topk)
        for _, r in top.iterrows():
            rows.append({
                "date": str(d),
                "platform": plat,
                "hashtag": tag,
                "zscore_7d": p["zscore_7d"],
                "post_id": r["post_id"],
                "author": r.get("author"),
                "engagement": r.get("engagement"),
                "sentiment": r.get("sentiment"),
                "text": (r.get("text") or "").replace("\n"," ")[:200],
                "url": r.get("url"),
            })

    out_df = pd.DataFrame(rows)
    out_df.to_csv(args.out, index=False)
    print(f"Saved: {args.out}, rows={len(out_df)}")

if __name__ == "__main__":
    main()
