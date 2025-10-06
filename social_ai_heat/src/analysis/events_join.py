
import argparse, pandas as pd
from src.utils.io import read_parquet

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)  # CSV: date,title,source
    ap.add_argument("--metrics", required=True) # Parquet daily_metrics
    ap.add_argument("--out", required=True)
    ap.add_argument("--z", type=float, default=2.0)  # 尖峰門檻
    args = ap.parse_args()

    events = pd.read_csv(args.events, parse_dates=["date"]).assign(date=lambda d: d["date"].dt.date)
    daily = read_parquet(args.metrics).copy()
    daily["date"] = pd.to_datetime(daily["date"]).dt.date

    peaks = daily[daily["zscore_7d"] >= args.z]
    out = peaks.merge(events, on="date", how="left").sort_values(["date","platform","hashtag"])
    out.to_csv(args.out, index=False)
    print(f"Saved: {args.out}, rows={len(out)}")

if __name__ == "__main__":
    main()
