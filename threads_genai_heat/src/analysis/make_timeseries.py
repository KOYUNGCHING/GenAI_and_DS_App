import argparse, os, pandas as pd, numpy as np, matplotlib.pyplot as plt
from src.utils.io import read_parquet, to_parquet

def compute_daily(df: pd.DataFrame) -> pd.DataFrame:
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["date"] = df["created_at"].dt.tz_convert("UTC").dt.date

    def expand_hashtags(row):
        tags = [t.strip() for t in (row.get("hashtags") or "").split(",") if t.strip()]
        if not tags:
            tags = ["(no_tag)"]
        return [(row["platform"], row["date"], t, row["sentiment"], row["sent_wtd"]) for t in tags]

    rows = []
    for _, r in df.iterrows():
        rows.extend(expand_hashtags(r))
    z = pd.DataFrame(rows, columns=["platform","date","hashtag","sentiment","sent_wtd"])

    grp = z.groupby(["platform","hashtag","date"])
    daily = grp.agg(
        post_count=("sentiment","size"),
        pos=("sentiment", lambda s: (s=="pos").sum()),
        neg=("sentiment", lambda s: (s=="neg").sum()),
        sent_wtd_mean=("sent_wtd","mean")
    ).reset_index()
    daily["pos_ratio"] = daily["pos"] / daily["post_count"]
    daily["neg_ratio"] = daily["neg"] / daily["post_count"]

    daily = daily.sort_values(["platform","hashtag","date"])
    daily["ma7"] = daily.groupby(["platform","hashtag"])["post_count"].transform(lambda s: s.rolling(7, min_periods=3).mean())
    daily["std7"] = daily.groupby(["platform","hashtag"])["post_count"].transform(lambda s: s.rolling(7, min_periods=3).std())
    daily["zscore_7d"] = (daily["post_count"] - daily["ma7"]) / daily["std7"]
    return daily

def plot_charts(daily: pd.DataFrame, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    for (plat, tag), grp in daily.groupby(["platform","hashtag"]):
        fig = plt.figure()
        plt.title(f"{plat} | {tag}")
        plt.plot(grp["date"], grp["post_count"], label="post_count")
        plt.plot(grp["date"], grp["ma7"], label="ma7")
        plt.legend(); plt.xlabel("date"); plt.ylabel("count")
        fig.autofmt_xdate()
        fp = os.path.join(out_dir, f"{plat}_{tag.replace('#','')}.png")
        plt.savefig(fp, dpi=150, bbox_inches="tight")
        plt.close(fig)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--plot", action="store_true")
    ap.add_argument("--plotdir", default="reports/daily_charts")
    args = ap.parse_args()

    df = read_parquet(args.inp)
    daily = compute_daily(df)
    to_parquet(daily, args.out)
    print(f"Saved: {args.out}, rows={len(daily)}")

    if args.plot:
        plot_charts(daily, args.plotdir)
        print(f"Charts saved to: {args.plotdir}")

if __name__ == "__main__":
    main()
