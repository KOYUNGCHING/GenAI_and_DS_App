import argparse, os, pandas as pd, matplotlib.pyplot as plt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--plot", action="store_true")
    ap.add_argument("--plotdir", default="reports/charts")
    args = ap.parse_args()

    df = pd.read_parquet(args.inp)
    summary = (df.groupby(["city","area_name"])
                 .agg(n=("place_id","count"),
                      rating_avg=("rating","mean"),
                      rating_med=("rating","median"),
                      popularity_sum=("popularity","sum"),
                      popularity_med=("popularity","median"))
                 .reset_index())
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    summary.to_csv(args.out, index=False, encoding="utf-8-sig")
    print("Saved:", args.out)

    if args.plot:
        os.makedirs(args.plotdir, exist_ok=True)
        for city, sub in summary.groupby("city"):
            fig = plt.figure()
            sub = sub.sort_values("popularity_sum", ascending=False)
            plt.bar(sub["area_name"], sub["popularity_sum"])
            plt.title(f"{city} 各區餐廳人氣合計（user_ratings_total）")
            plt.xlabel("area")
            plt.ylabel("popularity_sum")
            plt.xticks(rotation=20)
            fp = os.path.join(args.plotdir, f"{city}_popularity_sum.png")
            plt.savefig(fp, dpi=150, bbox_inches="tight")
            plt.close(fig)
        print("Charts saved to:", args.plotdir)

if __name__ == "__main__":
    main()
