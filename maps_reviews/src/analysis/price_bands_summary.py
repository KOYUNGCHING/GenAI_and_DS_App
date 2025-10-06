# maps_reviews/src/analysis/price_bands_summary.py
import argparse, os, pandas as pd, numpy as np
import matplotlib.pyplot as plt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--plot", action="store_true")
    ap.add_argument("--plotdir", default="reports/charts")
    args = ap.parse_args()

    df = pd.read_parquet(args.inp)
    sub = df[df["price_band4"].notna() & (df["price_band4"]!="未知")].copy()

    summary = (sub.groupby("price_band4")
                 .agg(n=("place_id","count"),
                      rating_avg=("rating","mean"),
                      rating_med=("rating","median"),
                      popularity_avg=("popularity","mean"),
                      popularity_med=("popularity","median"))
                 .reset_index()
                 .sort_values("price_band4"))

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    summary.to_csv(args.out, index=False, encoding="utf-8-sig")
    print("Saved:", args.out)
    print(summary)

    if args.plot:
        os.makedirs(args.plotdir, exist_ok=True)

        # 1) 各價格級樣本數
        fig = plt.figure()
        plt.bar(summary["price_band4"], summary["n"])
        plt.title("各價格級樣本數")
        plt.xlabel("price band")
        plt.ylabel("count")
        plt.savefig(os.path.join(args.plotdir, "price_band4_counts.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 2) 各價格級 平均評分
        fig = plt.figure()
        plt.plot(summary["price_band4"], summary["rating_avg"], marker="o")
        plt.title("各價格級的平均評分")
        plt.xlabel("price band")
        plt.ylabel("avg rating")
        plt.savefig(os.path.join(args.plotdir, "price_band4_rating_avg.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 3) 各價格級 人氣中位數
        fig = plt.figure()
        plt.plot(summary["price_band4"], summary["popularity_med"], marker="o")
        plt.title("各價格級的人氣（評論數）中位數")
        plt.xlabel("price band")
        plt.ylabel("median popularity")
        plt.savefig(os.path.join(args.plotdir, "price_band4_popularity_med.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)

        print("Charts saved to:", args.plotdir)

if __name__ == "__main__":
    main()