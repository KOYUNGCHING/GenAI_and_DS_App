# maps_reviews/src/analysis/city_overview.py
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
    city_sum = (df.groupby("city")
                  .agg(n=("place_id","count"),
                       rating_avg=("rating","mean"),
                       rating_med=("rating","median"),
                       rating_std=("rating","std"),
                       popularity_sum=("popularity","sum"),
                       popularity_med=("popularity","median"))
                  .reset_index()
                  .sort_values("popularity_sum", ascending=False))

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    city_sum.to_csv(args.out, index=False, encoding="utf-8-sig")
    print("Saved:", args.out)
    print(city_sum.head())

    if args.plot:
        os.makedirs(args.plotdir, exist_ok=True)

        # 1) 各城市人氣合計（長條圖）
        fig = plt.figure()
        plt.bar(city_sum["city"], city_sum["popularity_sum"])
        plt.title("各城市人氣合計（user_ratings_total）")
        plt.xticks(rotation=20)
        plt.ylabel("popularity_sum")
        plt.savefig(os.path.join(args.plotdir, "city_popularity_sum.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 2) 各城市評分中位數（長條圖）
        fig = plt.figure()
        plt.bar(city_sum["city"], city_sum["rating_med"])
        plt.title("各城市評分中位數")
        plt.xticks(rotation=20)
        plt.ylabel("rating_median")
        plt.savefig(os.path.join(args.plotdir, "city_rating_median.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)

if __name__ == "__main__":
    main()