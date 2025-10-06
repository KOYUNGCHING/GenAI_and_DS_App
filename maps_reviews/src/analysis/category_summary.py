# maps_reviews/src/analysis/category_summary.py
import argparse, os, pandas as pd
import matplotlib.pyplot as plt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--plot", action="store_true")
    ap.add_argument("--plotdir", default="reports/plots")
    args = ap.parse_args()

    df = pd.read_parquet(args.inp).copy()
    df = df[df["category_main"].notna()]

    summary = (df.groupby("category_main")
                 .agg(n=("place_id","count"),
                      rating_avg=("rating","mean"),
                      rating_med=("rating","median"),
                      pop_avg=("popularity","mean"),
                      pop_med=("popularity","median"))
                 .reset_index()
                 .sort_values("n", ascending=False))

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    summary.to_csv(args.out, index=False, encoding="utf-8-sig")
    print("Saved:", args.out)
    print(summary.head(10))

    if args.plot:
        os.makedirs(args.plotdir, exist_ok=True)

        # 1) 類別樣本數
        fig = plt.figure()
        plt.bar(summary["category_main"], summary["n"])
        plt.title("各餐廳類別樣本數")
        plt.xticks(rotation=20)
        plt.ylabel("count")
        plt.savefig(os.path.join(args.plotdir, "category_counts.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 2) 類別平均評分
        fig = plt.figure()
        plt.bar(summary["category_main"], summary["rating_avg"])
        plt.title("各餐廳類別平均評分")
        plt.xticks(rotation=20)
        plt.ylabel("avg rating")
        plt.ylim(3.5, 5.0)  # 可視需求調整
        plt.savefig(os.path.join(args.plotdir, "category_rating_avg.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)

        # 3) 類別人氣（中位數）
        fig = plt.figure()
        plt.bar(summary["category_main"], summary["pop_med"])
        plt.title("各餐廳類別人氣（評論數中位數）")
        plt.xticks(rotation=20)
        plt.ylabel("median popularity")
        plt.savefig(os.path.join(args.plotdir, "category_popularity_med.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)

if __name__ == "__main__":
    main()