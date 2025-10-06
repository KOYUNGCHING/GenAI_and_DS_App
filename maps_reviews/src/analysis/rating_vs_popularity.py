# maps_reviews/src/analysis/rating_vs_popularity.py
import argparse, os, pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--plotdir", default="reports/plots")
    args = ap.parse_args()

    df = pd.read_parquet(args.inp).copy()
    df = df.dropna(subset=["rating","popularity"])

    os.makedirs(args.plotdir, exist_ok=True)

    # 1) 全體散點（log1p 人氣）
    fig = plt.figure()
    x = np.log1p(df["popularity"].astype(float))
    y = df["rating"].astype(float)
    plt.scatter(x, y, alpha=0.25, s=10)
    plt.xlabel("log(1 + popularity)")
    plt.ylabel("rating")
    plt.title("評分 vs 人氣")
    plt.savefig(os.path.join(args.plotdir, "rating_vs_logpop_all.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)

    # 2) 各類別平均點（更穩定）
    grp = (df.groupby("category_main")
             .agg(logpop_mean=("popularity", lambda s: np.log1p(s).mean()),
                  rating_mean=("rating","mean"),
                  n=("place_id","count"))
             .reset_index()
             .sort_values("n", ascending=False))

    fig = plt.figure()
    plt.scatter(grp["logpop_mean"], grp["rating_mean"])
    for _, r in grp.iterrows():
        plt.text(r["logpop_mean"], r["rating_mean"], r["category_main"], fontsize=8)
    plt.xlabel("avg log(1+popularity)")
    plt.ylabel("avg rating")
    plt.title("各類別：平均人氣 vs 平均評分")
    plt.savefig(os.path.join(args.plotdir, "rating_vs_logpop_by_category.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)

if __name__ == "__main__":
    main()