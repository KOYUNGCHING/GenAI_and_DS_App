# maps_reviews/src/analysis/chain_local_summary.py
import argparse, os, pandas as pd, numpy as np
import matplotlib.pyplot as plt

def save_bar(x, y, title, ylabel, outfile):
    fig = plt.figure()
    plt.bar(x, y)
    plt.title(title); plt.ylabel(ylabel)
    if len(x) > 6: plt.xticks(rotation=20)
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    plt.savefig(outfile, dpi=150, bbox_inches="tight")
    plt.close(fig)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--outdir", default="reports")
    ap.add_argument("--plotdir", default="reports/plots")
    ap.add_argument("--bycity_top", type=int, default=12)
    args = ap.parse_args()

    df = pd.read_parquet(args.inp).copy()
    df = df.dropna(subset=["rating"])
    os.makedirs(args.outdir, exist_ok=True); os.makedirs(args.plotdir, exist_ok=True)

    # 1) 全體：連鎖 vs 在地
    g = (df.groupby("is_chain")
           .agg(n=("place_id","count"),
                rating_avg=("rating","mean"),
                rating_med=("rating","median"),
                pop_med=("popularity","median"),
                price_avg=("price_level","mean"))
           .reset_index())
    g["group"] = g["is_chain"].map({True:"chain", False:"local"})
    g.to_csv(os.path.join(args.outdir, "chain_vs_local_overall.csv"), index=False, encoding="utf-8-sig")

    save_bar(g["group"], g["rating_avg"], "連鎖 vs 在地 平均評分", "avg rating",
             os.path.join(args.plotdir, "chain_vs_local_rating_avg.png"))
    save_bar(g["group"], g["pop_med"], "連鎖 vs 在地 人氣（評論數）中位數", "median popularity",
             os.path.join(args.plotdir, "chain_vs_local_pop_median.png"))

    # 2) 各縣市：連鎖占比、連鎖與在地的平均評分
    by_city = (df.groupby(["city","is_chain"])
                 .agg(n=("place_id","count"),
                      rating_avg=("rating","mean"))
                 .reset_index())
    # 連鎖占比
    total_city = by_city.groupby("city")["n"].sum().rename("n_total")
    chain_city = by_city[by_city["is_chain"]==True].set_index("city")["n"].rename("n_chain")
    ratio = (pd.concat([total_city, chain_city], axis=1)
             .fillna(0).assign(chain_ratio=lambda d: d["n_chain"]/d["n_total"])
             .sort_values("chain_ratio", ascending=False))
    ratio.to_csv(os.path.join(args.outdir, "city_chain_ratio.csv"), encoding="utf-8-sig")

    top = ratio.head(args.bycity_top)
    save_bar(top.index, top["chain_ratio"], "各縣市連鎖咖啡廳占比（Top）", "ratio",
             os.path.join(args.plotdir, "city_chain_ratio_top.png"))

    # 3) 價格層級 vs 評分（整體）
    if "price_level" in df.columns:
        corr = df[["rating","price_level"]].dropna().corr().iloc[0,1]
        with open(os.path.join(args.outdir, "corr_price_rating.txt"), "w", encoding="utf-8") as f:
            f.write(f"Pearson corr(rating, price_level) = {corr:.4f}\n")

        # 價格層級箱型圖（視覺比較）
        pv = df[["rating","price_level"]].dropna()
        if not pv.empty:
            fig = plt.figure()
            data = [pv[pv["price_level"]==k]["rating"] for k in sorted(pv["price_level"].dropna().unique())]
            labels = [f"${'$'*int(k)}" if not np.isnan(k) else "NA" for k in sorted(pv["price_level"].dropna().unique())]
            plt.boxplot(data, labels=labels)
            plt.title("不同 price_level 的評分分佈")
            plt.ylabel("rating")
            plt.savefig(os.path.join(args.plotdir, "rating_by_price_level_box.png"), dpi=150, bbox_inches="tight")
            plt.close(fig)

    print("Done. Reports in:", args.outdir, " Plots in:", args.plotdir)

if __name__ == "__main__":
    main()