# maps_reviews/src/analysis/corr_and_reg.py
import argparse, os, pandas as pd, numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm

def add_const(x): return sm.add_constant(x, has_constant='add')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--outdir", default="reports")
    ap.add_argument("--plot", action="store_true")
    ap.add_argument("--plotdir", default="reports/charts")
    args = ap.parse_args()

    df = pd.read_parquet(args.inp).copy()
    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs(args.plotdir, exist_ok=True)

    # ① 皮爾森相關係數（rating / popularity / rep_price）
    corr_cols = ["rating", "popularity", "rep_price"]
    corr_df = df[corr_cols].corr(method="pearson")
    corr_path = os.path.join(args.outdir, "correlation_matrix.csv")
    corr_df.to_csv(corr_path, encoding="utf-8-sig")
    print("Saved:", corr_path)
    print(corr_df)

    # ② 簡單迴歸：rating ~ rep_price
    sub1 = df[["rating", "rep_price"]].dropna()
    X1 = add_const(sub1["rep_price"])
    y1 = sub1["rating"]
    m1 = sm.OLS(y1, X1).fit()
    with open(os.path.join(args.outdir, "reg_rating_rep_price.txt"), "w", encoding="utf-8") as f:
        f.write(m1.summary().as_text())
    print("Saved: reg_rating_rep_price.txt")

    # ③ 擴充：rating ~ rep_price + log1p(popularity)
    sub2 = df[["rating", "rep_price", "popularity"]].dropna()
    sub2["log_pop"] = np.log1p(sub2["popularity"])
    X2 = add_const(sub2[["rep_price","log_pop"]])
    y2 = sub2["rating"]
    m2 = sm.OLS(y2, X2).fit()
    with open(os.path.join(args.outdir, "reg_rating_price_logpop.txt"), "w", encoding="utf-8") as f:
        f.write(m2.summary().as_text())
    print("Saved: reg_rating_price_logpop.txt")

    if args.plot:
        # A) rating vs rep_price（含回歸線）
        fig = plt.figure()
        plt.scatter(sub1["rep_price"], sub1["rating"], alpha=0.25)
        xs = np.linspace(sub1["rep_price"].min(), sub1["rep_price"].max(), 100)
        ys = m1.params["const"] + m1.params["rep_price"] * xs
        plt.plot(xs, ys)
        plt.xlabel("rep_price")
        plt.ylabel("rating")
        plt.title("rating ~ rep_price")
        plt.savefig(os.path.join(args.plotdir, "reg_rating_rep_price.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)

        # B) rating vs log(popularity)
        fig = plt.figure()
        plt.scatter(sub2["log_pop"], sub2["rating"], alpha=0.25)
        plt.xlabel("log(1+popularity)")
        plt.ylabel("rating")
        plt.title("rating vs log popularity")
        plt.savefig(os.path.join(args.plotdir, "rating_vs_logpop.png"), dpi=150, bbox_inches="tight")
        plt.close(fig)

if __name__ == "__main__":
    main()