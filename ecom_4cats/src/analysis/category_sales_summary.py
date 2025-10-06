import argparse, os, pandas as pd, numpy as np, matplotlib.pyplot as plt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--plot", action="store_true")
    ap.add_argument("--plotdir", default="reports")
    args = ap.parse_args()

    df = pd.read_parquet(args.inp)
    df["sales_metric"] = np.where(df["platform"].eq("shopee"), df["sales"], df["sales_proxy"])

    summary = (df.groupby(["platform","category"])
               .agg(items=("title","count"),
                    sales_sum=("sales_metric","sum"),
                    sales_med=("sales_metric","median"),
                    price_med=("price","median"),
                    rating_med=("rating","median"),
                    review_med=("review_count","median"))
               .reset_index())

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    summary.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"Saved {args.out}")
    print(summary)

    if args.plot:
        os.makedirs(args.plotdir, exist_ok=True)
        for plat, sub in summary.groupby("platform"):
            sub = sub.sort_values("sales_sum", ascending=False)
            fig = plt.figure()
            plt.bar(sub["category"], sub["sales_sum"])
            plt.title(f"{plat}｜四大品類銷售指標（合計）")
            plt.ylabel("sales_sum（Shopee: historical_sold；PChome: review_count proxy）")
            plt.xlabel("category")
            plt.xticks(rotation=15)
            fp = os.path.join(args.plotdir, f"category_sales_bar_{plat}.png")
            plt.savefig(fp, dpi=150, bbox_inches="tight")
            plt.close(fig)
        print(f"Charts saved to: {args.plotdir}")

if __name__=="__main__":
    main()
