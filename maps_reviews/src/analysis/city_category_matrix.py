# maps_reviews/src/analysis/city_category_matrix.py
import argparse, os, pandas as pd
import matplotlib.pyplot as plt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)  # CSV
    ap.add_argument("--plot", action="store_true")
    ap.add_argument("--plotfile", default="reports/plots/city_category_heatmap.png")
    args = ap.parse_args()

    df = pd.read_parquet(args.inp).copy()
    tab = pd.pivot_table(df, index="city", columns="category_main",
                         values="place_id", aggfunc="count", fill_value=0)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    tab.to_csv(args.out, encoding="utf-8-sig")
    print("Saved:", args.out)

    if args.plot:
        fig = plt.figure(figsize=(max(8, tab.shape[1]*0.6), max(6, tab.shape[0]*0.45)))
        plt.imshow(tab.values, aspect="auto")
        plt.yticks(range(len(tab.index)), tab.index)
        plt.xticks(range(len(tab.columns)), tab.columns, rotation=30, ha="right")
        plt.colorbar(label="count")
        plt.title("城市 × 餐廳類別 熱度圖")
        os.makedirs(os.path.dirname(args.plotfile), exist_ok=True)
        plt.savefig(args.plotfile, dpi=150, bbox_inches="tight")
        plt.close(fig)
        print("Heatmap saved:", args.plotfile)

if __name__ == "__main__":
    main()