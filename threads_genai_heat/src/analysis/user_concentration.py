import argparse, pandas as pd, numpy as np, json

def gini(x):
    x = np.sort(np.asarray(x, dtype=float))
    if x.size == 0 or x.sum() == 0: return 0.0
    n = x.size
    cum = np.cumsum(x)
    coeff = (np.arange(1, n+1) - 0.5)
    return float(1 - 2.0 * (coeff @ x) / (n * x.sum()))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.inp)
    agg = df.groupby("author", dropna=False)["engagement"].sum().sort_values(ascending=False)
    n = len(agg)
    top1_share = float(agg.iloc[:max(1,int(0.01*n))].sum() / agg.sum()) if n>0 and agg.sum()>0 else 0.0
    top5_share = float(agg.iloc[:max(1,int(0.05*n))].sum() / agg.sum()) if n>0 and agg.sum()>0 else 0.0
    out = {"gini": gini(agg.values), "top1_share": top1_share, "top5_share": top5_share, "n_authors": int(n)}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(out)

if __name__ == "__main__":
    main()
