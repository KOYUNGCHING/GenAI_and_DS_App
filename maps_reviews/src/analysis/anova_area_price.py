import argparse, pandas as pd
import statsmodels.api as sm
from statsmodels.formula.api import ols

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--metric", required=True, choices=["rating","popularity"])
    ap.add_argument("--out", dest="out", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.inp)
    df = df.dropna(subset=[args.metric])
    df = df[df["price_label"].notna() & df["area_name"].notna()]

    formula = f"{args.metric} ~ C(area_name) + C(price_label) + C(area_name):C(price_label)"
    model = ols(formula, data=df).fit()
    aov = sm.stats.anova_lm(model, typ=2)
    aov.to_csv(args.out, encoding="utf-8-sig")
    print("Saved:", args.out)
    print(aov)

if __name__ == "__main__":
    main()
