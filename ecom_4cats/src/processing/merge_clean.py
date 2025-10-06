import argparse, os, glob, pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    parts=[]
    for plat in ["shopee","pchome"]:
        for fp in glob.glob(os.path.join(args.raw, plat, f"{plat}_*.parquet")):
            try:
                parts.append(pd.read_parquet(fp))
            except Exception as e:
                print("skip", fp, e)

    if not parts:
        print("No raw data found."); return

    df = pd.concat(parts, ignore_index=True)
    for c in ["platform","category","title","price","rating","review_count","sales","url","query","scraped_at"]:
        if c not in df.columns: df[c]=None

    df["sales_proxy"] = df.apply(lambda r: r["review_count"] if r["platform"]=="pchome" else None, axis=1)

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce")
    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    df["sales_proxy"] = pd.to_numeric(df["sales_proxy"], errors="coerce")

    df = df.drop_duplicates(subset=["platform","url","title"], keep="first")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_parquet(args.out, index=False)
    print(f"Saved {args.out}, rows={len(df)}")

if __name__=="__main__":
    main()
