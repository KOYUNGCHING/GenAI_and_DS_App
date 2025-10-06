
import argparse, os, glob, pandas as pd
from src.utils.io import ensure_dir, read_parquet, to_parquet

def unify_schema(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["post_id","platform","created_at","author","text","hashtags","url","score","comments","lang"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("raw_dir", type=str)
    ap.add_argument("out_dir", type=str)
    args = ap.parse_args()

    ensure_dir(args.out_dir)
    parts = []
    for sub in ["x","reddit","zhihu"]:
        for fp in glob.glob(os.path.join(args.raw_dir, sub, "*.parquet")):
            try:
                df = read_parquet(fp)
                parts.append(unify_schema(df))
            except Exception as e:
                print("skip", fp, e)

    if not parts:
        print("No raw data found.")
        return

    all_df = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["platform","post_id"])
    out = os.path.join(args.out_dir, "all_posts.parquet")
    to_parquet(all_df, out)
    print(f"Saved: {out}, rows={len(all_df)}")

if __name__ == "__main__":
    main()
