import argparse, pandas as pd, numpy as np
from src.utils.io import read_parquet, to_parquet
import re

def extract_hashes(text):
    return ",".join(sorted(set(re.findall(r"#\w+", text or ""))))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--w_like", type=float, default=1.0)
    ap.add_argument("--w_comment", type=float, default=2.0)
    args = ap.parse_args()

    df = read_parquet(args.inp).copy()
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["date"] = df["created_at"].dt.date

    df["likes"] = pd.to_numeric(df.get("score"), errors="coerce").fillna(0)
    df["comments"] = pd.to_numeric(df.get("comments"), errors="coerce").fillna(0)
    df["engagement"] = args.w_like*df["likes"] + args.w_comment*df["comments"]
    df["sent_wtd"] = df["sent_score"] * (1 + np.log1p(df["engagement"]))

    df["hashtags"] = df["hashtags"].fillna("")
    df.loc[df["hashtags"].eq(""), "hashtags"] = df.loc[df["hashtags"].eq(""), "text"].map(extract_hashes)

    to_parquet(df, args.out)
    print(f"Saved: {args.out}, rows={len(df)}")

if __name__ == "__main__":
    main()
