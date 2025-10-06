# maps_reviews/src/processing/prepare_analysis.py
import argparse, os, re, pandas as pd, numpy as np

# 連鎖品牌關鍵字（可自行擴充）
CHAIN_MAP = {
    r"星巴克|Starbucks": "Starbucks",
    r"85 ?度?C|85°C|85c": "85°C",
    r"路易莎|Louisa": "Louisa",
    r"cama|Cama": "Cama",
    r"伯朗|Mr\.?\s*Brown": "Mr. Brown",
    r"丹堤|Dante": "Dante",
    r"麥當勞|McDonald'?s?": "McDonalds",
    r"肯德基|KFC": "KFC",
}

def detect_chain(name: str):
    if not isinstance(name, str): return None
    for pat, cname in CHAIN_MAP.items():
        if re.search(pat, name, flags=re.IGNORECASE):
            return cname
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--places", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.places).copy()
    # 欄位統一
    if "types" in df.columns and "types_csv" not in df.columns:
        df = df.rename(columns={"types":"types_csv"})
    df = df.rename(columns={"user_ratings_total":"popularity"})

    # 數值型轉換
    df["rating"]     = pd.to_numeric(df["rating"], errors="coerce")
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")
    # price_level 可能缺值，保留 NaN 即可
    if "price_level" in df.columns:
        df["price_level"] = pd.to_numeric(df["price_level"], errors="coerce")

    # 連鎖偵測
    df["chain_name"] = df["name"].apply(detect_chain)
    df["is_chain"]   = df["chain_name"].notna()

    # 去除前一版留下的欄位（保險）
    drop_cols = [c for c in ["rep_price","price_band4","category_main"] if c in df.columns]
    if drop_cols: df = df.drop(columns=drop_cols)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_parquet(args.out, index=False)
    print(f"Saved {args.out}, rows={len(df)}")
    print("Chain ratio =", df["is_chain"].mean().round(3) if len(df)>0 else 0)

if __name__ == "__main__":
    main()