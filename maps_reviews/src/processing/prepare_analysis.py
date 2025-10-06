# maps_reviews/src/processing/prepare_analysis.py
import argparse, os, pandas as pd, numpy as np

# 以 price_level(0~4) 指派代表人均價（NTD）— 可依你想法微調
REP_PRICE = {0:200, 1:200, 2:400, 3:700, 4:1000}

def level_to_rep_price(v):
    if pd.isna(v): return np.nan
    try:
        return REP_PRICE.get(int(v), np.nan)
    except Exception:
        return np.nan

def band4_from_price(p):
    """門檻：200 / 500 / 800 → 四級"""
    if pd.isna(p): return "未知"
    p = float(p)
    if p <= 200: return "<=200"
    elif p <= 500: return "201-500"
    elif p <= 800: return "501-800"
    else: return ">800"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--places", required=True)
    ap.add_argument("--reviews", required=False, default=None)  # 可省略
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    places = pd.read_parquet(args.places).copy()
    places = places.rename(columns={"user_ratings_total":"popularity","types":"types_csv"})
    places["rating"] = pd.to_numeric(places["rating"], errors="coerce")
    places["popularity"] = pd.to_numeric(places["popularity"], errors="coerce")

    # 價格代表值與四級分箱
    places["rep_price"] = places["price_level"].apply(level_to_rep_price)
    places["price_band4"] = places["rep_price"].apply(band4_from_price)

    # 若有評論檔，可合併（沒有就給空欄位）
    if args.reviews and os.path.exists(args.reviews):
        reviews = pd.read_parquet(args.reviews).copy()
        reviews["rev_len"] = reviews["rev_text"].fillna("").str.len()
        rev_agg = (reviews.groupby("place_id")
                   .agg(rev_count=("rev_text","count"),
                        rev_avg_rating=("rev_rating","mean"),
                        rev_avg_len=("rev_len","mean"))
                   .reset_index())
        df = places.merge(rev_agg, on="place_id", how="left")
    else:
        df = places.copy()
        df["rev_count"]=0; df["rev_avg_rating"]=np.nan; df["rev_avg_len"]=np.nan

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_parquet(args.out, index=False)
    print(f"Saved {args.out}, rows={len(df)}")

if __name__ == "__main__":
    main()