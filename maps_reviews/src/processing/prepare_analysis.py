import argparse, os, pandas as pd, numpy as np

def map_price_label(v):
    if pd.isna(v): return "未知"
    m = {0:"$",1:"$",2:"$$",3:"$$$",4:"$$$$"}
    try:
        return m.get(int(v), "未知")
    except Exception:
        return "未知"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--places", required=True)
    ap.add_argument("--reviews", required=False, default=None)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    places = pd.read_parquet(args.places)
    places = places.rename(columns={"user_ratings_total":"popularity","types":"types_csv"})
    places["price_label"] = places["price_level"].apply(map_price_label)
    places["rating"] = pd.to_numeric(places["rating"], errors="coerce")
    places["popularity"] = pd.to_numeric(places["popularity"], errors="coerce")

    if args.reviews and os.path.exists(args.reviews):
        reviews = pd.read_parquet(args.reviews)
        reviews["rev_len"] = reviews["rev_text"].fillna("").str.len()
        # 保留評論與店家 join 欄位
        merged = places.merge(reviews.groupby("place_id")
                              .agg(rev_count=("rev_text","count"),
                                   rev_avg_rating=("rev_rating","mean"),
                                   rev_avg_len=("rev_len","mean")).reset_index(),
                              on="place_id", how="left")
    else:
        merged = places.copy()
        merged["rev_count"]=0; merged["rev_avg_rating"]=None; merged["rev_avg_len"]=None

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    merged.to_parquet(args.out, index=False)
    print(f"Saved {args.out}, rows={len(merged)}")

if __name__ == "__main__":
    main()
