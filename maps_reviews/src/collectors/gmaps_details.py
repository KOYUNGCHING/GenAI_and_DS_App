import os, time, argparse, pandas as pd
from dotenv import load_dotenv
import googlemaps
from tqdm import tqdm

FIELDS = ["name","rating","user_ratings_total","price_level","types",
          "geometry/location","formatted_address","adr_address","opening_hours",
          "reviews"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--lang", default="zh-TW")
    ap.add_argument("--max_reviews", type=int, default=5)
    args = ap.parse_args()

    load_dotenv()
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        raise SystemExit("Missing GOOGLE_MAPS_API_KEY in .env")

    gm = googlemaps.Client(key=api_key)
    places = pd.read_parquet(args.inp)
    place_ids = places["place_id"].dropna().unique().tolist()

    rows=[]
    for pid in tqdm(place_ids):
        try:
            resp = gm.place(place_id=pid, language=args.lang, fields=",".join(FIELDS))
            r = resp.get("result", {})
            reviews = (r.get("reviews") or [])[:args.max_reviews]
            for rv in reviews:
                rows.append({
                    "place_id": pid,
                    "name": r.get("name"),
                    "rating": r.get("rating"),
                    "user_ratings_total": r.get("user_ratings_total"),
                    "price_level": r.get("price_level"),
                    "formatted_address": r.get("formatted_address"),
                    "rev_rating": rv.get("rating"),
                    "rev_time": rv.get("time"),
                    "rev_lang": rv.get("language"),
                    "rev_text": rv.get("text"),
                })
            time.sleep(0.2)
        except Exception:
            continue

    if rows:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        pd.DataFrame(rows).to_parquet(args.out, index=False)
        print(f"Saved {args.out}, rows={len(rows)}")
    else:
        print("No reviews fetched.")

if __name__ == "__main__":
    main()
