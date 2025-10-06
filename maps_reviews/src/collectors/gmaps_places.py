import os, time, argparse, pandas as pd
from dotenv import load_dotenv
import googlemaps
from tqdm import tqdm

def nearby_pages(gm, location, radius, language, max_pages=3):
    page = 0
    results, token = [], None
    while True:
        if page==0:
            resp = gm.places_nearby(location=location, radius=radius, type="restaurant", language=language)
        else:
            time.sleep(2)
            resp = gm.places_nearby(page_token=token, language=language)
        results.extend(resp.get("results", []))
        token = resp.get("next_page_token")
        page += 1
        if not token or page>=max_pages: break
        time.sleep(1.2)
    return results

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--areas", required=True)
    ap.add_argument("--max_pages", type=int, default=3)
    ap.add_argument("--lang", default="zh-TW")
    ap.add_argument("--outdir", default="data/raw/places")
    args = ap.parse_args()

    load_dotenv()
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        raise SystemExit("Missing GOOGLE_MAPS_API_KEY in .env")

    gm = googlemaps.Client(key=api_key)
    areas = pd.read_csv(args.areas)

    os.makedirs(args.outdir, exist_ok=True)
    all_parts=[]
    for _, row in areas.iterrows():
        city, area, lat, lng, r = row["city"], row["area_name"], row["lat"], row["lng"], int(row["radius_m"])
        print(f"Fetching: {city}-{area} ...")
        res = nearby_pages(gm, (lat, lng), r, args.lang, args.max_pages)
        df = pd.DataFrame([{
            "city": city,
            "area_name": area,
            "place_id": x.get("place_id"),
            "name": x.get("name"),
            "lat": x.get("geometry",{}).get("location",{}).get("lat"),
            "lng": x.get("geometry",{}).get("location",{}).get("lng"),
            "rating": x.get("rating"),
            "user_ratings_total": x.get("user_ratings_total"),
            "price_level": x.get("price_level"),
            "types": ",".join(x.get("types",[])),
            "address": x.get("vicinity"),
        } for x in res])
        out = os.path.join(args.outdir, f"places_{city}_{area}.parquet")
        df.to_parquet(out, index=False)
        print(f"Saved {out}, rows={len(df)}")
        all_parts.append(df)
        time.sleep(1.0)

    if all_parts:
        merged = pd.concat(all_parts, ignore_index=True).drop_duplicates(subset=["place_id"])
        os.makedirs("data/processed", exist_ok=True)
        merged.to_parquet("data/processed/places_all.parquet", index=False)
        print("Merged -> data/processed/places_all.parquet, rows=", len(merged))

if __name__ == "__main__":
    main()
