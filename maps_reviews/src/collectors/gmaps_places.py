# maps_reviews/src/collectors/gmaps_places.py
import os, time, argparse, pandas as pd
from dotenv import load_dotenv
import googlemaps

def text_pages(gm: googlemaps.Client, query: str, language: str, max_pages: int = 3):
    """
    使用 Google Places Text Search 依 query 抓取多頁結果。
    每頁 ~20 筆；以 next_page_token 翻頁。回傳 list[dict].
    """
    results, token, page = [], None, 0
    while True:
        if page == 0:
            resp = gm.places(query=query, language=language)
        else:
            # 官方建議等待 2 秒讓 next_page_token 生效
            time.sleep(2.0)
            resp = gm.places(query=None, page_token=token, language=language)

        results.extend(resp.get("results", []))
        token = resp.get("next_page_token")
        page += 1
        if not token or page >= max_pages:
            break
        # 溫和節流，避免 QPS 過高
        time.sleep(1.2)
    return results

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--areas", required=True, help="CSV 檔，欄位需包含 city,area_name,keyword")
    ap.add_argument("--max_pages", type=int, default=3, help="每區最多抓幾頁（每頁約 20 筆）")
    ap.add_argument("--lang", default="zh-TW")
    ap.add_argument("--outdir", default="data/raw/places")
    args = ap.parse_args()

    load_dotenv()
    api_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Missing GOOGLE_MAPS_API_KEY in .env")

    gm = googlemaps.Client(key=api_key)
    areas = pd.read_csv(args.areas)  # 需要 city, area_name, keyword 三欄

    os.makedirs(args.outdir, exist_ok=True)
    all_parts = []
    total = 0

    for _, row in areas.iterrows():
        city, area, kw = str(row["city"]).strip(), str(row["area_name"]).strip(), str(row["keyword"]).strip()
        query = kw if kw else f"{city}{area} 餐廳"
        print(f"Fetching: {city}-{area} | query='{query}' ...")

        res = text_pages(gm, query, language=args.lang, max_pages=args.max_pages)

        # 注意：不再存經緯度（lat/lng）
        df = pd.DataFrame([{
            "city": city,
            "area_name": area,
            "place_id": x.get("place_id"),
            "name": x.get("name"),
            "rating": x.get("rating"),
            "user_ratings_total": x.get("user_ratings_total"),
            "price_level": x.get("price_level"),
            "types": ",".join(x.get("types", [])),
            "address": x.get("formatted_address") or x.get("vicinity"),
        } for x in res])

        out = os.path.join(args.outdir, f"places_{city}_{area}.parquet")
        df.to_parquet(out, index=False)
        print(f"  Saved {out}, rows={len(df)}")
        all_parts.append(df)
        total += len(df)
        time.sleep(0.8)  # 每區之間再稍微休息

    if all_parts:
        merged = pd.concat(all_parts, ignore_index=True).drop_duplicates(subset=["place_id"])
        os.makedirs("data/processed", exist_ok=True)
        merged.to_parquet("data/processed/places_all.parquet", index=False)
        print("Merged -> data/processed/places_all.parquet, rows=", len(merged))
        print(f"Total raw rows before dedup: {total}")
    else:
        print("No data fetched. 請檢查 API 金鑰與 areas CSV 內容。")

if __name__ == "__main__":
    main()