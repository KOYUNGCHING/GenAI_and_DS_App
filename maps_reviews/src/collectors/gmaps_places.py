import os, time, argparse, pandas as pd
from dotenv import load_dotenv
import googlemaps
from tqdm import tqdm

# ---- 小工具：帶重試的 single call ----
def _places_nearby_once(gm, **kwargs):
    for i in range(3):  # 最多重試3次
        try:
            return gm.places_nearby(**kwargs)
        except Exception as e:
            if i == 2:
                raise
            time.sleep(1.5 + i)  # 逐步退避
    return {"results": []}

def nearby_pages(gm, location, radius, language, max_pages=20, keyword="咖啡 OR coffee OR cafe", gtype="cafe"):
    """
    使用 keyword + type='cafe' 拉高涵蓋率，並允許較多頁數。
    Google Places Nearby 一頁最多 ~20，通常最多能到 3 頁，
    但我們將 max_pages 開放，保留彈性（若沒有 token 就會提前結束）。
    """
    page = 0
    results, token = [], None

    while True:
        if page == 0:
            # 第1頁：帶 location / radius / keyword / type
            resp = _places_nearby_once(
                gm,
                location=location,
                radius=radius,
                keyword=keyword,
                type=gtype,
                language=language
            )
        else:
            # 後續頁：只要 page_token + language（其他參數會被忽略）
            time.sleep(2.0)  # 等 token 生效
            resp = _places_nearby_once(
                gm,
                page_token=token,
                language=language
            )

        batch = resp.get("results", [])
        results.extend(batch)

        token = resp.get("next_page_token")
        page += 1

        # 沒有下一頁，或達到上限就停
        if not token or page >= max_pages:
            break

        # 輕微間隔避免節流
        time.sleep(1.2)

    return results

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--areas", required=True, help="例如 data/areas/areas_tw.csv")
    ap.add_argument("--max_pages", type=int, default=20, help="每個地點最多翻頁數，預設20（通常實際到3頁就結束）")
    ap.add_argument("--lang", default="zh-TW")
    ap.add_argument("--outdir", default="data/raw/places")
    ap.add_argument("--keyword", default="咖啡 OR coffee OR cafe", help="關鍵字（預設含中英）")
    ap.add_argument("--type", default="cafe", help="Google Places type，預設 cafe")
    ap.add_argument("--also_csv", action="store_true", help="同時輸出 CSV 方便檢查（預設只輸出 parquet）")
    args = ap.parse_args()

    load_dotenv()
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        raise SystemExit("Missing GOOGLE_MAPS_API_KEY in .env")

    gm = googlemaps.Client(key=api_key)
    areas = pd.read_csv(args.areas)

    # 檢查欄位
    must = {"city","area_name","lat","lng","radius_m"}
    if not must.issubset(areas.columns):
        raise SystemExit(f"{args.areas} 必須包含欄位: {sorted(must)}")

    os.makedirs(args.outdir, exist_ok=True)
    all_parts = []

    for _, row in areas.iterrows():
        city, area = row["city"], row["area_name"]
        lat, lng, r = float(row["lat"]), float(row["lng"]), int(row["radius_m"])

        print(f"📍 Fetching: {city}-{area} (radius={r}, keyword='{args.keyword}', type='{args.type}') ...")
        res = nearby_pages(
            gm,
            (lat, lng),
            r,
            args.lang,
            max_pages=args.max_pages,
            keyword=args.keyword,
            gtype=args.type
        )

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
            "types": ",".join(x.get("types",[])) if x.get("types") else None,
            "address": x.get("vicinity"),
        } for x in res])

        out_parquet = os.path.join(args.outdir, f"places_{city}_{area}.parquet")
        df.to_parquet(out_parquet, index=False)
        if args.also_csv:
            out_csv = os.path.join(args.outdir, f"places_{city}_{area}.csv")
            df.to_csv(out_csv, index=False, encoding="utf-8-sig")

        print(f"   ↳ Saved {out_parquet}, rows={len(df)}")
        all_parts.append(df)
        time.sleep(0.8)  # 城市/區與區之間稍微停一下

    if all_parts:
        merged = pd.concat(all_parts, ignore_index=True).drop_duplicates(subset=["place_id"])
        os.makedirs("data/processed", exist_ok=True)
        merged_out = "data/processed/places_all.parquet"
        merged.to_parquet(merged_out, index=False)
        if args.also_csv:
            merged_csv = "data/processed/places_all.csv"
            merged.to_csv(merged_csv, index=False, encoding="utf-8-sig")
        print("✅ Merged ->", merged_out, "rows=", len(merged))

if __name__ == "__main__":
    main()