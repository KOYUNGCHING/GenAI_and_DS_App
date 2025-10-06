# maps_reviews/src/processing/prepare_analysis.py
import argparse, os, re, pandas as pd, numpy as np

# ---- 類別規則：依名稱/Google types 決定主類別（由上而下比對，先中後外） ----
CATEGORY_RULES = [
    ("日式", ["japanese", "壽司", "鮨", "拉麵", "ramen", "丼", "居酒屋", "yakitori", "天婦羅", "天ぷら", "烏龍麵", "udon"]),
    ("韓式", ["korean", "韓式", "泡菜", "石鍋拌飯", "韓國烤肉", "bibimbap", "kimchi"]),
    ("中式", ["chinese", "中式", "台菜", "台式", "熱炒", "小吃", "麵館", "牛肉麵", "燒臘", "滷味"]),
    ("美式", ["american", "美式", "漢堡", "burger", "steakhouse", "炸雞", "fried chicken", "bbq"]),
    ("火鍋", ["hotpot", "hot pot", "火鍋", "鍋物", "涮涮鍋"]),
    ("咖啡廳", ["cafe", "coffee", "咖啡廳", "咖啡館"]),
    ("義式", ["italian", "義式", "披薩", "pizza", "pasta", "義大利麵", "trattoria"]),
    ("泰式", ["thai", "泰式", "泰國"]),
    ("早午餐", ["brunch", "breakfast", "早午餐", "早餐"]),
    ("甜點/烘焙", ["dessert", "甜點", "蛋糕", "bakery", "烘焙", "ice cream", "冰淇淋"]),
    ("海鮮", ["seafood", "海鮮"]),
    ("燒烤/串燒", ["barbecue", "燒烤", "串燒", "烤肉", "yakitori", "bbq"]),
    ("素食", ["vegetarian", "vegan", "素食", "蔬食"]),
]

def decide_category(name: str, types_csv: str) -> str:
    text = f"{name or ''} {(types_csv or '')}".lower()
    for label, kws in CATEGORY_RULES:
        for kw in kws:
            if kw.lower() in text:
                return label
    return "其他"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--places", required=True, help="data/processed/places_all.parquet")
    ap.add_argument("--reviews", required=False, default=None)  # 目前不使用評論也可
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # 讀資料
    df = pd.read_parquet(args.places).copy()

    # 基本欄位整理
    # 收集端存的是 'types'（逗號串），這裡統一成 types_csv；同時把評論數改名 popularity
    if "types" in df.columns and "types_csv" not in df.columns:
        df = df.rename(columns={"types":"types_csv"})
    df = df.rename(columns={"user_ratings_total":"popularity"})

    # 數值轉型
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")

    # 產生主類別（完全不使用 rep_price/price_band4）
    df["category_main"] = df.apply(
        lambda r: decide_category(r.get("name", ""), r.get("types_csv", "")), axis=1
    )

    # 如果保留了過去跑出的價位欄位，這裡一起丟掉，避免干擾
    drop_cols = [c for c in ["rep_price", "price_band4"] if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    #（可選）若你仍想合併評論，可在此加上 reviews 的彙整，略

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_parquet(args.out, index=False)
    print(f"Saved {args.out}, rows={len(df)}")
    print("類別分佈（前 10）：")
    print(df["category_main"].value_counts().head(10))

if __name__ == "__main__":
    main()