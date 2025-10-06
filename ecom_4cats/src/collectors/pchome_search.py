# PChome 搜尋 JSON 收集器
import argparse, time, random, requests, pandas as pd
from datetime import datetime

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
BASES = ["https://ecapi-pchome.cdn.hinet.net/apis/v3.3/search/all/results","https://ecshweb.pchome.com.tw/search/v3.3/all/results"]

def fetch_page(base, keyword, page):
    params={"q":keyword,"page":page}
    headers={"User-Agent":UA,"Accept":"application/json"}
    r = requests.get(base, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    return r.json()

def parse_items(js):
    prods = js.get("prods") or js.get("Prods") or []
    for p in prods:
        title = p.get("name") or p.get("Name")
        price = p.get("price") or p.get("Price")
        review_count = p.get("reviewCount") or p.get("review") or p.get("rtn") or None
        rating = p.get("rating") or p.get("Rating") or None
        pid = p.get("Id") or p.get("Idv") or p.get("id") or ""
        url = f"https://24h.pchome.com.tw/prod/{pid}" if pid else None
        yield {"platform":"pchome","title":title,"price":price,"sales":None,"rating":rating,"review_count":review_count,"url":url}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", required=True)
    ap.add_argument("--query", required=True)
    ap.add_argument("--pages", type=int, default=2)
    ap.add_argument("--outdir", default="data/raw/pchome")
    args = ap.parse_args()

    rows=[]
    for kw in args.query.split():
        for p in range(1, args.pages+1):
            js=None
            for base in BASES:
                try:
                    js=fetch_page(base, kw, p); break
                except Exception: continue
            if not js: 
                print("skip", kw, p); 
                continue
            for r in parse_items(js):
                r.update({"category":args.category,"query":kw,"scraped_at":datetime.utcnow().isoformat()})
                rows.append(r)
            time.sleep(1.2 + random.random()*0.8)

    if not rows:
        print("No data scraped."); return
    import os; os.makedirs(args.outdir, exist_ok=True)
    out=f"{args.outdir}/pchome_{args.category}.parquet"
    pd.DataFrame(rows).to_parquet(out, index=False)
    print(f"Saved {out}, rows={len(rows)}")

if __name__=="__main__":
    main()
