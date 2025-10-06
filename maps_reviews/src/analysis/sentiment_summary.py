import argparse, pandas as pd, numpy as np, os
from snownlp import SnowNLP

def sn_sent(t):
    if not isinstance(t,str) or not t.strip(): return np.nan
    try: return SnowNLP(t).sentiments
    except Exception: return np.nan

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    args = ap.parse_args()

    df = pd.read_parquet(args.inp)
    if "rev_text" not in df.columns:
        pd.DataFrame(columns=["city","area_name","sent_mean","sent_pos_share","n"]).to_csv(args.out, index=False, encoding="utf-8-sig")
        print("No review texts found. Saved empty summary.")
        return

    sub = df[["city","area_name","rev_text"]].dropna(subset=["rev_text"]).copy()
    sub["sent"] = sub["rev_text"].apply(sn_sent)
    sub["is_pos"] = sub["sent"].apply(lambda x: 1 if (isinstance(x,float) and x>=0.6) else 0)

    out = (sub.groupby(["city","area_name"])
             .agg(sent_mean=("sent","mean"),
                  sent_pos_share=("is_pos","mean"),
                  n=("rev_text","count"))
             .reset_index())
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print("Saved:", args.out)

if __name__ == "__main__":
    main()
