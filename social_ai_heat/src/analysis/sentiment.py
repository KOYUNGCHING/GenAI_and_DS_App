
import argparse, pandas as pd, numpy as np
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from snownlp import SnowNLP
from langdetect import detect, LangDetectException
from src.utils.io import read_parquet, to_parquet
from tqdm import tqdm

def detect_lang_fast(text: str, fallback: str):
    # 若已提供 lang 就信任，否則嘗試偵測
    try:
        return detect(text) if text else fallback
    except LangDetectException:
        return fallback

def en_sent(text: str, analyzer) -> float:
    vs = analyzer.polarity_scores(text or "")
    return float(vs["compound"])  # [-1,1]

def zh_sent(text: str) -> float:
    # SnowNLP: [0,1]，線性映到 [-1,1]
    try:
        s = SnowNLP(text or "").sentiments
        return float(s * 2 - 1)
    except Exception:
        return 0.0

def to_label(score: float) -> str:
    if score >  0.05: return "pos"
    if score < -0.05: return "neg"
    return "neu"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    args = ap.parse_args()

    df = read_parquet(args.inp)
    analyzer = SentimentIntensityAnalyzer()

    scores = []
    labels = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        text = row.get("text","") or ""
        lang = (row.get("lang") or "").lower()
        if not lang or lang == "unknown":
            lang = detect_lang_fast(text, "en")

        if lang.startswith("zh"):
            sc = zh_sent(text)
        else:
            sc = en_sent(text, analyzer)

        scores.append(sc)
        labels.append(to_label(sc))

    df["sent_score"] = scores
    df["sentiment"] = labels

    to_parquet(df, args.out)
    print(f"Saved: {args.out}, rows={len(df)}")

if __name__ == "__main__":
    main()
