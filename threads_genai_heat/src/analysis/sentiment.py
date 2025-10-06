import argparse, pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from snownlp import SnowNLP
from langdetect import detect, LangDetectException
from src.utils.io import read_parquet, to_parquet
from tqdm import tqdm

def detect_lang_fast(text: str, fallback: str):
    try:
        return detect(text) if text else fallback
    except LangDetectException:
        return fallback

def en_sent(text: str, analyzer) -> float:
    vs = analyzer.polarity_scores(text or "")
    return float(vs["compound"])  # [-1,1]

def zh_sent(text: str) -> float:
    try:
        s = SnowNLP(text or "").sentiments  # [0,1]
        return float(s * 2 - 1)             # 映到 [-1,1]
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

    scores, labels = [], []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        text = (row.get("text") or "").strip()
        lang = (row.get("lang") or "").lower() or detect_lang_fast(text, "en")
        sc = zh_sent(text) if lang.startswith("zh") else en_sent(text, analyzer)
        scores.append(sc); labels.append(("pos" if sc>0.05 else "neg" if sc<-0.05 else "neu"))

    df["sent_score"] = scores
    df["sentiment"] = labels
    to_parquet(df, args.out)
    print(f"Saved: {args.out}, rows={len(df)}")

if __name__ == "__main__":
    main()
