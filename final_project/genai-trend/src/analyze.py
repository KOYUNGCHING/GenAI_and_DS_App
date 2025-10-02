import pandas as pd, numpy as np, matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

def add_log_growth_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ph_votes_log1p"] = np.log1p(df["ph_votes"])
    df["reddit_posts_log1p"] = np.log1p(df["reddit_posts"])
    df["gh_stars_log1p"] = np.log1p(df["gh_stars"])
    df["heat_index"] = df["ph_votes_log1p"]*0.4 + df["reddit_posts_log1p"]*0.3 + df["gh_stars_log1p"]*0.3
    return df

def kmeans_cluster(df: pd.DataFrame, k=4):
    feats = df[["ph_votes_log1p","reddit_posts_log1p","gh_stars_log1p"]].values
    X = StandardScaler().fit_transform(feats)
    km = KMeans(n_clusters=k, n_init=20, random_state=42)
    labels = km.fit_predict(X)
    df = df.copy()
    df["cluster"] = labels
    return df, km

def plot_scatter(df: pd.DataFrame, path="figs/cluster_scatter.png"):
    plt.figure(figsize=(7,6))
    plt.scatter(df["ph_votes_log1p"], df["gh_stars_log1p"], s=30, alpha=0.7)
    plt.xlabel("log(1+PH votes)")
    plt.ylabel("log(1+GH stars)")
    for _, r in df.nlargest(20, "heat_index").iterrows():
        plt.text(r["ph_votes_log1p"], r["gh_stars_log1p"], r["tool_name"][:12], fontsize=8)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
