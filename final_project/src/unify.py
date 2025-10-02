import pandas as pd
from .utils import normalize_tool_name
from .categorize import classify

def build_tool_list_from_producthunt(ph_df: pd.DataFrame, top_k=300) -> list:
    ph = ph_df.sort_values("ph_votes", ascending=False).head(top_k)
    return ph["tool_name"].dropna().unique().tolist()

def unify(ph_df: pd.DataFrame, rd_df: pd.DataFrame, gh_df: pd.DataFrame) -> pd.DataFrame:
    ph_df = ph_df.copy()
    ph_df["tool_key"] = ph_df["tool_name"].map(normalize_tool_name)

    if rd_df is not None and not rd_df.empty:
        rd_df = rd_df.copy()
        rd_df["tool_key"] = rd_df["tool_name"].map(normalize_tool_name)
        rd_agg = rd_df.groupby("tool_key").agg(
            reddit_posts=("id", "nunique"),
            reddit_comments=("num_comments", "sum"),
            reddit_score=("score", "sum"),
            reddit_first_seen=("created_utc", "min"),
        ).reset_index()
    else:
        rd_agg = pd.DataFrame(columns=["tool_key","reddit_posts","reddit_comments","reddit_score","reddit_first_seen"])

    if gh_df is not None and not gh_df.empty:
        gh_df = gh_df.copy()
        gh_df["repo_name"] = gh_df["repo"].str.split("/").str[-1].fillna("")
        gh_df["tool_key"] = gh_df["repo_name"].map(normalize_tool_name)
        gh_agg = gh_df.groupby("tool_key").agg(
            gh_stars=("gh_stars","sum"),
            gh_forks=("gh_forks","sum"),
            gh_repo_count=("repo","nunique"),
            gh_first_repo=("created_at","min"),
            gh_last_update=("updated_at","max"),
            gh_langs=("primary_lang", lambda s: "|".join(pd.Series(s).dropna().unique()[:5]))
        ).reset_index()
    else:
        gh_agg = pd.DataFrame(columns=["tool_key","gh_stars","gh_forks","gh_repo_count","gh_first_repo","gh_last_update","gh_langs"])

    base = ph_df.copy()
    base["category"] = [classify(n, d, t) for n,d,t in zip(base["tool_name"], base.get("description",""), base.get("topics",""))]

    out = (base.merge(rd_agg, on="tool_key", how="left")
                .merge(gh_agg, on="tool_key", how="left"))

    for c in ["reddit_posts","reddit_comments","reddit_score","gh_stars","gh_forks","gh_repo_count"]:
        if c in out.columns:
            out[c] = out[c].fillna(0).astype(int)

    for c in ["created_at","reddit_first_seen","gh_first_repo","gh_last_update"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce", utc=True)

    keep = [
        "tool_name","tool_key","category","url",
        "created_at","ph_votes","ph_comments",
        "reddit_posts","reddit_comments","reddit_score",
        "gh_stars","gh_forks","gh_repo_count","gh_langs"
    ]
    keep = [k for k in keep if k in out.columns]
    return out[keep].drop_duplicates(subset=["tool_key"])
