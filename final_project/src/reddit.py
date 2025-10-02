import time, pandas as pd
import praw
from typing import List
from .config import REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT

SUBREDDITS = ["ArtificialIntelligence", "MachineLearning", "ChatGPT", "LocalLLaMA", "DataScience"]

def reddit_client():
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_USER_AGENT):
        raise RuntimeError("Missing Reddit credentials in .env")
    return praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )

def search_mentions(tool_names: List[str], limit_per_tool=300, time_filter="year") -> pd.DataFrame:
    reddit = reddit_client()
    rows = []
    for tool in tool_names:
        q = f'"{tool}"'
        for sub in SUBREDDITS:
            try:
                for post in reddit.subreddit(sub).search(q, sort="relevance", time_filter=time_filter, limit=limit_per_tool):
                    rows.append({
                        "source": "reddit",
                        "tool_name": tool,
                        "subreddit": sub,
                        "created_utc": post.created_utc,
                        "score": post.score,
                        "num_comments": post.num_comments,
                        "title": post.title,
                        "id": post.id,
                        "permalink": f"https://reddit.com{post.permalink}"
                    })
                time.sleep(0.3)
            except Exception as e:
                print("Error", tool, sub, e)
                time.sleep(1.0)
    return pd.DataFrame(rows)
