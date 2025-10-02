import requests, pandas as pd, time
from .config import PRODUCTHUNT_TOKEN

GQL_ENDPOINT = "https://api.producthunt.com/v2/api/graphql"

QUERY = """
query($after: String, $postedAfter: DateTime) {
  posts(order: RANKING, postedAfter: $postedAfter, after: $after, first: 50) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        name
        slug
        createdAt
        votesCount
        commentsCount
        tagline
        url
        topics(first: 10) { edges { node { name slug } } }
      }
    }
  }
}
"""

def fetch_all_posts(posted_after="2022-01-01T00:00:00Z") -> pd.DataFrame:
    if not PRODUCTHUNT_TOKEN:
        raise RuntimeError("Missing PRODUCTHUNT_TOKEN in .env")
    headers = {"Authorization": f"Bearer {PRODUCTHUNT_TOKEN}"}
    after = None
    rows = []
    while True:
        variables = {"after": after, "postedAfter": posted_after}
        r = requests.post(GQL_ENDPOINT, json={"query": QUERY, "variables": variables}, headers=headers, timeout=30)
        r.raise_for_status()
        payload = r.json()
        data = payload["data"]["posts"]
        for e in data["edges"]:
            n = e["node"]
            topics = [t["node"]["name"] for t in n["topics"]["edges"]]
            rows.append({
                "source": "producthunt",
                "tool_name": n["name"],
                "slug": n["slug"],
                "created_at": n["createdAt"],
                "ph_votes": n["votesCount"],
                "ph_comments": n["commentsCount"],
                "description": n["tagline"],
                "topics": "|".join(topics),
                "url": n["url"],
            })
        if not data["pageInfo"]["hasNextPage"]:
            break
        after = data["pageInfo"]["endCursor"]
        time.sleep(0.4)
    return pd.DataFrame(rows)
