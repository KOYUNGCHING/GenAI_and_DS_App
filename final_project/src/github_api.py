import requests, time, pandas as pd
from .config import GITHUB_TOKEN

GQL = "https://api.github.com/graphql"

SEARCH_QUERY = """
query($q:String!, $cursor:String) {
  search(query:$q, type: REPOSITORY, first:50, after:$cursor) {
    repositoryCount
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        ... on Repository {
          nameWithOwner
          url
          stargazerCount
          forkCount
          createdAt
          updatedAt
          primaryLanguage { name }
          repositoryTopics(first: 10){ nodes { topic { name } } }
          description
        }
      }
    }
  }
}
"""

def gh_search(q: str, max_pages=10) -> pd.DataFrame:
    if not GITHUB_TOKEN:
        raise RuntimeError("Missing GITHUB_TOKEN in .env")
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    cursor = None
    rows = []
    for _ in range(max_pages):
        variables = {"q": q, "cursor": cursor}
        r = requests.post(GQL, json={"query": SEARCH_QUERY, "variables": variables}, headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()["data"]["search"]
        for e in data["edges"]:
            n = e["node"]
            topics = [t["topic"]["name"] for t in n["repositoryTopics"]["nodes"]]
            rows.append({
                "source": "github",
                "repo": n["nameWithOwner"],
                "url": n["url"],
                "gh_stars": n["stargazerCount"],
                "gh_forks": n["forkCount"],
                "created_at": n["createdAt"],
                "updated_at": n["updatedAt"],
                "primary_lang": (n["primaryLanguage"] or {}).get("name") if n["primaryLanguage"] else None,
                "topics": "|".join(topics),
                "description": n["description"] or "",
            })
        if not data["pageInfo"]["hasNextPage"]:
            break
        cursor = data["pageInfo"]["endCursor"]
        time.sleep(0.4)
    return pd.DataFrame(rows)

def gh_for_tools(tool_names):
    all_df = []
    for t in tool_names:
        q = f'{t} in:name,description stars:>50'
        all_df.append(gh_search(q, max_pages=5))
        time.sleep(0.3)
    import pandas as pd
    return pd.concat(all_df, ignore_index=True) if all_df else pd.DataFrame()
