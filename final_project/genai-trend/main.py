from src.config import DATA_DIR, FIG_DIR
from src.utils import ensure_dirs
from src.producthunt import fetch_all_posts
from src.reddit import search_mentions
from src.github_api import gh_for_tools
from src.unify import build_tool_list_from_producthunt, unify
from src.analyze import add_log_growth_features, kmeans_cluster, plot_scatter

def main():
    ensure_dirs(DATA_DIR, FIG_DIR, "report")

    # A. Product Hunt
    ph = fetch_all_posts(posted_after="2022-01-01T00:00:00Z")
    ph.to_csv(f"{DATA_DIR}/producthunt_raw.csv", index=False)

    # B. Prepare tool list (top 300 by votes)
    tools = build_tool_list_from_producthunt(ph, top_k=300)

    # C. Reddit
    rd = search_mentions(tools, limit_per_tool=200, time_filter="year")
    rd.to_csv(f"{DATA_DIR}/reddit_raw.csv", index=False)

    # D. GitHub
    gh = gh_for_tools(tools)
    gh.to_csv(f"{DATA_DIR}/github_raw.csv", index=False)

    # E. Merge
    master = unify(ph, rd, gh)
    master = add_log_growth_features(master)
    master.to_csv(f"{DATA_DIR}/tools_master.csv", index=False)

    # F. Clustering + plot
    clustered, _ = kmeans_cluster(master, k=4)
    clustered.to_csv(f"{DATA_DIR}/tools_master_clustered.csv", index=False)
    plot_scatter(clustered, path=f"{FIG_DIR}/cluster_scatter.png")

    print("Done. -> data/tools_master.csv & figs/cluster_scatter.png")

if __name__ == "__main__":
    main()
