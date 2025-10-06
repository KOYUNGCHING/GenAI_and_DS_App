import argparse, pandas as pd, networkx as nx, itertools

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)  # .gexf
    args = ap.parse_args()

    df = pd.read_parquet(args.inp)
    tags_series = df["hashtags"].fillna("").map(lambda s: [t.strip() for t in s.split(",") if t.strip()])
    G = nx.Graph()
    for tags in tags_series:
        uniq = sorted(set(tags))
        for a,b in itertools.combinations(uniq, 2):
            G.add_edge(a,b, weight=G.get_edge_data(a,b,{}).get("weight",0)+1)
    nx.write_gexf(G, args.out)
    print(f"Saved graph: {args.out}, nodes={G.number_of_nodes()}, edges={G.number_of_edges()}")

if __name__ == "__main__":
    main()
