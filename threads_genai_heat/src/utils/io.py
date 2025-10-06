import os, pandas as pd, pyarrow as pa, pyarrow.parquet as pq

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def to_parquet(df: pd.DataFrame, path: str):
    ensure_dir(os.path.dirname(path))
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)

def read_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)
