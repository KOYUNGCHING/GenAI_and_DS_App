import re
import unicodedata
from pathlib import Path

def ensure_dirs(*paths):
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)

def normalize_tool_name(s: str) -> str:
    s = unicodedata.normalize("NFKC", s).strip().lower()
    s = re.sub(r"[\s\-\_\.\!]+", " ", s)
    return s
