from pathlib import Path
import json
import pandas as pd

def ensure_dirs(*paths: Path) -> None:
    for p in paths:
        Path(p).mkdir(parents=True, exist_ok=True)

def read_csv(path: Path, **kwargs) -> pd.DataFrame:
    if not Path(path).exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    return pd.read_csv(path, **kwargs)

def write_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_dirs(Path(path).parent)
    df.to_csv(path, index=False)

def write_json(obj, path: Path) -> None:
    ensure_dirs(Path(path).parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def write_jsonl(df: pd.DataFrame, path: Path) -> None:
    """Append-safe writer; empty DataFrame creates"""
    ensure_dirs(Path(path).parent)
    if df is None or df.empty:
        Path(path).write_text("")
    else:
        df.to_json(path, orient="records", lines=True)
