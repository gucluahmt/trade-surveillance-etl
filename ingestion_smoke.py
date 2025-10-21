# --- quick path patch (safe to keep) ---
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "10_process", "src"))
# --------------------------------------
from p01_ingestion.extract import run_ingestion
from core import config
import pandas as pd

df = run_ingestion(config.RAW_TRADES_CSV)
print("rows:", len(df))
print("first 10 columns:", df.columns.tolist()[:10])
print(df.head(3).to_string(index=False))

