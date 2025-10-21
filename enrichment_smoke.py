import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from core import config            # <-- import config BEFORE using it
import pandas as pd

# Diagnose client_master headers
print("client_master columns:",
      pd.read_csv(config.CLIENT_MASTER_CSV, nrows=0).columns.tolist())

# Continue with normal imports/runs
from p01_ingestion.extract import run_ingestion
from p02_enrichment.enrich import run_enrichment

import pandas as pd; print("client_master columns:", pd.read_csv(config.CLIENT_MASTER_CSV, nrows=0).columns.tolist())
from p01_ingestion.extract import run_ingestion
from p02_enrichment.enrich import run_enrichment
from core import config

df_in = run_ingestion(config.RAW_TRADES_CSV)
df_en = run_enrichment(df_in)

print("rows:", len(df_en))
print("columns:", df_en.columns.tolist()[:12], "...")
print(df_en[["trade_id","instrument_type","symbol","liquidity_bucket","risk_tier","region","notional"]].head(5).to_string(index=False))

# simple sanity checks
print("\nNulls after enrichment (key fields):")
for col in ["instrument_type", "liquidity_bucket", "risk_tier", "region", "notional"]:
    print(col, "→", int(df_en[col].isna().sum()))
