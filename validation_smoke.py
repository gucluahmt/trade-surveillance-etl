# validation_smoke.py (project root)
# Ensure our src/ is on sys.path for imports
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from core import config
from p01_ingestion.extract import run_ingestion
from p02_enrichment.enrich import run_enrichment
from p03_validation.validator import run_validation

df_in = run_ingestion(config.RAW_TRADES_CSV)
df_en = run_enrichment(df_in)
result = run_validation(df_en)

print("\n=== Validation Summary ===")
print(result["metrics"])
print("Curated file   :", result["curated_path"])
print("Exceptions file:", result["exceptions_path"])
print("Metrics file   :", result["metrics_path"])
