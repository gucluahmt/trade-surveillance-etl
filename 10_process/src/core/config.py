from pathlib import Path

# project root: .../trade-surveillance-etl/
ROOT = Path(__file__).resolve().parents[3]

# INPUT
INPUT_DIR = ROOT / "00_input"
INBOUND_DIR = INPUT_DIR / "inbound"
REFERENCE_DIR = INPUT_DIR / "reference"

RAW_TRADES_CSV = INBOUND_DIR / "trades_raw.csv"
PRODUCT_MASTER_CSV = REFERENCE_DIR / "product_master.csv"
CLIENT_MASTER_CSV = REFERENCE_DIR / "client_master.csv"
MAPPING_CSV = REFERENCE_DIR / "mapping_source_to_target.csv"

# OUTCOME
OUTCOME_DIR = ROOT / "20_outcome"
CURATED_DIR = OUTCOME_DIR / "curated"
EXCEPTIONS_DIR = OUTCOME_DIR / "exceptions"
METRICS_DIR = OUTCOME_DIR / "metrics"
REPORTS_DIR = OUTCOME_DIR / "reports"

CURATED_TRADES_CSV = CURATED_DIR / "trades_curated.csv"
EXCEPTIONS_JSONL = EXCEPTIONS_DIR / "exceptions.jsonl"
RUN_METRICS_JSON = METRICS_DIR / "etl_run_metrics.json"
DQ_SUMMARY_MD = REPORTS_DIR / "dq_summary.md"
DQ_DASHBOARD_CSV = REPORTS_DIR / "dq_dashboard.csv"

# General
TIMEZONE = "UTC"  # demo purposes
