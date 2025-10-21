# src/p03_validation/validator.py
"""
Validation engine that:
- runs rule set
- splits data into PASSED vs FAILED
- writes outputs to 20_outcome/{curated,exceptions,metrics}
"""

from __future__ import annotations
from datetime import datetime
from pathlib import Path
import json
import pandas as pd

from core import config
from core.logger import get_logger
from core.utils import write_csv, write_jsonl
from p03_validation.rules import run_all_rules

log = get_logger("validation")

def _timestamp() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def run_validation(df_enriched: pd.DataFrame) -> dict:
    log.info("Running validation...")
    breaches = run_all_rules(df_enriched)

    # Build failed index set
    failed_idx = set(breaches["row_index"].tolist()) if len(breaches) else set()
    passed_mask = ~df_enriched.index.isin(failed_idx)

    df_passed = df_enriched.loc[passed_mask].copy()
    df_failed = df_enriched.loc[~passed_mask].copy()

    # Write outputs
    ts = _timestamp()

    curated_path = Path(f"20_outcome/curated/enriched_validated_trades_{ts}.csv")
    write_csv(df_passed, curated_path)

    exceptions_path = Path(f"20_outcome/exceptions/validation_breaches_{ts}.jsonl")
    write_jsonl(breaches, exceptions_path)

    # Simple metrics
    metrics = {
        "run_ts_utc": ts,
        "input_rows": int(len(df_enriched)),
        "passed_rows": int(len(df_passed)),
        "failed_rows": int(len(df_failed)),
        "breach_count": int(len(breaches)),
        "pass_rate_pct": round(100.0 * len(df_passed) / max(1, len(df_enriched)), 2)
    }
    metrics_path = Path(f"20_outcome/metrics/validation_metrics_{ts}.json")
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(metrics, indent=2))

    log.info(f"Validation complete. Passed={len(df_passed)} Failed={len(df_failed)} Breaches={len(breaches)}")

    return {
        "curated_path": str(curated_path),
        "exceptions_path": str(exceptions_path),
        "metrics_path": str(metrics_path),
        "metrics": metrics
    }
