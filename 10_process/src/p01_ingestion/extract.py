# 10_process/src/p01_ingestion/extract.py
"""
Handles data ingestion and normalization:
- Reads inbound CSV files.
- Applies field-level mapping (source → canonical).
- Normalizes basic string, numeric, and date fields.
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict
import pandas as pd

from core import config
from core.logger import get_logger
from core.utils import read_csv
from p01_ingestion.schema import (
    CANONICAL_ORDER, DTYPES, SIDE_NORMALIZATION
)

log = get_logger("ingestion")

def load_mapping(path: Path) -> Dict[str, str]:
    """
    Load field-level mapping from the source_to_target CSV.
    Expected columns: source_field, target_field
    """
    mdf = read_csv(path)
    if not {"source_field", "target_field"}.issubset(mdf.columns):
        raise ValueError("Mapping file must include 'source_field' and 'target_field' columns.")
    return dict(zip(mdf["source_field"], mdf["target_field"]))

def apply_mapping(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """Rename source columns to canonical target names."""
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns=mapping)
    return df

def normalize_values(df: pd.DataFrame) -> pd.DataFrame:
    """Basic normalization: trim strings, unify enums, parse timestamps and numerics."""
    df = df.copy()

    # Trim all string columns
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype("string").str.strip()

    # Normalize 'side' values
    if "side" in df.columns:
        df["side"] = df["side"].map(lambda x: SIDE_NORMALIZATION.get(x, x)).astype("string")

    # Parse trade_date (convert to YYYY-MM-DD string)
    if "trade_date" in df.columns:
        parsed = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
        df["trade_date"] = parsed.astype("string")

    # Parse trade_ts (convert to ISO-8601 UTC)
    if "trade_ts" in df.columns:
        parsed = pd.to_datetime(df["trade_ts"], errors="coerce", utc=True)
        df["trade_ts"] = parsed.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Coerce numeric fields
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce").astype("float64")

    return df

def coerce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure all canonical fields exist and align datatypes.
    Adds missing columns with NaN and orders fields as defined in schema.
    """
    df = df.copy()

    # Add missing canonical columns
    for col in CANONICAL_ORDER:
        if col not in df.columns:
            df[col] = pd.NA

    # Reorder
    df = df[CANONICAL_ORDER]

    # Enforce dtype consistency
    for col, dtype in DTYPES.items():
        try:
            df[col] = df[col].astype(dtype)
        except Exception:
            log.warning(f"Type coercion failed for {col} ({dtype})")

    return df

def run_ingestion(raw_path: Path = config.RAW_TRADES_CSV) -> pd.DataFrame:
    """
    End-to-end ingestion:
    - Load raw data
    - Apply mapping
    - Normalize
    - Coerce schema
    """
    log.info("Starting ingestion process...")
    mapping = load_mapping(config.MAPPING_CSV)
    raw_df = read_csv(raw_path)
    log.info(f"Loaded raw trades: {len(raw_df)} rows")

    mapped_df = apply_mapping(raw_df, mapping)
    normalized_df = normalize_values(mapped_df)
    final_df = coerce_schema(normalized_df)

    log.info(f"Ingestion completed: {len(final_df)} records aligned to canonical schema.")
    return final_df
