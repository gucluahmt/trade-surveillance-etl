# src/p03_validation/rules.py
"""
Validation rules for trade-surveillance ETL.
Each rule returns a DataFrame of breaches with:
- rule_id
- severity
- reason
- row_index  (original index in the input df)
- keys       (dict-like string: trade_id/order_id etc.)

Rules are simple & fast; they are easy to extend.
"""

from __future__ import annotations
from typing import List, Dict
import pandas as pd
import re

# Enumerations expected (aligned with schema.py)
ENUM_SIDE = {"BUY", "SELL"}
ENUM_INSTRUMENT = {"BOND", "SWAP", "FUTURE", "OPTION", "REPO"}
ENUM_CCY = {"USD", "EUR", "GBP", "JPY"}

def _breaches_df(rule_id: str, severity: str, reason: str, idx: pd.Index, df: pd.DataFrame) -> pd.DataFrame:
    if len(idx) == 0:
        return pd.DataFrame(columns=["rule_id","severity","reason","row_index","keys"])
    keys = []
    for i in idx:
        # try to carry helpful identifiers
        t = str(df.at[i, "trade_id"]) if "trade_id" in df.columns else "NA"
        o = str(df.at[i, "order_id"]) if "order_id" in df.columns else "NA"
        keys.append(f"trade_id={t}|order_id={o}")
    return pd.DataFrame({
        "rule_id": rule_id,
        "severity": severity,
        "reason": reason,
        "row_index": list(idx),
        "keys": keys
    })

def rule_mandatory(df: pd.DataFrame) -> pd.DataFrame:
    required = ["trade_id","order_id","client_id","side","quantity","price","trade_ts","instrument_type"]
    missing_idx = pd.Index([])
    for col in required:
        if col not in df.columns:
            # if a required column doesn't exist, everything fails
            missing_idx = df.index
            break
        missing_idx = missing_idx.union(df[df[col].isna()].index)
    return _breaches_df(
        "R001_MANDATORY", "CRITICAL",
        "Mandatory fields must not be null",
        missing_idx, df
    )

def rule_domain_enums(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.Index([])
    if "side" in df.columns:
        idx = idx.union(df[~df["side"].isin(ENUM_SIDE) & df["side"].notna()].index)
    if "instrument_type" in df.columns:
        idx = idx.union(df[~df["instrument_type"].isin(ENUM_INSTRUMENT) & df["instrument_type"].notna()].index)
    if "currency" in df.columns:
        idx = idx.union(df[~df["currency"].isin(ENUM_CCY) & df["currency"].notna()].index)
    return _breaches_df(
        "R002_ENUMS", "HIGH",
        "Value not in allowed enumerations",
        idx, df
    )

def rule_numeric_positive(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.Index([])
    if "quantity" in df.columns:
        idx = idx.union(df[(df["quantity"].notna()) & (df["quantity"] <= 0)].index)
    if "price" in df.columns:
        idx = idx.union(df[(df["price"].notna()) & (df["price"] <= 0)].index)
    return _breaches_df(
        "R003_POSITIVE", "HIGH",
        "Quantity and Price must be > 0",
        idx, df
    )

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")   # checksum not fully enforced here (demo)
_CUSIP_RE = re.compile(r"^[A-Z0-9]{9}$")

def rule_identifier_format(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.Index([])
    if "isin" in df.columns:
        idx = idx.union(df[(df["isin"].notna()) & (~df["isin"].str.fullmatch(_ISIN_RE))].index)
    if "cusip" in df.columns:
        idx = idx.union(df[(df["cusip"].notna()) & (~df["cusip"].str.fullmatch(_CUSIP_RE))].index)
    return _breaches_df(
        "R004_ID_FORMAT", "MEDIUM",
        "Invalid ISIN/CUSIP format",
        idx, df
    )

def rule_timestamp_sanity(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.Index([])
    if "trade_date" in df.columns and "trade_ts" in df.columns:
        tdate = pd.to_datetime(df["trade_date"], errors="coerce", utc=False)
        tts = pd.to_datetime(df["trade_ts"], errors="coerce", utc=True)
        bad_order = df[(tdate.notna()) & (tts.notna()) & (tts.dt.date < tdate)].index
        idx = idx.union(bad_order)
    return _breaches_df(
        "R005_TS_SANITY", "MEDIUM",
        "trade_ts must be on/after trade_date",
        idx, df
    )

def rule_notional_consistency(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.Index([])
    if {"quantity","price","notional"}.issubset(df.columns):
        calc = (df["quantity"].astype("float64") * df["price"].astype("float64"))
        diff = (df["notional"].astype("float64") - calc).abs()
        idx = df[(df["quantity"].notna()) & (df["price"].notna()) & (df["notional"].notna()) & (diff > 0.01)].index
    return _breaches_df(
        "R006_NOTIONAL", "LOW",
        "Notional differs from quantity*price (tolerance 0.01)",
        idx, df
    )

def rule_duplicate_keys(df: pd.DataFrame) -> pd.DataFrame:
    idx = pd.Index([])
    if "trade_id" in df.columns:
        d1 = df[df.duplicated("trade_id", keep=False)].index
        idx = idx.union(d1)
    # soft collision: order_id + trade_ts + quantity + price
    cols = ["order_id","trade_ts","quantity","price"]
    if all(c in df.columns for c in cols):
        d2 = df[df.duplicated(cols, keep=False)].index
        idx = idx.union(d2)
    return _breaches_df(
        "R007_DUPLICATES", "CRITICAL",
        "Duplicate trade_id or (order_id,trade_ts,quantity,price) collision",
        idx, df
    )

def run_all_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Return a concatenated DataFrame of all breaches."""
    rules = [
        rule_mandatory,
        rule_domain_enums,
        rule_numeric_positive,
        rule_identifier_format,
        rule_timestamp_sanity,
        rule_notional_consistency,
        rule_duplicate_keys,
    ]
    out = [r(df) for r in rules]
    out = [o for o in out if len(o) > 0]
    return pd.concat(out, ignore_index=True) if out else _breaches_df("NA","NA","NA", pd.Index([]), df)
