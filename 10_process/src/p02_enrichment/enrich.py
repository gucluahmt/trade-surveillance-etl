# 10_process/src/p02_enrichment/enrich.py
from __future__ import annotations
from typing import Tuple
import pandas as pd

from core import config
from core.logger import get_logger
from core.utils import read_csv

log = get_logger("enrichment")

def _load_reference() -> Tuple[pd.DataFrame, pd.DataFrame]:
    pm = read_csv(config.PRODUCT_MASTER_CSV)   # expected: isin, cusip, symbol, instrument_type, liq_rule_key (or liquidity_bucket)
    cm = read_csv(config.CLIENT_MASTER_CSV)    # expected: customerID (or client_id), risk_tier, region

    # normalize headers (trim)
    pm.columns = [c.strip() for c in pm.columns]
    cm.columns = [c.strip() for c in cm.columns]

    # client master header normalization
    if "customerID" not in cm.columns:
        if "client_id" in cm.columns:
            cm = cm.rename(columns={"client_id": "customerID"})

    # product master: allow liquidity_bucket as alias for liq_rule_key
    if "liq_rule_key" not in pm.columns:
        if "liquidity_bucket" in pm.columns:
            pm = pm.rename(columns={"liquidity_bucket": "liq_rule_key"})
        else:
            pm["liq_rule_key"] = pd.NA

    # trim string values
    for df in (pm, cm):
        for c in df.columns:
            if pd.api.types.is_string_dtype(df[c]):
                df[c] = df[c].astype("string").str.strip()

    return pm, cm

def _enrich_product(df: pd.DataFrame, pm: pd.DataFrame) -> pd.DataFrame:
    out = df.merge(
        pm[["isin", "symbol", "instrument_type", "liq_rule_key"]],
        on="isin", how="left", suffixes=("", "_pm")
    )
    need_cusip = out["symbol"].isna() & out["cusip"].notna()
    if need_cusip.any():
        by_cusip = df.loc[need_cusip, ["cusip"]].merge(
            pm[["cusip", "symbol", "instrument_type", "liq_rule_key"]],
            on="cusip", how="left"
        )
        out.loc[need_cusip, ["symbol", "instrument_type", "liq_rule_key"]] = \
            by_cusip[["symbol", "instrument_type", "liq_rule_key"]].values

    out["instrument_type"] = out["instrument_type"].combine_first(df["instrument_type"])
    out["symbol"] = out["symbol"].combine_first(df["symbol"])
    return out

def _enrich_client(df: pd.DataFrame, cm: pd.DataFrame) -> pd.DataFrame:
    right_key = "customerID" if "customerID" in cm.columns else None
    if right_key is None:
        out = df.copy()
        out["risk_tier"] = pd.NA
        out["region"] = pd.NA
        return out

    out = df.merge(
        cm[[right_key] + [c for c in ["risk_tier", "region"] if c in cm.columns]],
        left_on="client_id", right_on=right_key, how="left"
    )
    if right_key in out.columns:
        out = out.drop(columns=[right_key])

    if "risk_tier" not in out.columns:
        out["risk_tier"] = pd.NA
    if "region" not in out.columns:
        out["region"] = pd.NA
    return out

def _derive_notional(df: pd.DataFrame) -> pd.DataFrame:
    df["notional"] = (df["quantity"].astype("float64") * df["price"].astype("float64"))
    return df

def _fill_liquidity(df: pd.DataFrame) -> pd.DataFrame:
    src = df["liquidity_bucket"]
    filled = src.copy()
    needs_fill = src.isna() & df["liq_rule_key"].notna()
    filled.loc[needs_fill] = df.loc[needs_fill, "liq_rule_key"]

    still_missing = filled.isna()
    if still_missing.any():
        tmp = df.loc[still_missing, ["instrument_type", "notional"]].copy()
        def rule(row):
            it, n = row["instrument_type"], row["notional"]
            if pd.isna(it) or pd.isna(n):
                return pd.NA
            if it == "BOND":
                if n >= 5_000_000: return "HIGH"
                if n >= 1_000_000: return "MED"
                return "LOW"
            elif it in ("SWAP", "FUTURE", "OPTION", "REPO"):
                if n >= 10_000_000: return "HIGH"
                if n >= 2_000_000:  return "MED"
                return "LOW"
            return pd.NA
        filled.loc[still_missing] = tmp.apply(rule, axis=1)

    df["liquidity_bucket"] = filled.astype("string")
    return df

def run_enrichment(df_ingested: pd.DataFrame) -> pd.DataFrame:
    log.info("Starting enrichment...")
    pm, cm = _load_reference()
    df = _enrich_product(df_ingested, pm)
    df = _enrich_client(df, cm)
    df = _derive_notional(df)
    df = _fill_liquidity(df)
    if "liq_rule_key" in df.columns:
        df = df.drop(columns=["liq_rule_key"])
    log.info("Enrichment completed.")
    return df

__all__ = ["run_enrichment"]
