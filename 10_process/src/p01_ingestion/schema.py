# 10_process/src/p01_ingestion/schema.py
"""
Defines the canonical trade schema used after ingestion.
All mappings and validation layers reference these definitions.
"""

from typing import Dict, List, Set

# Canonical (target) schema — every inbound trade will be mapped to this structure
CANONICAL_ORDER: List[str] = [
    "trade_id", "order_id", "client_id",
    "isin", "cusip", "symbol",
    "side", "quantity", "price",
    "trade_date", "trade_ts",
    "currency", "venue", "instrument_type",
    "liquidity_bucket", "desk", "source_system",
    "risk_tier", "region", "notional"
]

# Expected datatypes (pandas-compatible)
DTYPES: Dict[str, str] = {
    "trade_id": "string",
    "order_id": "string",
    "client_id": "string",
    "isin": "string",
    "cusip": "string",
    "symbol": "string",
    "side": "string",             # BUY / SELL
    "quantity": "Int64",          # nullable integer
    "price": "float64",
    "trade_date": "string",       # YYYY-MM-DD
    "trade_ts": "string",         # ISO 8601 UTC timestamp
    "currency": "string",
    "venue": "string",
    "instrument_type": "string",  # BOND / SWAP / FUTURE / OPTION / REPO
    "liquidity_bucket": "string", # HIGH / MED / LOW
    "desk": "string",
    "source_system": "string",
    "risk_tier": "string",
    "region": "string",
    "notional": "float64"
}

# Mandatory fields — required for any trade to be valid
MANDATORY: Set[str] = {
    "trade_id", "order_id", "client_id",
    "side", "quantity", "price", "trade_ts", "instrument_type"
}

# Enumerated domains for categorical validation
ENUM_SETS = {
    "side": {"BUY", "SELL"},
    "instrument_type": {"BOND", "SWAP", "FUTURE", "OPTION", "REPO"},
    "currency": {"USD", "EUR", "GBP", "JPY"},
    "liquidity_bucket": {"HIGH", "MED", "LOW"}
}

# Basic normalization map for 'side' field (handles typos or spacing)
SIDE_NORMALIZATION = {
    "BUY": "BUY",
    "BUY ": "BUY",
    "SEL": "SELL",
    "SELL": "SELL"
}
