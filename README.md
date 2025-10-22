# 🧩 Trade Data ETL Process with Validation Rules 


---

## 🧭 Project Overview

This project demonstrates a realistic **ETL (Extract, Transform, Load)** pipeline for trade datasets.  
It simulates how raw trading data (from an OMS feed or CSV source) is:

1. **Extracted** — read from raw input files.  
2. **Transformed** — normalized into a canonical schema.  
3. **Enriched** — combined with reference data (product & client master).  
4. **Validated** — checked against field-level, schema-level, and business rules.  
5. **Loaded** — saved into curated folders for analytics or reporting use.

The pipeline ensures that all data passing into the next stage is clean, accurate, and aligned with defined data standards.

---

## 🧾 Business Impact

| Impact Area | Description |
|--------------|-------------|
| **Data Reliability** | Guarantees that downstream systems (analytics, reporting, or compliance) receive only validated and consistent trade data. |
| **Operational Efficiency** | Reduces manual data reviews by detecting and isolating data issues early in the ETL pipeline. |
| **Auditability** | Each run is fully traceable — timestamps, validation rules, and output files are versioned for audit readiness. |
| **Decision Confidence** | Business stakeholders can make faster, data-driven decisions with confidence in the accuracy of underlying data. |
| **Cost Optimization** | Preventing data quality issues early lowers reprocessing and reconciliation costs. |

> **Clean data = Confident decisions.**  
> This validation framework ensures that every record entering the business ecosystem is accurate, complete, and compliant with defined rules.



---


### Tech Stack
- **Language:** Python 3.11  
- **Libraries:** pandas, logging, pathlib, json  
- **Structure:** Modular ETL with separate ingestion, enrichment, and validation layers  

---

## 🧩 Pipeline Stages

| Stage | Description | Output |
|--------|--------------|---------|
| **Ingestion** | Reads raw CSV files and converts them into canonical schema | Normalized DataFrame |
| **Enrichment** | Joins product & client master data and derives new fields | Enriched DataFrame |
| **Validation** | Runs data-quality and business rules | Curated + Exception outputs |
| **Outcome** | Stores results: curated (passed), exceptions (failed), metrics (summary) | Files in `20_outcome/` |

---

### Validation Rule Set 

| Rule ID            | Description                                                       | Severity  |
|--------------------|-------------------------------------------------------------------|-----------|
| R001_MANDATORY     | Mandatory fields must not be null (`trade_id`, `order_id`, etc.) | CRITICAL  |
| R002_ENUMS         | Enum checks for `side`, `instrument_type`, `currency`            | HIGH      |
| R003_POSITIVE      | `quantity` and `price` must be > 0                               | HIGH      |
| R004_ID_FORMAT     | ISIN / CUSIP format verification (regex-based)                   | MEDIUM    |
| R005_TS_SANITY     | `trade_ts` must be on/after `trade_date`                         | MEDIUM    |
| R006_NOTIONAL      | `notional ≈ quantity × price` within tolerance                   | LOW       |
| R007_DUPLICATES    | Duplicate `trade_id` or (`order_id`,`trade_ts`,`quantity`,`price`)| CRITICAL |


---

## 📊 Example Run Summary

```bash
INFO | ingestion | Starting ingestion...
INFO | enrichment | Enrichment completed.
INFO | validation | Validation complete. Passed=8460 Failed=1540 Breaches=1566
=== Validation Summary ===
{
  "input_rows": 10000,
  "passed_rows": 8460,
  "failed_rows": 1540,
  "pass_rate_pct": 84.6,
  "run_ts_utc": "2025-10-21T16:46:17Z"
}

PROJECT STRUCTURE
├── 00_input/
│   ├── inbound/              # raw trade feeds
│   └── reference/            # product_master.csv, client_master.csv
├── 10_process/
│   └── src/
│       ├── core/             # config, utils, logger
│       ├── p01_ingestion/    # schema normalization
│       ├── p02_enrichment/   # join reference data
│       ├── p03_validation/   # rule engine + validator
│       └── pipeline.py       # orchestrator 
├── 20_outcome/
│   ├── curated/              # clean validated data
│   ├── exceptions/           # failed records
│   └── metrics/              # validation summary
└── README.md
