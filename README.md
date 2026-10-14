# Marketplace Seller Mart

A production-grade, local data engineering pipeline that models sellers, products, orders, and returns into analytics-ready tables in SQLite, enriched with AI-generated weekly performance summaries.

Built exclusively using the **Python Standard Library** and **SQLite3**, requiring zero heavy runtime dependencies (e.g. no Pandas).

---

## Key Features

- **Multi-Tier Architecture**: Clean separation across raw (`raw_`), staging (`stg_`), and analytics mart (`mart_`) layers.
- **Incremental Processing**: Record-level change detection via deterministic SHA-256 hashing. Identical records are safely bypassed; updated records modify existing rows without duplication.
- **Dual-Transaction Auditability**: Business data transformations are executed within atomic transactions. Ingestion batches (`pipeline_batches`) and row-level rejections (`raw_rejections`) persist outside the data transaction for durable audit trails.
- **Single-Seller Invariant Enforcement**: Protects against ambiguous event attribution by rejecting orders or product reassignments that would cause an order to contain items from multiple sellers.
- **Cohort-Based Weekly Metrics**: Returns and cancellations are strictly attributed to the ISO week (`YYYY-Www`) of the order placement date. CTE aggregations prevent fan-out inflation, and zero denominators safely evaluate to `NULL`.
- **Stale Seller-Week Pruning**: Obsolete seller-week rows are pruned atomically when order dates shift or no attributable orders remain in that week.
- **Isolated AI Summaries with Deterministic Fallback**: Weekly summaries are generated from structured metrics using an isolated provider interface (`LocalOllamaProvider`). If Ollama is unavailable, offline, or times out, the pipeline automatically falls back to deterministic, rule-based summaries with zero external dependencies.

---

## Architecture & Data Flow

```
[CSV Files]
     │
     ▼
[Raw Ingestion] ───────────────► pipeline_batches (durable audit)
  (raw_sellers, raw_products,
   raw_orders, raw_order_items,
   raw_returns)
     │
     ▼
[Staging Validation & Load] ───► raw_rejections (durable audit)
  (stg_sellers, stg_products,
   stg_orders, stg_order_items,
   stg_returns)
     │  (enforces single-seller invariant & SHA-256 change detection)
     ▼
[Seller-Weekly Metrics Mart] ──► mart_seller_weekly_metrics
     │  (cohort ISO weeks, fan-out prevention, stale pruning)
     ▼
[AI Summary Generation] ───────► mart_summary_tracking
  (Ollama llama3.2 with deterministic fallback)
```

---

## Setup Instructions

### Prerequisites
- **Python 3.10+** (tested on Python 3.13)
- **SQLite 3** (included in Python standard library)

### 1. Extract & Navigate
Extract `marketplace-seller-mart.zip` into your desired working directory and navigate into it:
```powershell
cd marketplace-seller-mart
```

### 2. Create and Activate Virtual Environment
**Windows PowerShell**:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**macOS / Linux**:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies
Only `pytest` is required for testing:
```powershell
python -m pip install -r requirements.txt
```

### 4. Optional Environment Configuration
Copy `.env.example` to `.env` if customizing the local Ollama instance:
```powershell
Copy-Item .env.example .env
```
Contents of `.env.example`:
```env
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=llama3.2
```
*Note: If Ollama is not installed or running, the pipeline automatically uses the deterministic fallback. No configuration or API keys are required to run the pipeline offline.*

---

## Data Generation

The project includes a deterministic synthetic data generator with built-in overwrite protection:

```powershell
python -m src.generate_data
```

This generates five CSV files in `data/raw/`:
- `sellers.csv` (10 sellers)
- `products.csv` (50 products)
- `orders.csv` (200 orders across DELIVERED, SHIPPED, CANCELLED, PENDING)
- `order_items.csv` (1 to 4 items per order, strictly single-seller)
- `returns.csv` (returns linked to order IDs, including occasional multiple returns)

*Overwrite Protection*: If files already exist in `data/raw/`, running the generator without overwrite permissions raises `FileExistsError` to prevent accidental data loss.

---

## Running the Pipeline

Execute the end-to-end pipeline via the CLI:

```powershell
python -m src.main
```

### Command-Line Arguments & Options

| Option | Type | Default | Description |
|---|---|---|---|
| `--db-path` | `Path` | `data/marketplace.db` | Target SQLite database file. |
| `--raw-dir` | `Path` | `data/raw` | Directory containing raw CSV files. |
| `--generate-data` | `flag` | `False` | Generates synthetic CSV data before running the pipeline. Automatically triggered if `data/raw/` is empty. |
| `--overwrite-data`| `flag` | `False` | Allows overwriting existing CSV files when generating data. |
| `--force-summaries`| `flag` | `False` | Forces regeneration of all AI summaries even when metrics are unchanged. |

### Example CLI Commands

**Run with automatic data generation if CSVs are missing:**
```powershell
python -m src.main
```

**Generate fresh data and overwrite existing CSVs:**
```powershell
python -m src.main --generate-data --overwrite-data
```

**Force regeneration of all AI summaries:**
```powershell
python -m src.main --force-summaries
```

**Target a custom database file:**
```powershell
python -m src.main --db-path outputs/test_run.db
```

### Exit Codes
- `0`: Pipeline completed successfully (`status = SUCCESS`).
- `1`: Pipeline failed on a required stage (`status = FAILED`).

---

## Optional Local AI Summary Provider

- **Interface**: Abstract class `AIProvider(ABC)` in `src/ai_summary.py`.
- **Default Implementation**: `LocalOllamaProvider`, connecting to Ollama at `http://localhost:11434` (model: `llama3.2`).
- **Prompt Grounding**: The LLM prompt strictly restricts assertions to calculated metrics (`sales_volume`, `order_return_rate`, `cancellation_rate`) and prohibits hallucinating external causes.
- **Incremental Refresh**: `mart_summary_tracking` stores a deterministic SHA-256 hash of the three business metrics. Unchanged metrics skip LLM invocation on subsequent runs.
- **Offline / Failure Fallback**: If Ollama is offline, unreachable, times out (2.0s), or returns an empty response, the pipeline calls `generate_deterministic_summary()` to produce an accurate, rule-based summary without failing the pipeline.

---

## Database Schema & Key Metric Definitions

### Table Layers
1. **Pipeline Audit**:
   - `pipeline_batches`: Ingestion batch tracking (`batch_id`, `filename`, `status`, `error_details`, timestamps).
   - `raw_rejections`: Row-level rejection log (`rejection_id`, `batch_id`, `source_table`, `raw_record_json`, `rejection_reason`).
2. **Raw Layer** (all columns stored as `TEXT`):
   - `raw_sellers`, `raw_products`, `raw_orders`, `raw_order_items`, `raw_returns`.
3. **Staging Layer** (typed, validated, primary and foreign keys):
   - `stg_sellers`, `stg_products`, `stg_orders`, `stg_order_items`, `stg_returns`.
4. **Mart Layer**:
   - `mart_seller_weekly_metrics`: Analytics mart table (`seller_id`, `reporting_week`, `sales_volume`, `order_return_rate`, `cancellation_rate`, `ai_summary`).
   - `mart_summary_tracking`: Summary change detection (`seller_id`, `reporting_week`, `metrics_hash`, `generated_at`).

### Metric Definitions & Rules
- **Cohort Reporting (`reporting_week`)**: Formatted as ISO 8601 week `YYYY-Www` (Monday to Sunday) based on `order_date`. All subsequent returns and cancellations map to the week the order was placed.
- **Sales Volume**: `SUM(stg_order_items.quantity * stg_order_items.price)` for items in orders with `status IN ('DELIVERED', 'SHIPPED')`.
- **Order Return Rate**:
  - *Numerator*: Count of distinct `order_id`s with `status IN ('DELIVERED', 'SHIPPED')` that have at least one return record.
  - *Denominator*: Count of distinct `order_id`s with `status IN ('DELIVERED', 'SHIPPED')`.
  - Evaluates to `NULL` when denominator is 0.
- **Cancellation Rate**:
  - *Numerator*: Count of distinct `order_id`s where `status = 'CANCELLED'`.
  - *Denominator*: Count of all distinct attributable `order_id`s for the seller.
  - Evaluates to `NULL` when denominator is 0.
- **Single-Seller Attribution**: Staged orders without items are not attributed to arbitrary sellers.
- **Stale Week Pruning**: Before upserting metrics, seller-weeks with no attributable orders are pruned atomically from `mart_seller_weekly_metrics`.

---

## Running the Test Suite

The test suite runs 100% offline without network or external service dependencies.

Execute the full test suite:
```powershell
python -m pytest -q
```

### Test Coverage Summary (93 Tests across 10 Test Modules)
- `tests/test_schema.py` (6 tests): Schema initialization, foreign key enforcement, transaction commit, and atomic rollback.
- `tests/test_generation.py` (8 tests): Determinism, single-seller constraint, foreign key integrity, headers, and overwrite protection.
- `tests/test_validation.py` (4 tests): Data-type validators for dates, numbers, and allowed order statuses.
- `tests/test_change_detection.py` (6 tests): Canonical record serialization, deterministic SHA-256 hashing, null vs. empty string policy, and key order independence.
- `tests/test_ingestion.py` (20 tests): CSV ingestion, batch auditing, header checks, invalid row rejection, duplicate handling, and rollback on DB error.
- `tests/test_pipeline.py` (9 tests): Staging transformations, duplicate ID rejection, multi-seller rejection, and product seller reassignment rejection.
- `tests/test_metrics.py` (11 tests): Metrics aggregations, fan-out prevention, zero denominators, ISO week cohorting, stale week pruning, and transaction rollback.
- `tests/test_ai_summary.py` (15 tests): Grounded prompt formatting, summary persistence, hash-based incremental skipping, mock providers, and deterministic fallback.
- `tests/test_e2e.py` (13 tests): Full end-to-end integration, repeated runs, changed-input propagation, stage failure gating, `fail_stage` single invocation verification, and CLI entrypoint.
- `tests/test_smoke.py` (2 tests): Project structure, module imports, and baseline execution environment checks.

---

## Demo Walkthrough

### Step 1: Initialize and Run Pipeline
```powershell
python -m src.main --generate-data --overwrite-data
```
Expected output:
```text
============================================================
Marketplace Seller Mart - Pipeline Execution
============================================================
[1/5] Initializing database schema...
  -> Schema initialized successfully.
[DATA_GEN] Generating synthetic CSV data...
Generated data in ...\data\raw
Sellers: 10, Products: 50, Orders: 200, Items: 529, Returns: 26
  -> Synthetic CSV data generated.
[2/5] Ingesting CSV files into raw layer...
  -> Raw ingestion completed successfully.
[3/5] Validating and staging data...
  -> Staging pipeline completed successfully.
[4/5] Calculating seller-weekly metrics...
  -> Metrics calculated successfully (50 active seller-weeks).
[5/5] Generating weekly performance summaries...
  -> Summaries generated/refreshed (50 updated).

Pipeline completed successfully! [RESULT: SUCCESS]
```

### Step 2: Query the Analytics Mart
Inspect the generated metrics and AI summaries using Python or SQLite CLI:
```powershell
python -c "import sqlite3; conn = sqlite3.connect('data/marketplace.db'); rows = conn.execute('SELECT seller_id, reporting_week, sales_volume, order_return_rate, cancellation_rate, ai_summary FROM mart_seller_weekly_metrics LIMIT 2').fetchall(); [print(f'Seller: {r[0]} | Week: {r[1]} | Sales: ${r[2]:,.2f} | Return Rate: {r[3]} | Cancel Rate: {r[4]}\nSummary: {r[5]}\n---') for r in rows]"
```
Expected output:
```text
Seller: S001 | Week: 2026-W36 | Sales: $3,831.67 | Return Rate: 0.25 | Cancel Rate: 0.0
Summary: Seller S001 performance summary for 2026-W36: Generated $3,831.67 in eligible sales volume with an elevated return rate of 25.0%. For order fulfillment, no orders were cancelled.
---
Seller: S001 | Week: 2026-W37 | Sales: $0.00 | Return Rate: None | Cancel Rate: 0.0
Summary: Seller S001 performance summary for 2026-W37: Generated $0.00 in eligible sales volume with return rate unavailable (no eligible orders). For order fulfillment, no orders were cancelled.
---
```

### Step 3: Run the Complete Test Suite
```powershell
python -m pytest -q
```
Expected output:
```text
93 passed in ~10s
```

---

## Troubleshooting

- **`FileExistsError: File ... already exists`**:
  - Cause: Running `src.generate_data` or `main --generate-data` when CSV files exist.
  - Solution: Pass `--overwrite-data` if you intend to replace existing files.
- **AI Summary indicates "performance summary for ... (deterministic fallback)"**:
  - Cause: Ollama is not installed or not running at `http://localhost:11434`.
  - Solution: This is normal offline behavior. To use Ollama, run `ollama serve` and pull `llama3.2`.
- **Viewing Validation Rejections**:
  - Query rejected records:
    ```sql
    SELECT source_table, rejection_reason, raw_record_json FROM raw_rejections;
    ```
