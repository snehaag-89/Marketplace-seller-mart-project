# Marketplace Seller Mart: Project Documentation

**Assignment:** DAI-029, Marketplace Seller Mart
**Built with:** Python 3.10+ (standard library only), SQLite, pytest


---

## 1. What this project does

Marketplace Seller Mart is a small data pipeline that runs on your own machine. It reads five CSV files (sellers, products, orders, order items, returns), checks and cleans them, and builds a table of **weekly quality numbers for each seller**: how much they sold, how often their orders were returned, and how often they were cancelled. For every seller-week it then adds a short written summary, produced by a local AI model (Ollama) or, if that isn't available, by a simple rule-based template.

### How it maps to the assignment

| ID | What was asked | Where to find it |
|---|---|---|
| REQ-01 | Model sellers, products, orders and returns into tables ready for analysis | `sql/schema.sql`, `src/pipeline.py` |
| REQ-02 | Process data incrementally and detect changes | `src/hash_utils.py`, `src/ingestion.py`, `src/pipeline.py` |
| REQ-03 | Seller quality metrics | `sql/transformations.sql`, `src/pipeline.py` |
| REQ-04 | AI weekly summary behind a clean interface | `src/ai_summary.py` |
| REQ-05 | pytest suite that runs offline, with an edge case | `tests/` |
| REQ-06 | Repo, README, SQL, design note, demo video | this repository |

---

## 2. What's in the repository

```
marketplace-seller-mart/
├── README.md                  Setup, usage and demo walkthrough
├── requirements.txt           pytest only
├── .env.example               Optional Ollama settings (no secrets)
├── data/raw/                  Input CSVs (generated)
├── outputs/                   Generated output
├── docs/
│   ├── PROJECT_DOCUMENTATION.md   This file
│   ├── architecture.md            How the layers and tables fit together
│   ├── design-decisions.md        1-page decision note
│   ├── requirements.md            Requirement matrix
│   ├── implementation-plan.md     Plan and requirement-to-test map
│   └── demo-script.md             Script for the demo video
├── sql/
│   ├── schema.sql             Table definitions (safe to run repeatedly)
│   └── transformations.sql    Seller-weekly metrics SQL
├── src/
│   ├── main.py                Command-line entry point, runs all stages
│   ├── config.py              Paths and defaults
│   ├── db.py                  Connections and schema setup
│   ├── generate_data.py       Synthetic data generator
│   ├── ingestion.py           CSV to raw_ tables, batch logging
│   ├── validation.py          Date, number and status checks
│   ├── hash_utils.py          Record fingerprints (SHA-256)
│   ├── pipeline.py            raw_ to stg_ to mart_
│   ├── ai_summary.py          AI interface, Ollama provider, fallback
│   └── metrics.py             Placeholder (metrics live in SQL and pipeline.py)
└── tests/                     93 tests in 10 files
```

---

## 3. How it works

### 3.1 The flow

```
CSV files
   ▼
raw_ tables ───────────► pipeline_batches   (log of each file load)
   ▼
stg_ tables ───────────► raw_rejections     (bad rows and why)
   ▼
mart_seller_weekly_metrics
   ▼
Weekly summaries (Ollama, or rule-based fallback)
```

### 3.2 The five stages (`src/main.py`)

| Stage | What it does | If it fails |
|---|---|---|
| 1 | Create the tables from `sql/schema.sql` | Run stops, exit code 1 |
| 2 | Load the CSVs into `raw_` tables | The batch is marked FAILED and the run stops |
| 3 | Check and load the `stg_` tables | That stage rolls back and the run stops |
| 4 | Calculate the weekly metrics | That stage rolls back and the run stops |
| 5 | Write the summaries | A failing AI call falls back to the rule-based text, and the run continues |

If you pass `--generate-data`, or the `data/raw/` folder is empty, the synthetic data is generated first. The program exits with `0` on success and `1` if a required stage fails.

### 3.3 Transactions and the audit trail

Two kinds of writes are kept apart on purpose:

- **Audit writes** (`pipeline_batches` and `raw_rejections`) use their own connections and are saved straight away, so a rollback can't erase them. A batch goes from `STARTED` to `COMPLETED` or `FAILED`, and failures keep an `error_details` message.
- **Business writes** are atomic **per stage**. Raw ingestion is one transaction per file, staging is one transaction, and the metrics step (cleanup plus upsert) is one transaction. If a stage fails, that stage rolls back and the run stops. Stages that already finished stay saved, and re-running is safe because unchanged records are skipped.

So a failed stage never leaves half-written data behind, and there is always a record of what went wrong.

---

## 4. Data model

### 4.1 Tables

| Layer | Table | One row per |
|---|---|---|
| Audit | `pipeline_batches` | file load (`batch_id`) |
| Audit | `raw_rejections` | rejected record (`rejection_id`) |
| Raw | `raw_sellers`, `raw_products`, `raw_orders`, `raw_order_items`, `raw_returns` | source row, stored as text with `batch_id` and `ingested_at` |
| Staging | `stg_sellers` | seller (`seller_id`) |
| Staging | `stg_products` | product (`product_id`, FK to seller) |
| Staging | `stg_orders` | order (`order_id`, `order_date`, `status`) |
| Staging | `stg_order_items` | item (`order_item_id`, FK to order and product, integer `quantity`, real `price`) |
| Staging | `stg_returns` | return (`return_id`, FK to order) |
| Mart | `mart_seller_weekly_metrics` | seller and week |
| Mart | `mart_summary_tracking` | seller and week, with a hash of the metrics |

### 4.2 Input files

| File | Columns |
|---|---|
| `sellers.csv` | `seller_id`, `seller_name` |
| `products.csv` | `product_id`, `seller_id`, `product_name` |
| `orders.csv` | `order_id`, `order_date`, `status` |
| `order_items.csv` | `order_item_id`, `order_id`, `product_id`, `quantity`, `price` |
| `returns.csv` | `return_id`, `order_id`, `return_date` |

Order statuses allowed: `DELIVERED`, `SHIPPED`, `CANCELLED`, `PENDING`.

### 4.3 What gets rejected

A bad row never fails the whole file. It goes into `raw_rejections` with a reason. Typical reasons:

- a missing or malformed date, number or status
- a reference to something that doesn't exist (for example a product whose seller isn't there)
- a duplicate ID within a file
- an order that would end up with items from more than one seller

To look at them:

```sql
SELECT source_table, rejection_reason, raw_record_json FROM raw_rejections;
```

---

## 5. Incremental processing

Every record is turned into a SHA-256 fingerprint (`src/hash_utils.py`). Keys are sorted, the JSON is compact, values are kept exactly as they are, `NULL` is treated differently from an empty string, and bookkeeping fields like `batch_id` are left out.

| Situation | Result |
|---|---|
| Same file loaded again | Fingerprints match, rows are skipped, no duplicates |
| A record changed | The existing row is updated in place |
| A row is missing from a later file | Not detected (see limitations) |

Changes carry through to the results: if an order's date or status changes, the affected seller-weeks are recalculated, and a seller-week with no orders left is removed.

Summaries use the same idea. `mart_summary_tracking` stores a hash of `(sales_volume, order_return_rate, cancellation_rate)`, and the AI is only called again when that hash changes, or when you pass `--force-summaries`.

---

## 6. How the metrics are defined

Everything is per seller per **reporting week**, which is the ISO week (`YYYY-Www`, Monday to Sunday) of the order date. Returns and cancellations count in the week the order was placed, not the week the return happened.

| Metric | Definition | If the denominator is zero |
|---|---|---|
| `sales_volume` | quantity × price for items in `SHIPPED` or `DELIVERED` orders | `0.0` |
| `order_return_rate` | shipped/delivered orders with at least one return ÷ shipped/delivered orders | `NULL` |
| `cancellation_rate` | `CANCELLED` orders ÷ all attributable orders | `NULL` |

**Seller attribution.** A seller is found through the order's items and their products. Orders with no items can't be attributed and are left out.

**Avoiding double counting.** Before grouping by seller and week, items are reduced to one row per order and returns to one yes/no flag per order. An order with four items or three return records still counts once.

---

## 7. AI summaries

| Part | What it does |
|---|---|
| `AIProvider` | The interface: `generate_summary(metrics)` returns text or `None` |
| `LocalOllamaProvider` | Calls Ollama at `OLLAMA_BASE_URL` (default `http://localhost:11434`), model `OLLAMA_MODEL` (default `llama3.2`), 2-second timeout |
| `MockAIProvider` | Used in tests so no network is needed |
| `format_prompt()` | Builds a prompt with only the three calculated metrics and tells the model not to invent causes |
| `is_valid_summary()` | Rejects empty text, text under 20 characters, or text starting with `error:`. It does **not** check that the numbers match |
| `generate_deterministic_summary()` | Writes a rule-based summary from the metrics |
| `generate_seller_summaries()` | Ties it together: generate, hash, save |

**When the fallback kicks in:** Ollama isn't running, can't be reached, takes too long, or returns something unusable. The pipeline logs a warning and uses the rule-based summary. The AI step never stops the run.

**Example (fallback output from a real run):**

> Seller S001 performance summary for 2026-W36: Generated $3,831.67 in eligible sales volume with an elevated return rate of 25.0%. For order fulfillment, no orders were cancelled.

---

## 8. Setup and usage

You need Python 3.10 or newer. SQLite comes with Python.

```bash
python3 -m venv .venv
source .venv/bin/activate            # Windows: .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

**Optional, for the real AI model:**

```bash
ollama pull llama3.2
ollama serve
cp .env.example .env                 # only if you want non-default settings
```

No API keys are used anywhere, and without Ollama the pipeline still runs from start to finish.

**Run it:**

```bash
python -m src.main                                  # generates data if data/raw is empty
python -m src.main --generate-data --overwrite-data # regenerate the CSVs
python -m src.main --force-summaries                # regenerate every summary
python -m src.main --db-path outputs/test_run.db    # use a different database file
```

| Option | Default | What it does |
|---|---|---|
| `--db-path` | `data/marketplace.db` | The SQLite file |
| `--raw-dir` | `data/raw` | Folder with the input CSVs |
| `--generate-data` | off | Create synthetic CSVs first |
| `--overwrite-data` | off | Allow replacing existing CSVs |
| `--force-summaries` | off | Ignore the hash and regenerate summaries |

The synthetic data (seed 42) has 10 sellers, 50 products, 200 orders, 529 items and 26 returns, spread over five ISO weeks (`2026-W36` to `2026-W40`).

**Look at the results:**

```sql
SELECT seller_id, reporting_week, sales_volume,
       order_return_rate, cancellation_rate, ai_summary
FROM mart_seller_weekly_metrics
ORDER BY seller_id, reporting_week;
```

---

## 9. Testing

```bash
python -m pytest -q
```

The last run gave **93 passed** in a few seconds, entirely offline. Full output is in `docs/testing-evidence.md`.

| File | Tests | What it covers |
|---|---|---|
| `test_schema.py` | 6 | Schema setup, foreign keys, commit and rollback |
| `test_generation.py` | 8 | Repeatable data, single-seller orders, overwrite protection |
| `test_validation.py` | 4 | Date, number and status checks |
| `test_change_detection.py` | 6 | Serialization and hashing |
| `test_ingestion.py` | 20 | CSV loading, batch logging, rejections, rollback |
| `test_pipeline.py` | 9 | Staging, duplicate and multi-seller rejection |
| `test_metrics.py` | 10 | Aggregation, double counting, zero denominators, ISO weeks, cleanup |
| `test_ai_summary.py` | 15 | Prompt, saving, hash-based skipping, fallback |
| `test_e2e.py` | 13 | Full runs, repeat runs, changed input, stage failure, CLI |
| `test_smoke.py` | 2 | Structure and imports |

Edge cases covered include: invalid rows going to `raw_rejections`, an order with two sellers being rejected, a forced database error rolling back while the audit rows stay, zero denominators returning `NULL`, a late return updating the original order's week, and the AI being unavailable.

---

## 10. Why I built it this way

| Choice | Reason | Downside |
|---|---|---|
| SQLite and the standard library | Nothing to install or run | Not meant for large data or many writers |
| SHA-256 fingerprints | Simple and easy to explain | Can't spot deleted rows |
| Audit log on separate connections | Failures stay traceable even after a rollback | A bit more connection handling |
| One seller per order | Returns and cancellations map to exactly one seller | Real carts can mix sellers |
| Weeks based on order date | Rates stay easy to interpret | A late return changes an older week |
| Returns tied to the order, not the item | Keeps the model simple | No item-level return rates |
| Local AI with a rule-based fallback | Free, private, testable offline | Fallback text is plainer |

The full reasoning is in `docs/design-decisions.md`.

---

## 11. Known limitations

1. **Deleted rows aren't noticed.** A row removed from a later CSV stays in staging. Fixing that would need full snapshots.
2. **The AI output check is basic.** It checks length and format only. It doesn't verify that the numbers in the text match the metrics. The prompt asks the model to stay within the numbers, but nothing enforces it afterwards.
3. **Summary source isn't stored.** The `ai_summary` column doesn't say whether the text came from the model or the fallback; only the logs do. Adding a `summary_source` column would fix that.
4. **No stale-record rule.** The CSVs have no update timestamp, so a changed record always overwrites the stored one.
5. **Every seller gets every week.** Seller-weeks with no sales show up with `sales_volume = 0`, so the sample data has 50 rows (10 sellers × 5 weeks).
6. **Ollama isn't covered by the automated tests**, since they run offline. Try it manually before the demo.
7. **`src/metrics.py` is a placeholder.** The metrics logic is in `sql/transformations.sql` and `src/pipeline.py`.
8. **Two `compute_iso_week` functions** exist (in `src/main.py` and `src/pipeline.py`). One would be enough.

---

## 12. Troubleshooting

| What you see | Why | What to do |
|---|---|---|
| `FileExistsError` from the generator | The CSVs already exist | Add `--overwrite-data` |
| Log lines saying "AI provider error ... using deterministic fallback" | Ollama isn't running | That's expected offline. Run `ollama serve` to use the model |
| Some rows are missing from staging | They were rejected | Query `raw_rejections` |
| The pipeline exits with code 1 | A required stage failed | Check the console and `pipeline_batches.error_details` |

---

## 13. Submission checklist

- [x] Git repository created, committed and pushed to GitHub (no `.env`, `.venv` or `*.db` files)
- [x] Sample CSVs included in `data/raw/` (the `.gitignore` rule that hid them is removed)
- [x] README with setup and architecture
- [x] Project documentation (this file)
- [x] Architecture and design explanation (`docs/architecture.md`)
- [x] 1-page design note (`docs/design-decisions.md`)
- [x] SQL and data model (`sql/schema.sql`, `sql/transformations.sql`)
- [x] Tests passing offline (93)
- [x] Testing evidence saved in the repo (`docs/testing-evidence.md`)
- [x] 5 to 8 minute demo video recorded and linked in the README
- [x] Fresh-copy run checked (clean setup, tests and pipeline both pass)
