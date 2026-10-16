# Testing Evidence

All tests run **offline**. They need no network, no Ollama and no API keys.

## Result

| Item | Value |
|---|---|
| Command | `python -m pytest -v` |
| Result | **93 passed, 0 failed** |
| Run time | about 3 seconds |
| Python / pytest | 3.12.3 / 9.1.1 |
| Date | 2026-10-16 |

The full verbose output is saved in [`testing-output.txt`](testing-output.txt).

To reproduce:

```bash
python -m pip install -r requirements.txt
python -m pytest -v
```

## Tests per file

| File | Tests | What it covers |
|---|---|---|
| `test_schema.py` | 6 | Schema setup, foreign keys, commit and rollback |
| `test_generation.py` | 8 | Repeatable data, single-seller orders, overwrite protection |
| `test_validation.py` | 4 | Date, number and status checks |
| `test_change_detection.py` | 6 | Serialization and SHA-256 hashing |
| `test_ingestion.py` | 20 | CSV loading, batch log, rejections, rollback |
| `test_pipeline.py` | 9 | Staging, duplicate and multi-seller rejection |
| `test_metrics.py` | 11 | Aggregation, double counting, zero denominators, ISO weeks, cleanup |
| `test_ai_summary.py` | 15 | Prompt, saving, hash-based skipping, fallback |
| `test_e2e.py` | 13 | Full runs, repeat runs, changed input, stage failure, CLI |
| `test_smoke.py` | 2 | Project structure and imports |

## Requirement to test mapping

| Requirement | Example tests |
|---|---|
| REQ-01 Tables ready for analysis | `test_e2e_successful_complete_run` |
| REQ-02 Incremental processing | `test_deterministic_hash_same_record`, `test_reingest_identical_records_skipped`, `test_repeated_staging_runs_succeed`, `test_e2e_changed_input_updates_downstream` |
| REQ-03 Metrics | `test_multiple_items_per_order`, `test_multiple_returns_for_one_order`, `test_week_boundaries`, `test_late_return_updates_original_order_week` |
| REQ-04 AI summary with fallback | `test_provider_unavailable_triggers_fallback`, `test_empty_or_invalid_provider_response_triggers_fallback` |
| REQ-05 Offline tests, failure/edge case | all of the above, plus the cases below |

## Failure and edge-case demonstrations

| Case | Test |
|---|---|
| Invalid rows go to `raw_rejections`, the batch continues | `test_invalid_row_rejection`, `test_invalid_types_and_missing_refs` |
| An order with two sellers is rejected | `test_single_seller_constraint`, `test_new_multi_seller_order_rejected` |
| A database error rolls back business data, audit rows stay | `test_atomic_rollback_on_db_error`, `test_metrics_pipeline_failure_rolls_back_atomically` |
| Zero denominators give `NULL` | `test_zero_denominators` |
| An order without items is not attributed to any seller | `test_order_without_items_not_attributed_to_seller` |
| A stale seller-week is removed when an order date changes | `test_stale_seller_week_removed_on_order_date_change` |
| AI unavailable, the pipeline still succeeds | `test_e2e_ai_unavailability_uses_deterministic_fallback` |

To run one of them on its own:

```bash
python -m pytest -v tests/test_metrics.py -k zero_denominators
```

## What is not covered

The real Ollama call is not exercised by the automated tests, because they run offline. It was only checked through the fallback path.
