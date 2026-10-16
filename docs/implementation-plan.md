# Implementation Plan


## Phase 1: Foundation & Data Generation 
*   Set up Python virtual environment, `requirements.txt`, and clean project skeleton.
*   Create synthetic CSV data generator. **Crucial Constraint**: Generate data such that each `order_id` contains items belonging to exactly *one* `seller_id`.
*   Define `pipeline_batches` and `raw_rejections` tables outside the main data transaction for auditability.

## Phase 2: Ingestion & Incremental Logic 
*   **Prioritize a working end-to-end pipeline**: Implement ingestion from CSV -> `raw_` -> `stg_`.
*   Implement deterministic SHA-256 hashing for record-level change detection, handling identical and stale records.
*   Implement dual-transaction logic: one for atomic business data (`raw_`, `stg_`), and one for persistent batch tracking and error logging.
*   Add validation rule: Divert records to `raw_rejections` if an order violates the single-seller constraint.

## Phase 3: Analytics & Metrics Calculation 
*   Create `mart_seller_weekly_metrics`.
*   Write SQL aggregations calculating Sales Volume, Order Return Rate, and Cancellation Rate. 
*   Use CTEs to pre-aggregate items and deduplicate returns at the order level *before* grouping, ensuring no fan-out inflation.
*   Implement explicit denominators (e.g. only 'DELIVERED' or 'SHIPPED' for return rate denominator) and zero-denominator handling.

## Phase 4: AI Integration & Fallback 
*   Implement `AIProvider` interface (compatible with a local/free model) and the deterministic fallback.
*   Implement validation logic to ensure the AI's cited numbers match calculated facts.

## Phase 5: Regression Testing & Refinement
*   Write `pytest` suites focusing on the explicit requirement-to-test mapping (including the mandatory failure/edge-case demonstration).
*   Perform a Clean Checkout Test: clone to a fresh directory, setup, and run end-to-end successfully.

## Phase 6: Documentation & Demo Preparation 
*   Finalize `README.md` (setup/architecture) and the 1-page design/decision note.
*   Record the 5-8 minute demo video showcasing the working pipeline.

---

## Requirement-to-Test Mapping

| Category | Implemented Test Scenarios | Test Module | Description |
|---|---|---|---|
| **Validation** | `test_invalid_row_rejection`, `test_invalid_types_and_missing_refs` | `tests/test_ingestion.py`, `tests/test_pipeline.py` | Records failing standard schema/type validation are routed to `raw_rejections` without failing the batch. |
| **Validation** | `test_single_seller_constraint`, `test_product_seller_change_causes_multi_seller_rejection`, `test_new_multi_seller_order_rejected` | `tests/test_pipeline.py` | Orders containing items from multiple sellers or product reassignments that violate single-seller attribution are rejected into `raw_rejections`. |
| **Change Detection**| `test_deterministic_hash_same_record`, `test_reingest_identical_records_skipped`, `test_repeated_staging_runs_succeed` | `tests/test_change_detection.py`, `tests/test_ingestion.py`, `tests/test_pipeline.py` | Repeated ingestion of identical records is skipped. Updated records apply correctly using deterministic SHA-256 hashes. |
| **Auditability & Recovery**| `test_atomic_rollback_on_db_error`, `test_atomic_rollback_on_conflict`, `test_metrics_pipeline_failure_rolls_back_atomically` | `tests/test_ingestion.py`, `tests/test_pipeline.py`, `tests/test_metrics.py` | A forced data failure rolls back business tables atomically while preserving batch status and rejection audit trails persistently. |
| **Metrics (Cohort)**| `test_week_boundaries`, `test_late_return_updates_original_order_week`, `test_stale_seller_week_removed_on_order_date_change` | `tests/test_metrics.py` | Returns and cancellations are correctly assigned to the `order_date` ISO week. Stale weeks are pruned if order dates shift. |
| **Metrics (Math)** | `test_zero_denominators`, `test_order_without_items_not_attributed_to_seller` | `tests/test_metrics.py` | Zero denominators evaluate to NULL. Orders without items are not attributed to arbitrary sellers. |
| **Metrics (Math)** | `test_multiple_items_per_order`, `test_multiple_returns_for_one_order` | `tests/test_metrics.py` | Orders with multiple items and multiple return records compute exact Sales Volume and Order Counts without inflation. |
| **AI Fallback** | `test_provider_unavailable_triggers_fallback`, `test_empty_or_invalid_provider_response_triggers_fallback`, `test_e2e_ai_unavailability_uses_deterministic_fallback` | `tests/test_ai_summary.py`, `tests/test_e2e.py` | Simulated LLM timeout, exception, or invalid output triggers the deterministic summary template cleanly without failing pipeline. |
| **E2E / Clean Setup**| `test_e2e_successful_complete_run`, `test_e2e_changed_input_updates_downstream`, `test_e2e_cli_entrypoint` | `tests/test_e2e.py` | Runs the full pipeline from an empty SQLite DB to populated marts, validating downstream updates and CLI execution. |

