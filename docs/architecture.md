# Architecture & Design

## 1. Data Pipeline Architecture (SQLite)

We use a 3-tier architecture with specific table prefixes in SQLite:

*   **`raw_` (Raw Layer)**: Direct load of CSV data. All columns are stored as `TEXT`. Includes ingestion metadata (`batch_id`, `ingested_at`).
*   **`stg_` (Staging Layer)**: Cleaned, casted, and validated data. Invalid rows are diverted to a rejection table.
*   **`mart_` (Analytics Layer)**: Denormalized tables and aggregated metrics optimized for reporting.

### Data Flow
`CSV Files` -> `Python Pipeline (Batch Tracking)` -> `raw_ tables` -> `SQL Transforms` -> `stg_ tables` (valid) / `raw_rejections` (invalid) -> `mart_ tables`

## 2. Table Definitions & Grain

*   **`pipeline_batches`**: Tracks ingestion batches (`batch_id` PK, `filename`, `status`, `error_details`, `started_at`, `completed_at`).
*   **`raw_rejections`**: Tracks invalid records. 
    *   **Keys/Columns**: `rejection_id` (PK), `batch_id`, `source_table`, `raw_record_json`, `rejection_reason`, `rejected_at`.
*   **`stg_sellers`**: Grain: 1 row per Seller (`seller_id` PK).
*   **`stg_products`**: Grain: 1 row per Product (`product_id` PK, `seller_id` FK -> `stg_sellers`).
*   **`stg_orders`**: Grain: 1 row per Order Header (`order_id` PK, `order_date`, `status`).
*   **`stg_order_items`**: Grain: 1 row per Order Item (`order_item_id` PK, `order_id` FK -> `stg_orders`, `product_id` FK -> `stg_products`, `quantity`, `price`).
*   **`stg_returns`**: Grain: 1 row per Return record (`return_id` PK, `order_id` FK -> `stg_orders`, `return_date`). Returns relate to the order header.

## 3. Incremental Processing & Change Detection

*   **Change Detection via SHA-256**: We use deterministic serialization of normalized fields (e.g., lowercased, trimmed strings, sorted JSON keys) followed by a SHA-256 cryptographic hash to detect changes.
*   **Record-Level Processing**: Stable business identifiers (`seller_id`, `order_id`, `order_item_id`, etc.) serve as primary keys. 
    *   **Identical Record**: The SHA-256 hash matches the existing record in `stg_`; the record is safely skipped.
    *   **Updated Record**: The hash differs. If source update timestamps are available, stale records (incoming timestamp < stored timestamp) are ignored. Otherwise, the record is updated using `INSERT ... ON CONFLICT(id) DO UPDATE`.
    *   **Deleted Record**: Physical source deletions cannot be inferred reliably from incremental CSVs without full snapshots. Deletions are out-of-scope for this iteration.

## 4. Auditability and Transactions

*   **Independent Audit Logging**: The `pipeline_batches` record (status `STARTED`) is committed immediately in a separate transaction (or connection). Validation rejections written to `raw_rejections` are also persisted outside the main data transaction.
*   **Atomic Business Changes**: Processing business data (`raw_`, `stg_`, `mart_`) is wrapped in a single, separate SQLite transaction.
    *   *Commit*: If successful, the data transaction commits, and `pipeline_batches` is updated to `SUCCESS`.
    *   *Fail*: The data transaction rolls back (preventing partial data loads), but the `pipeline_batches` status is updated to `FAILED` with `error_details` persisted. 

## 5. Business Metrics & Mart Transformations

The `mart_seller_weekly_metrics` table has a grain of 1 row per `seller_id` per `reporting_week`.

*   **Seller Attribution**: To unambiguously attribute order-level events (cancellations and returns) to a single seller, we enforce the constraint that **each order contains products from exactly one seller**. Seller attribution is derived strictly from `stg_order_items` -> `stg_products`. Staged orders lacking items cannot be attributed to any seller and are excluded from seller metrics to prevent arbitrary attribution.
*   **Reporting Date Rule**: The `reporting_week` is strictly the ISO week (Monday to Sunday, formatted `YYYY-Www`) of the `order_date` (cohort reporting).
*   **Zero-Denominator Behavior**: All rate calculations use `NULLIF(denominator, 0)` or `CASE` expressions to yield `NULL` when denominators are zero.
*   **Sales Volume**: `SUM(stg_order_items.price * stg_order_items.quantity)` for all items in an order where the order `status` is IN `('DELIVERED', 'SHIPPED')`.
*   **Order Return Rate**: 
    *   *Numerator*: Count of distinct `order_id`s with `status` IN `('DELIVERED', 'SHIPPED')` that have one or more return records.
    *   *Denominator*: Count of distinct `order_id`s where the order `status` is IN `('DELIVERED', 'SHIPPED')`.
*   **Cancellation Rate**: 
    *   *Numerator*: Count of distinct `order_id`s where `status = 'CANCELLED'`.
    *   *Denominator*: Count of all distinct attributable `order_id`s.
*   **Fan-Out Prevention**: SQL aggregations use CTEs (Common Table Expressions) to pre-aggregate order sales and deduplicate return flags at the `order_id` level *before* grouping by `seller_id` and week.
*   **Stale Seller-Week Pruning**: Before upserting calculated metrics, stale seller-week rows where no attributable orders remain in that week are pruned atomically within the same transaction.

## 6. AI Summary Workflow

*   **Interface**: An abstract Python class `AIProvider(ABC)` defining `generate_summary(metrics: Dict[str, Any]) -> Optional[str]`.
*   **Default Provider**: `LocalOllamaProvider` connecting to a local Ollama HTTP API endpoint (`http://localhost:11434` by default, configurable via `OLLAMA_BASE_URL` and `OLLAMA_MODEL`).
*   **Incremental Tracking**: `mart_summary_tracking` stores a deterministic SHA-256 hash of `(sales_volume, order_return_rate, cancellation_rate)`. Summaries are only regenerated when metrics change, existing summaries are missing/invalid, or explicitly forced.
*   **Grounding & Fallback**: The prompt enforces strict grounding in provided numbers without hallucinated causes. If the AI provider is offline, unreachable, times out (2.0s), or returns an invalid string, the pipeline invokes `generate_deterministic_summary()` to produce a structured, rule-based summary without failing the pipeline.

