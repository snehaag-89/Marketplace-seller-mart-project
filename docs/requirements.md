# Requirements & Implementation Matrix

DAI-029: Marketplace Seller Mart 

## 1. Confirmed Requirements 

| Req ID | Requirement | Acceptance Criteria |
|---|---|---|
| REQ-01 | Model sellers, products, orders, and returns into analytics-ready tables | **Initial ingestion**: Populates raw tables successfully. Staging tables enforce data types. Analytics tables (marts) are created and queryable. |
| REQ-02 | Implement incremental processing | **Repeated ingestion**: Processing the same file twice creates no duplicates. **Changed records**: A modified record updates the existing row using deterministic SHA-256 change detection. **Unchanged records**: Identical records are bypassed. |
| REQ-03 | Calculate seller-quality metrics | **Accurate metrics**: Pipeline calculates return rate, cancellation rate, and volume accurately per seller/week. SQL aggregation prevents fan-out inflation from multiple items or return records. |
| REQ-04 | Generate an AI-powered weekly summary | **Summary execution**: A script generates a weekly summary for a seller, cleanly distinguishing calculated facts from AI-generated interpretations. AI calls are isolated behind a clear interface using a free/local model. |
| REQ-05 | Implement testing with `pytest` | **Automated testing**: Tests run fully offline without a live LLM. Coverage includes at least 3 tests and one failure/edge-case demonstration. |
| REQ-06 | Final deliverables | **Clean setup**: Solution runs from a clean setup. Git repository includes a README with setup/architecture, runnable code, synthetic sample data, SQL/data model, tests, a 1-page design/decision note, and a 5-8 min demo video. Secrets are kept out of code. |

## 2. Proposed Design Choices & Assumptions (To Be Confirmed)

| Req ID | Design Choice / Assumption | Acceptance Criteria |
|---|---|---|
| ASM-01 | Auditability & Invalid Records | **Invalid records**: Records failing constraints do not fail the batch; they are diverted to a rejection table with a traceable reason. Batch status and rejections are persisted outside the main data transaction for auditability. |
| ASM-02 | Atomic Data Transactions | **Failed-batch recovery**: Business-table changes are atomic. If a pipeline failure occurs, the data transaction rolls back cleanly, but the batch failure and error details are logged persistently. |
| ASM-03 | Cohort-Based Weekly Reporting | **Reporting Period**: Weekly metrics are assigned based on the `order_date` (cohort-based). Returns and cancellations are attributed to the week the order was placed. |
| ASM-04 | Simplified Returns Model | **Order-Level Returns**: Returns are linked to the order header (`order_id`) rather than individual items. |
| ASM-05 | Single-Seller Attribution | **Seller Attribution**: To ensure unambiguous attribution of order-level events (returns, cancellations), synthetic data is constrained so each order contains products from exactly *one* seller. The pipeline validates this constraint. |
| ASM-06 | AI Fallback | **AI fallback**: If the AI provider times out or fails numeric validation, a deterministic, rule-based text summary is output instead. |

## 3. Confirmed Specifications & Implementation Boundaries
- **Schema Specifics**: Confirmed standard e-commerce fields across 5 CSV files (`sellers.csv`, `products.csv`, `orders.csv`, `order_items.csv`, `returns.csv`). Typed and validated in `sql/schema.sql` and `src/pipeline.py`.
- **Metric Formulas**: Confirmed cohort reporting mapped to `order_date` ISO week (`YYYY-Www`). Sales volume aggregates `SHIPPED` and `DELIVERED` orders; return rate divides orders with returns by eligible `SHIPPED`/`DELIVERED` orders; cancellation rate divides `CANCELLED` orders by all attributable orders. Zero denominators return `NULL`.
- **Seller Attribution**: Enforced single-seller invariant: orders without items cannot be attributed to arbitrary sellers.
- **Deletions & Pruning**: Physical row deletions from upstream CSVs are out-of-scope without full snapshots. Stale seller-weeks are pruned from the mart when order dates shift or no attributable orders remain in a week.

