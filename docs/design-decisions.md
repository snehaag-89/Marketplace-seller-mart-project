# Design Decisions

1. **SQLite**: Chosen for simplicity and portability without requiring external services.
2. **Python Standard Library**: We use standard `csv` and `sqlite3` modules to avoid heavy dependencies like pandas, prioritizing speed and minimal footprint.
3. **Change Detection**: We use SHA-256 hashes of serialized fields for deterministic incremental processing.
4. **Single-Seller Orders**: We enforce a constraint that each order contains items from exactly *one* seller. This guarantees unambiguous attribution of order-level returns and cancellations.
5. **Cohort-Based Reporting**: Weekly metrics strictly map to the ISO week of the `order_date`. A return processed three weeks later will recalculate the seller metrics for the *original* order week.
6. **AI Fallback**: The AI summary generation has a strict deterministic fallback mechanism to gracefully handle timeouts or format hallucinations.
