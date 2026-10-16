# Design Decisions

A short note on the main choices I made in the Marketplace Seller Mart pipeline, why I made them, and what each one costs.

1. **SQLite.** Chosen for simplicity and portability. Anyone can clone the repo and run it with no database server. *Trade-off:* it isn't built for large data volumes or many writers at once.

2. **Python standard library only.** I use the built-in `csv` and `sqlite3` modules instead of pandas, so the only extra package is `pytest`. This keeps setup quick and the footprint small. *Trade-off:* a bit more code for things pandas would do in one line.

3. **Raw, staging and mart layers.** Raw keeps the data exactly as it arrived, staging cleans and checks it, and the mart is the table people query. If something looks wrong in the mart, I can trace it back to the original row. *Trade-off:* more tables than a single transform would need.

4. **Change detection with SHA-256.** Each record is turned into a fingerprint (sorted keys, compact JSON, values kept as they are). The same fingerprint means the row is skipped, and a different one means the existing row is updated. Re-running never creates duplicates. *Trade-off:* it can't tell when a row has been deleted from the source.

5. **Bad rows are rejected, not fatal.** A row with an invalid date, unknown status, missing reference or duplicate ID goes to `raw_rejections` with a reason, and the rest of the file still loads. *Trade-off:* someone has to look at the rejection table.

6. **Audit log on separate connections.** `pipeline_batches` and `raw_rejections` are saved on their own, while each stage's data changes are atomic. If a stage fails, it rolls back cleanly and the record of what went wrong is still there. *Trade-off:* slightly more connection handling in the code.

7. **Single-seller orders.** Each order contains items from exactly one seller. Returns and cancellations belong to an order, so this is what lets me attribute them to one seller without guessing. Orders that break the rule are rejected. *Trade-off:* real carts can mix sellers, so a production system would need to split those orders per seller.

8. **Cohort-based reporting.** Weekly metrics use the ISO week of the `order_date`. A return processed three weeks later is counted in the *original* order week, which keeps the rates easy to interpret. *Trade-off:* an older week's numbers can change when a late return arrives.

9. **Metrics are pre-aggregated.** Items and returns are reduced to one row per order before grouping by seller and week, so an order with four items or three returns still counts once. When a denominator is zero, the rate is `NULL`, because "no eligible orders" is not the same as "0%". *Trade-off:* longer SQL, and anyone using the data has to handle `NULL`.

10. **AI behind an interface, with a deterministic fallback.** The pipeline calls an `AIProvider`, and the default is a local Ollama model, so it's free and private. The prompt contains only the calculated metrics. If the model is offline, times out or returns something unusable (empty, very short, or starting with `error:`), the pipeline writes a rule-based summary from the same numbers and carries on. *Trade-off:* the fallback text is plainer, and the check on AI output is basic. It doesn't confirm that the numbers in the text match the metrics.

## Limitations and next steps

- **Deleted rows aren't detected.** A row removed from a later CSV stays in staging. Fixing this needs full snapshots or a delete flag.
- **No record of where a summary came from.** A `summary_source` column (model or fallback) would make that clear.
- **AI output isn't number-checked.** Adding a check that the figures in the text match the metrics would be the next improvement.
- **Out of scope for now:** multi-seller orders and item-level returns.