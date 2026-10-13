"""End-to-end integration and orchestration tests for Phase 5."""
import csv
import sqlite3
from pathlib import Path
from typing import Optional

import pytest

from src.ai_summary import AIProvider, AIProviderError, MockAIProvider
from src.config import PROJECT_ROOT
from src.db import get_db_connection, init_db
from src.main import PipelineError, compute_iso_week, fail_stage, main, run_pipeline

SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"


class FailingAIProvider(AIProvider):
    """AI provider that always raises an error to simulate model unavailability."""

    def __init__(self):
        self.call_count = 0

    def generate_summary(self, metrics: dict) -> str:
        self.call_count += 1
        raise AIProviderError("Simulated LLM connection timeout / service unavailable")


@pytest.fixture
def e2e_env(tmp_path):
    """Set up isolated directory paths for an e2e pipeline test."""
    db_path = tmp_path / "e2e_test.db"
    raw_dir = tmp_path / "raw_data"
    raw_dir.mkdir(parents=True, exist_ok=True)
    return {
        "db_path": db_path,
        "raw_dir": raw_dir,
        "schema_path": SCHEMA_PATH,
    }


def test_e2e_successful_complete_run(e2e_env):
    """Test 1: Complete end-to-end pipeline run from data generation to AI summaries."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    mock_ai = MockAIProvider(
        responder=lambda m: f"AI Summary for {m.get('seller_id')} ({m.get('reporting_week')})"
    )

    result = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        schema_path=e2e_env["schema_path"],
        generate_data=True,
        overwrite_data=False,
        ai_provider=mock_ai,
        force_summaries=False,
    )

    # 1. Pipeline execution status
    assert result["status"] == "SUCCESS", f"Pipeline failed: {result.get('error')}"
    assert result["error"] is None

    expected_stages = ["schema_init", "data_generation", "raw_ingestion", "staging", "metrics", "ai_summaries"]
    for stage in expected_stages:
        assert stage in result["stages"]
        assert result["stages"][stage]["status"] == "SUCCESS"

    # 2. Check that CSV files were generated
    for fname in ["sellers.csv", "products.csv", "orders.csv", "order_items.csv", "returns.csv"]:
        assert (raw_dir / fname).exists(), f"Expected {fname} to be generated"

    # 3. Verify SQLite DB contents across layers
    with get_db_connection(db_path) as conn:
        conn.row_factory = sqlite3.Row
        # Raw layer
        raw_sellers_cnt = conn.execute("SELECT COUNT(*) FROM raw_sellers").fetchone()[0]
        raw_orders_cnt = conn.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
        assert raw_sellers_cnt > 0
        assert raw_orders_cnt > 0

        # Pipeline batches audit
        completed_batches = conn.execute(
            "SELECT COUNT(*) FROM pipeline_batches WHERE status = 'COMPLETED'"
        ).fetchone()[0]
        assert completed_batches == 5  # 5 files ingested

        # Staging layer
        stg_sellers_cnt = conn.execute("SELECT COUNT(*) FROM stg_sellers").fetchone()[0]
        stg_orders_cnt = conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
        assert stg_sellers_cnt > 0
        assert stg_orders_cnt > 0

        # Mart layer
        metrics_rows = conn.execute("SELECT * FROM mart_seller_weekly_metrics").fetchall()
        assert len(metrics_rows) > 0

        for row in metrics_rows:
            assert row["seller_id"] is not None
            assert row["reporting_week"] is not None
            assert row["sales_volume"] >= 0.0
            assert row["ai_summary"] is not None
            assert "AI Summary for" in row["ai_summary"]

        # Summary tracking table
        tracking_cnt = conn.execute("SELECT COUNT(*) FROM mart_summary_tracking").fetchone()[0]
        assert tracking_cnt == len(metrics_rows)

    assert mock_ai.call_count == len(metrics_rows)


def test_e2e_second_run_unchanged_input(e2e_env):
    """Test 2: Second run with unchanged input is idempotent and skips regeneration."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    mock_ai = MockAIProvider(
        responder=lambda m: f"Mock Summary {m.get('seller_id')}"
    )

    # First run
    res1 = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=True,
        ai_provider=mock_ai,
    )
    assert res1["status"] == "SUCCESS"
    initial_ai_calls = mock_ai.call_count
    assert initial_ai_calls > 0

    # Second run without data regeneration
    res2 = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=False,
        ai_provider=mock_ai,
        force_summaries=False,
    )
    assert res2["status"] == "SUCCESS"
    assert res2["stages"]["data_generation"]["status"] == "SKIPPED"

    # Ingestion idempotency check: all files should report 0 inserted, 0 updated, all unchanged
    ingest_details = res2["stages"]["raw_ingestion"]["details"]
    for fname, details in ingest_details.items():
        assert details["status"] == "COMPLETED"
        assert details["inserted"] == 0
        assert details["updated"] == 0
        assert details["unchanged"] > 0

    # AI summary check: 0 updated, no new calls to mock AI provider
    assert res2["stages"]["ai_summaries"]["updated"] == 0
    assert mock_ai.call_count == initial_ai_calls


def test_e2e_changed_input_updates_downstream(e2e_env):
    """Test 3: Select shipped/delivered item, increase price, assert exact metric and summary update."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    mock_ai = MockAIProvider(
        responder=lambda m: f"Summary for {m.get('seller_id')} ({m.get('reporting_week')}): ${m.get('sales_volume', 0):.2f}"
    )

    # Initial run
    res1 = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=True,
        ai_provider=mock_ai,
    )
    assert res1["status"] == "SUCCESS"

    # 1. Identify an order item that contributes to sales_volume (order status SHIPPED or DELIVERED)
    with get_db_connection(db_path) as conn:
        conn.row_factory = sqlite3.Row
        contributing_item = conn.execute(
            """
            SELECT 
                oi.order_item_id,
                oi.order_id,
                oi.quantity,
                oi.price,
                p.seller_id,
                o.order_date
            FROM stg_order_items oi
            JOIN stg_orders o ON oi.order_id = o.order_id
            JOIN stg_products p ON oi.product_id = p.product_id
            WHERE o.status IN ('SHIPPED', 'DELIVERED')
            LIMIT 1
            """
        ).fetchone()
        assert contributing_item is not None, "Expected at least one eligible shipped/delivered item"

        target_item_id = contributing_item["order_item_id"]
        target_seller = contributing_item["seller_id"]
        target_qty = int(contributing_item["quantity"])
        old_price = float(contributing_item["price"])
        target_week = compute_iso_week(contributing_item["order_date"])

        # Record initial seller-week sales_volume and ai_summary
        initial_metric = conn.execute(
            "SELECT sales_volume, ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = ? AND reporting_week = ?",
            (target_seller, target_week),
        ).fetchone()
        assert initial_metric is not None
        initial_sales = float(initial_metric["sales_volume"])
        initial_summary = initial_metric["ai_summary"]

    # 2. Modify raw CSV for this specific order item
    delta_price = 100.00
    new_price = old_price + delta_price
    expected_sales_increase = target_qty * delta_price
    expected_new_sales = initial_sales + expected_sales_increase

    items_csv = raw_dir / "order_items.csv"
    with open(items_csv, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        fieldnames = list(reader[0].keys())

    modified = False
    for row in reader:
        if row["order_item_id"] == target_item_id:
            row["price"] = f"{new_price:.2f}"
            modified = True
            break
    assert modified, f"Target item {target_item_id} not found in CSV"

    with open(items_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reader)

    # 3. Rerun pipeline
    res2 = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=False,
        ai_provider=mock_ai,
    )
    assert res2["status"] == "SUCCESS"

    # Assert raw item is updated
    assert res2["stages"]["raw_ingestion"]["details"]["order_items.csv"]["updated"] == 1
    with get_db_connection(db_path) as conn:
        conn.row_factory = sqlite3.Row
        raw_row = conn.execute(
            "SELECT price FROM raw_order_items WHERE order_item_id = ?",
            (target_item_id,),
        ).fetchone()
        assert raw_row is not None
        assert abs(float(raw_row["price"]) - new_price) < 1e-4

        # Assert staging item contains new price
        stg_row = conn.execute(
            "SELECT price FROM stg_order_items WHERE order_item_id = ?",
            (target_item_id,),
        ).fetchone()
        assert stg_row is not None
        assert abs(float(stg_row["price"]) - new_price) < 1e-4

        # Assert corresponding seller-week metric changes by expected amount
        updated_metric = conn.execute(
            "SELECT sales_volume, ai_summary FROM mart_seller_weekly_metrics WHERE seller_id = ? AND reporting_week = ?",
            (target_seller, target_week),
        ).fetchone()
        assert updated_metric is not None
        assert float(updated_metric["sales_volume"]) == pytest.approx(expected_new_sales, rel=1e-3)

        # Assert corresponding AI summary is regenerated
        new_summary = updated_metric["ai_summary"]
        assert new_summary != initial_summary
        assert f"${expected_new_sales:.2f}" in new_summary


def test_e2e_raise_on_error_data_generation(e2e_env):
    """Test 4a: raise_on_error=True raises PipelineError exactly once on data generation failure."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    mock_ai = MockAIProvider(responder="summary")

    # Initial data generation
    res1 = run_pipeline(db_path=db_path, raw_dir=raw_dir, generate_data=True, ai_provider=mock_ai)
    assert res1["status"] == "SUCCESS"

    # With CSVs existing, generating without overwrite fails
    with pytest.raises(PipelineError) as exc_info:
        run_pipeline(
            db_path=db_path,
            raw_dir=raw_dir,
            generate_data=True,
            overwrite_data=False,
            ai_provider=mock_ai,
            raise_on_error=True,
        )

    err_msg = str(exc_info.value)
    assert err_msg.startswith("Stage 'data_generation' failed:")
    assert "already exists" in err_msg


def test_e2e_raise_on_error_raw_ingestion(e2e_env):
    """Test 4b: raise_on_error=True raises PipelineError exactly once on raw ingestion failure."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    mock_ai = MockAIProvider(responder="summary")

    init_db(db_path, e2e_env["schema_path"])
    corrupt_csv = raw_dir / "sellers.csv"
    corrupt_csv.write_text("bad_col1,bad_col2\nS1,Seller 1\n", encoding="utf-8")

    with pytest.raises(PipelineError) as exc_info:
        run_pipeline(
            db_path=db_path,
            raw_dir=raw_dir,
            generate_data=False,
            ai_provider=mock_ai,
            raise_on_error=True,
        )

    err_msg = str(exc_info.value)
    assert err_msg.startswith("Stage 'raw_ingestion' failed:")
    assert "sellers.csv" in err_msg


def test_e2e_raise_on_error_false_returns_failure_result(e2e_env):
    """Test 4c: raise_on_error=False does not raise and returns structured failure result."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    mock_ai = MockAIProvider(responder="summary")

    init_db(db_path, e2e_env["schema_path"])
    corrupt_csv = raw_dir / "sellers.csv"
    corrupt_csv.write_text("bad_col1,bad_col2\nS1,Seller 1\n", encoding="utf-8")

    result = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=False,
        ai_provider=mock_ai,
        raise_on_error=False,
    )

    assert result["status"] == "FAILED"
    assert result["error"] is not None
    assert result["stages"]["raw_ingestion"]["status"] == "FAILED"
    # Dependent downstream stages must not run
    assert "staging" not in result["stages"]
    assert "metrics" not in result["stages"]
    assert "ai_summaries" not in result["stages"]

    # Verify audit failure recorded in pipeline_batches
    with get_db_connection(db_path) as conn:
        conn.row_factory = sqlite3.Row
        failed_batch = conn.execute(
            "SELECT status, error_details FROM pipeline_batches WHERE status = 'FAILED'"
        ).fetchone()
        assert failed_batch is not None
        assert "Header mismatch" in failed_batch["error_details"]


def test_e2e_fail_stage_handled_exactly_once(e2e_env, monkeypatch):
    """Test 4d: Verify fail_stage is invoked exactly once for representative raise_on_error=True failures."""
    import src.main as main_mod

    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    mock_ai = MockAIProvider(responder="summary")

    fail_stage_calls = []
    orig_fail_stage = main_mod.fail_stage

    def spy_fail_stage(*args, **kwargs):
        fail_stage_calls.append((args, kwargs))
        return orig_fail_stage(*args, **kwargs)

    monkeypatch.setattr(main_mod, "fail_stage", spy_fail_stage)

    # 1. Representative failure: Raw Ingestion with corrupted header
    init_db(db_path, e2e_env["schema_path"])
    corrupt_csv = raw_dir / "sellers.csv"
    corrupt_csv.write_text("bad_col1,bad_col2\nS1,Seller 1\n", encoding="utf-8")

    fail_stage_calls.clear()
    with pytest.raises(PipelineError) as exc_info1:
        run_pipeline(
            db_path=db_path,
            raw_dir=raw_dir,
            generate_data=False,
            ai_provider=mock_ai,
            raise_on_error=True,
        )

    assert len(fail_stage_calls) == 1
    assert fail_stage_calls[0][0][1] == "raw_ingestion"
    assert "raw_ingestion" in str(exc_info1.value)

    # 2. Representative failure: Data Generation overwrite protection
    res_gen = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=True,
        overwrite_data=True,
        ai_provider=mock_ai,
    )
    assert res_gen["status"] == "SUCCESS"

    fail_stage_calls.clear()
    with pytest.raises(PipelineError) as exc_info2:
        run_pipeline(
            db_path=db_path,
            raw_dir=raw_dir,
            generate_data=True,
            overwrite_data=False,
            ai_provider=mock_ai,
            raise_on_error=True,
        )

    assert len(fail_stage_calls) == 1
    assert fail_stage_calls[0][0][1] == "data_generation"
    assert "data_generation" in str(exc_info2.value)

    # 3. Representative failure: Staging corruption
    fail_stage_calls.clear()
    with get_db_connection(db_path) as conn:
        conn.execute("DROP TABLE stg_returns")
        conn.execute("CREATE TABLE stg_returns (corrupt_column_only TEXT)")

    with pytest.raises(PipelineError) as exc_info3:
        run_pipeline(
            db_path=db_path,
            raw_dir=raw_dir,
            generate_data=False,
            ai_provider=mock_ai,
            raise_on_error=True,
        )

    assert len(fail_stage_calls) == 1
    assert fail_stage_calls[0][0][1] == "staging"
    assert "staging" in str(exc_info3.value)

    # 4. Representative failure: Metrics corruption
    with get_db_connection(db_path) as conn:
        conn.execute("DROP TABLE stg_returns")
    init_db(db_path, e2e_env["schema_path"])
    res_setup = run_pipeline(db_path=db_path, raw_dir=raw_dir, generate_data=False, ai_provider=mock_ai)
    assert res_setup["status"] == "SUCCESS"

    fail_stage_calls.clear()
    with get_db_connection(db_path) as conn:
        conn.execute("DROP TABLE mart_seller_weekly_metrics")
        conn.execute("CREATE TABLE mart_seller_weekly_metrics (corrupt_column_only TEXT)")

    with pytest.raises(PipelineError) as exc_info4:
        run_pipeline(
            db_path=db_path,
            raw_dir=raw_dir,
            generate_data=False,
            ai_provider=mock_ai,
            raise_on_error=True,
        )

    assert len(fail_stage_calls) == 1
    assert fail_stage_calls[0][0][1] == "metrics"
    assert "metrics" in str(exc_info4.value)


def test_e2e_staging_failure_stops_downstream_stages(e2e_env):
    """Test 5: Staging stage failure halts execution and skips metrics and summaries."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    mock_ai = MockAIProvider(responder="summary")

    # Initialize schema and generate data
    res_gen = run_pipeline(db_path=db_path, raw_dir=raw_dir, generate_data=True, ai_provider=mock_ai)
    assert res_gen["status"] == "SUCCESS"

    # Simulate staging failure contract: corrupt stg_returns table definition (leaf table with no FK references pointing to it)
    with get_db_connection(db_path) as conn:
        conn.execute("DROP TABLE stg_returns")
        conn.execute("CREATE TABLE stg_returns (corrupt_column_only TEXT)")

    # Run pipeline with raise_on_error=False
    result = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=False,
        ai_provider=mock_ai,
        raise_on_error=False,
    )

    assert result["status"] == "FAILED"
    assert result["stages"]["staging"]["status"] == "FAILED"
    assert "Staging error:" in result["error"]
    # Downstream stages must NOT be executed
    assert "metrics" not in result["stages"]
    assert "ai_summaries" not in result["stages"]

    # Verify raise_on_error=True raises PipelineError
    with pytest.raises(PipelineError) as exc_info:
        run_pipeline(
            db_path=db_path,
            raw_dir=raw_dir,
            generate_data=False,
            ai_provider=mock_ai,
            raise_on_error=True,
        )
    assert exc_info.value.args[0].startswith("Stage 'staging' failed:")


def test_e2e_metrics_failure_stops_downstream_stages(e2e_env):
    """Test 6: Metrics stage failure halts execution and skips AI summary generation."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    mock_ai = MockAIProvider(responder="summary")

    # Run complete pipeline to stage data
    res_gen = run_pipeline(db_path=db_path, raw_dir=raw_dir, generate_data=True, ai_provider=mock_ai)
    assert res_gen["status"] == "SUCCESS"

    # Simulate metrics failure: corrupt mart_seller_weekly_metrics table definition
    with get_db_connection(db_path) as conn:
        conn.execute("DROP TABLE mart_seller_weekly_metrics")
        conn.execute("CREATE TABLE mart_seller_weekly_metrics (corrupt_column_only TEXT)")

    result = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=False,
        ai_provider=mock_ai,
        raise_on_error=False,
    )

    assert result["status"] == "FAILED"
    assert result["stages"]["metrics"]["status"] == "FAILED"
    assert "Metrics calculation error:" in result["error"]
    # Downstream summary stage must NOT be executed
    assert "ai_summaries" not in result["stages"]

    # Verify raise_on_error=True raises PipelineError
    with pytest.raises(PipelineError) as exc_info:
        run_pipeline(
            db_path=db_path,
            raw_dir=raw_dir,
            generate_data=False,
            ai_provider=mock_ai,
            raise_on_error=True,
        )
    assert exc_info.value.args[0].startswith("Stage 'metrics' failed:")


def test_e2e_row_level_validation_rejections_do_not_fail_pipeline(e2e_env):
    """Test 7: Row-level validation rejections are auditable outcomes and do NOT fail pipeline."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    mock_ai = MockAIProvider(responder="summary")

    # Generate valid synthetic data first
    res_gen = run_pipeline(db_path=db_path, raw_dir=raw_dir, generate_data=True, ai_provider=mock_ai)
    assert res_gen["status"] == "SUCCESS"

    # Inject an invalid order row (invalid status) into orders.csv
    orders_csv = raw_dir / "orders.csv"
    with open(orders_csv, "a", encoding="utf-8", newline="") as f:
        f.write("ORD_INVALID_999,2026-09-15,UNKNOWN_STATUS\n")

    # Rerun pipeline
    result = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=False,
        ai_provider=mock_ai,
        raise_on_error=True,  # Even with raise_on_error=True, validation rejections should succeed
    )

    assert result["status"] == "SUCCESS"
    assert result["stages"]["staging"]["status"] == "SUCCESS"
    assert result["stages"]["metrics"]["status"] == "SUCCESS"
    assert result["stages"]["ai_summaries"]["status"] == "SUCCESS"

    # Verify rejection was durably audited
    with get_db_connection(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rejection = conn.execute(
            "SELECT * FROM raw_rejections WHERE source_table = 'raw_orders' AND rejection_reason LIKE '%status%'"
        ).fetchone()
        assert rejection is not None
        assert "ORD_INVALID_999" in rejection["raw_record_json"]


def test_e2e_ai_unavailability_uses_deterministic_fallback(e2e_env):
    """Test 8: Optional AI provider failure triggers deterministic fallback without failing run."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    failing_ai = FailingAIProvider()

    result = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=True,
        ai_provider=failing_ai,
    )

    # Pipeline overall should still SUCCEED because AI summary has deterministic fallback
    assert result["status"] == "SUCCESS", f"Expected pipeline success, got error: {result.get('error')}"
    assert result["stages"]["ai_summaries"]["status"] == "SUCCESS"
    assert result["stages"]["ai_summaries"]["updated"] > 0
    assert failing_ai.call_count > 0

    # Verify that deterministic fallback content was saved in DB
    with get_db_connection(db_path) as conn:
        conn.row_factory = sqlite3.Row
        metrics = conn.execute(
            "SELECT seller_id, reporting_week, sales_volume, ai_summary FROM mart_seller_weekly_metrics"
        ).fetchall()
        assert len(metrics) > 0

        for row in metrics:
            summary = row["ai_summary"]
            assert summary is not None
            # Deterministic fallback format: "Seller {seller_id} performance summary for {week}: ..."
            assert "performance summary for" in summary.lower()
            assert "sales volume" in summary.lower()
            assert f"${row['sales_volume']:,.2f}" in summary


def test_e2e_overwrite_protection_prevents_silent_overwrite(e2e_env):
    """Test 9: Existing CSVs cannot be overwritten unless overwrite_data is explicitly True."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]
    mock_ai = MockAIProvider(responder="summary")

    # Initial data generation
    res1 = run_pipeline(db_path=db_path, raw_dir=raw_dir, generate_data=True, ai_provider=mock_ai)
    assert res1["status"] == "SUCCESS"

    # Attempting to run with generate_data=True and overwrite_data=False must fail
    res2 = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=True,
        overwrite_data=False,
        ai_provider=mock_ai,
    )
    assert res2["status"] == "FAILED"
    assert res2["stages"]["data_generation"]["status"] == "FAILED"
    assert "already exists" in res2["error"]

    # Running with overwrite_data=True succeeds
    res3 = run_pipeline(
        db_path=db_path,
        raw_dir=raw_dir,
        generate_data=True,
        overwrite_data=True,
        ai_provider=mock_ai,
    )
    assert res3["status"] == "SUCCESS"
    assert res3["stages"]["data_generation"]["status"] == "SUCCESS"


def test_e2e_cli_entrypoint(e2e_env):
    """Test 10: CLI main() returns exit code 0 on success and 1 on failure."""
    db_path = e2e_env["db_path"]
    raw_dir = e2e_env["raw_dir"]

    # 1. Success run via CLI args
    exit_code = main([
        "--db-path", str(db_path),
        "--raw-dir", str(raw_dir),
        "--generate-data",
    ])
    assert exit_code == 0

    # 2. Run against existing CSVs (no --generate-data) via CLI
    exit_code_repeat = main([
        "--db-path", str(db_path),
        "--raw-dir", str(raw_dir),
    ])
    assert exit_code_repeat == 0

    # 3. Failure run via CLI args (e.g. conflict)
    # Trying to generate data without overwrite when files exist
    exit_code_conflict = main([
        "--db-path", str(db_path),
        "--raw-dir", str(raw_dir),
        "--generate-data",
    ])
    assert exit_code_conflict == 1
