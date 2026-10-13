"""Focused tests for seller-weekly metrics layer."""
import pytest
import sqlite3
from pathlib import Path

from src.db import init_db, get_db_connection
from src.pipeline import run_metrics_pipeline
from src.config import PROJECT_ROOT

SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"

@pytest.fixture
def test_db_path(tmp_path):
    db_path = tmp_path / "test_metrics.db"
    init_db(db_path, SCHEMA_PATH)
    return db_path

def test_ordinary_metrics(test_db_path):
    """Test ordinary metrics calculation for sales volume, return rate, and cancellation rate."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers VALUES ('S1', 'Seller 1')")
        conn.execute("INSERT INTO stg_products VALUES ('P1', 'S1', 'Product 1')")
        conn.execute("INSERT INTO stg_products VALUES ('P2', 'S1', 'Product 2')")

        # O1: DELIVERED, returned (sales 20.0)
        conn.execute("INSERT INTO stg_orders VALUES ('O1', '2026-09-02', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI1', 'O1', 'P1', 2, 10.0)")
        conn.execute("INSERT INTO stg_returns VALUES ('R1', 'O1', '2026-09-05')")

        # O2: DELIVERED, not returned (sales 30.0)
        conn.execute("INSERT INTO stg_orders VALUES ('O2', '2026-09-03', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI2', 'O2', 'P2', 1, 30.0)")

        # O3: CANCELLED (not in sales volume)
        conn.execute("INSERT INTO stg_orders VALUES ('O3', '2026-09-04', 'CANCELLED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI3', 'O3', 'P1', 1, 10.0)")

    count = run_metrics_pipeline(test_db_path)
    assert count == 1

    with get_db_connection(test_db_path) as conn:
        row = conn.execute(
            "SELECT seller_id, reporting_week, sales_volume, order_return_rate, cancellation_rate FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()

        assert row is not None
        assert row[0] == "S1"
        assert row[1] == "2026-W36"
        assert row[2] == 50.0  # 20.0 (O1) + 30.0 (O2)
        assert row[3] == 0.5   # 1 returned eligible / 2 eligible (O1, O2)
        assert row[4] == pytest.approx(1.0 / 3.0)  # 1 cancelled / 3 total (O1, O2, O3)

def test_multiple_items_per_order(test_db_path):
    """Test that multiple order items in an order sum properly without join fan-out."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers VALUES ('S1', 'Seller 1')")
        conn.execute("INSERT INTO stg_products VALUES ('P1', 'S1', 'Product 1')")
        conn.execute("INSERT INTO stg_products VALUES ('P2', 'S1', 'Product 2')")
        conn.execute("INSERT INTO stg_products VALUES ('P3', 'S1', 'Product 3')")

        # O1 with 3 items: 2*10.0 + 3*15.0 + 1*5.5 = 20.0 + 45.0 + 5.5 = 70.5
        conn.execute("INSERT INTO stg_orders VALUES ('O1', '2026-09-02', 'SHIPPED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI1', 'O1', 'P1', 2, 10.0)")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI2', 'O1', 'P2', 3, 15.0)")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI3', 'O1', 'P3', 1, 5.5)")
        conn.execute("INSERT INTO stg_returns VALUES ('R1', 'O1', '2026-09-04')")

    run_metrics_pipeline(test_db_path)

    with get_db_connection(test_db_path) as conn:
        row = conn.execute(
            "SELECT sales_volume, order_return_rate, cancellation_rate FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()
        assert row[0] == 70.5
        assert row[1] == 1.0
        assert row[2] == 0.0

def test_multiple_returns_for_one_order(test_db_path):
    """Test that multiple return records for one order do not inflate return rate or sales volume."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers VALUES ('S1', 'Seller 1')")
        conn.execute("INSERT INTO stg_products VALUES ('P1', 'S1', 'Product 1')")

        # O1: DELIVERED, $100. Has 3 return records!
        conn.execute("INSERT INTO stg_orders VALUES ('O1', '2026-09-02', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI1', 'O1', 'P1', 1, 100.0)")
        conn.execute("INSERT INTO stg_returns VALUES ('R1', 'O1', '2026-09-03')")
        conn.execute("INSERT INTO stg_returns VALUES ('R2', 'O1', '2026-09-04')")
        conn.execute("INSERT INTO stg_returns VALUES ('R3', 'O1', '2026-09-05')")

        # O2: DELIVERED, $50. No return.
        conn.execute("INSERT INTO stg_orders VALUES ('O2', '2026-09-03', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI2', 'O2', 'P1', 1, 50.0)")

    run_metrics_pipeline(test_db_path)

    with get_db_connection(test_db_path) as conn:
        row = conn.execute(
            "SELECT sales_volume, order_return_rate, cancellation_rate FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()
        assert row[0] == 150.0  # 100 + 50, not multiplied by 3 returns
        assert row[1] == 0.5    # 1 returned order / 2 eligible orders, not 3/2
        assert row[2] == 0.0

def test_zero_denominators(test_db_path):
    """Test that zero denominators return NULL without division-by-zero errors."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers VALUES ('S1', 'Seller 1')")
        conn.execute("INSERT INTO stg_sellers VALUES ('S2', 'Seller 2')")
        conn.execute("INSERT INTO stg_sellers VALUES ('S3', 'Seller 3')")

        conn.execute("INSERT INTO stg_products VALUES ('P1', 'S1', 'Product 1')")
        conn.execute("INSERT INTO stg_products VALUES ('P2', 'S2', 'Product 2')")
        conn.execute("INSERT INTO stg_products VALUES ('P3', 'S3', 'Product 3')")

        # S1 has only CANCELLED orders in W36:
        # Eligible orders = 0 -> return rate denominator is zero -> return rate must be NULL
        # Total orders = 1 -> cancellation rate = 1.0
        conn.execute("INSERT INTO stg_orders VALUES ('O1', '2026-09-02', 'CANCELLED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI1', 'O1', 'P1', 1, 20.0)")

        # S2 has only PENDING orders in W36:
        # Eligible orders = 0 -> return rate denominator is zero -> return rate must be NULL
        # Total orders = 1 -> cancellation rate = 0.0
        conn.execute("INSERT INTO stg_orders VALUES ('O2', '2026-09-03', 'PENDING')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI2', 'O2', 'P2', 1, 30.0)")

        # S3 has zero orders:
        # Total orders = 0 -> cancellation rate denominator is zero -> cancellation rate must be NULL
        # Eligible orders = 0 -> return rate denominator is zero -> return rate must be NULL

    run_metrics_pipeline(test_db_path)

    with get_db_connection(test_db_path) as conn:
        # S1
        row_s1 = conn.execute(
            "SELECT sales_volume, order_return_rate, cancellation_rate FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()
        assert row_s1[0] == 0.0
        assert row_s1[1] is None  # Zero eligible orders -> NULL
        assert row_s1[2] == 1.0

        # S2
        row_s2 = conn.execute(
            "SELECT sales_volume, order_return_rate, cancellation_rate FROM mart_seller_weekly_metrics WHERE seller_id = 'S2'"
        ).fetchone()
        assert row_s2[0] == 0.0
        assert row_s2[1] is None  # Zero eligible orders -> NULL
        assert row_s2[2] == 0.0

        # S3 (zero orders)
        row_s3 = conn.execute(
            "SELECT sales_volume, order_return_rate, cancellation_rate FROM mart_seller_weekly_metrics WHERE seller_id = 'S3'"
        ).fetchone()
        assert row_s3[0] == 0.0
        assert row_s3[1] is None  # Zero eligible orders -> NULL
        assert row_s3[2] is None  # Zero total orders -> NULL

def test_week_boundaries(test_db_path):
    """Test that ISO weeks properly begin on Monday and end on Sunday."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers VALUES ('S1', 'Seller 1')")
        conn.execute("INSERT INTO stg_products VALUES ('P1', 'S1', 'Product 1')")

        # 2026-08-31 is Monday (start of Week 36)
        conn.execute("INSERT INTO stg_orders VALUES ('O1', '2026-08-31', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI1', 'O1', 'P1', 1, 10.0)")

        # 2026-09-06 is Sunday (end of Week 36)
        conn.execute("INSERT INTO stg_orders VALUES ('O2', '2026-09-06', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI2', 'O2', 'P1', 1, 20.0)")

        # 2026-09-07 is Monday (start of Week 37)
        conn.execute("INSERT INTO stg_orders VALUES ('O3', '2026-09-07', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI3', 'O3', 'P1', 1, 30.0)")

    run_metrics_pipeline(test_db_path)

    with get_db_connection(test_db_path) as conn:
        rows = conn.execute(
            "SELECT reporting_week, sales_volume FROM mart_seller_weekly_metrics WHERE seller_id = 'S1' ORDER BY reporting_week"
        ).fetchall()

        assert len(rows) == 2
        # Week 36 includes Monday O1 ($10) and Sunday O2 ($20) -> $30
        assert rows[0][0] == "2026-W36"
        assert rows[0][1] == 30.0

        # Week 37 includes Monday O3 ($30)
        assert rows[1][0] == "2026-W37"
        assert rows[1][1] == 30.0

def test_late_return_updates_original_order_week(test_db_path):
    """Test that a return arriving weeks later updates the original order-date week cohort."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers VALUES ('S1', 'Seller 1')")
        conn.execute("INSERT INTO stg_products VALUES ('P1', 'S1', 'Product 1')")

        # O1 placed in Week 36 (2026-08-31), delivered, no return initially
        conn.execute("INSERT INTO stg_orders VALUES ('O1', '2026-08-31', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI1', 'O1', 'P1', 1, 100.0)")

    # First run: return rate is 0.0
    run_metrics_pipeline(test_db_path)

    with get_db_connection(test_db_path) as conn:
        row = conn.execute(
            "SELECT reporting_week, order_return_rate FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()
        assert row[0] == "2026-W36"
        assert row[1] == 0.0

    # Late return arrives with return_date in Week 39 (2026-09-25)
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_returns VALUES ('R1', 'O1', '2026-09-25')")

    # Re-run metrics: Week 36 return rate is updated to 1.0!
    run_metrics_pipeline(test_db_path)

    with get_db_connection(test_db_path) as conn:
        rows = conn.execute(
            "SELECT reporting_week, order_return_rate FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchall()
        # Exactly 1 week remains (no Week 39 created)
        assert len(rows) == 1
        assert rows[0][0] == "2026-W36"
        assert rows[0][1] == 1.0

def test_idempotent_rerun(test_db_path):
    """Test that re-running metrics is idempotent and preserves existing row metadata."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers VALUES ('S1', 'Seller 1')")
        conn.execute("INSERT INTO stg_products VALUES ('P1', 'S1', 'Product 1')")
        conn.execute("INSERT INTO stg_orders VALUES ('O1', '2026-08-31', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI1', 'O1', 'P1', 2, 25.0)")

    # Run 1
    run_metrics_pipeline(test_db_path)
    with get_db_connection(test_db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM mart_seller_weekly_metrics").fetchone()[0] == 1
        # Set an ai_summary
        conn.execute("UPDATE mart_seller_weekly_metrics SET ai_summary = 'Deterministic summary' WHERE seller_id = 'S1'")

    # Run 2
    run_metrics_pipeline(test_db_path)
    with get_db_connection(test_db_path) as conn:
        rows = conn.execute("SELECT seller_id, reporting_week, sales_volume, ai_summary FROM mart_seller_weekly_metrics").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "S1"
        assert rows[0][1] == "2026-W36"
        assert rows[0][2] == 50.0
        # ai_summary was preserved across re-run!
        assert rows[0][3] == "Deterministic summary"

def test_order_without_items_not_attributed_to_seller(test_db_path):
    """Test that staged orders lacking items have no determinable seller and do not corrupt seller metrics."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers VALUES ('S1', 'Seller 1')")
        conn.execute("INSERT INTO stg_sellers VALUES ('S2', 'Seller 2')")
        conn.execute("INSERT INTO stg_products VALUES ('P1', 'S1', 'Product 1')")

        # O1: Delivered order for S1 with items (sales 50.0)
        conn.execute("INSERT INTO stg_orders VALUES ('O1', '2026-09-02', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI1', 'O1', 'P1', 1, 50.0)")

        # O_empty: Staged order without items, status CANCELLED
        # It is valid in stg_orders per schema, but has no items in stg_order_items
        conn.execute("INSERT INTO stg_orders VALUES ('O_empty', '2026-09-03', 'CANCELLED')")

    run_metrics_pipeline(test_db_path)

    with get_db_connection(test_db_path) as conn:
        # S1 metrics: only O1 belongs to S1 (1 delivered, 0 cancelled).
        # O_empty is NOT attributed to S1 (cancellation rate is 0/1 = 0.0, NOT 1/2 = 0.5)
        row_s1 = conn.execute(
            "SELECT sales_volume, order_return_rate, cancellation_rate FROM mart_seller_weekly_metrics WHERE seller_id = 'S1'"
        ).fetchone()
        assert row_s1 is not None
        assert row_s1[0] == 50.0
        assert row_s1[1] == 0.0
        assert row_s1[2] == 0.0  # 0 cancelled / 1 total attributed to S1

        # S2 metrics: S2 had 0 attributed orders. O_empty is NOT attributed to S2 either.
        row_s2 = conn.execute(
            "SELECT sales_volume, order_return_rate, cancellation_rate FROM mart_seller_weekly_metrics WHERE seller_id = 'S2'"
        ).fetchone()
        assert row_s2 is not None
        assert row_s2[0] == 0.0
        assert row_s2[1] is None  # Zero denominator -> NULL
        assert row_s2[2] is None  # Zero denominator -> NULL

        # Integrity check: no row with NULL seller_id was inserted
        null_seller_count = conn.execute(
            "SELECT COUNT(*) FROM mart_seller_weekly_metrics WHERE seller_id IS NULL"
        ).fetchone()[0]
        assert null_seller_count == 0

def test_stale_seller_week_removed_on_order_date_change(test_db_path):
    """Test that moving an order's date to a new week removes stale rows for the previous week when no orders remain."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers VALUES ('S1', 'Seller 1')")
        conn.execute("INSERT INTO stg_sellers VALUES ('S2', 'Seller 2')")
        conn.execute("INSERT INTO stg_products VALUES ('P1', 'S1', 'Product 1')")

        # O1 in Week 36 (2026-08-31 is Monday of Week 36)
        conn.execute("INSERT INTO stg_orders VALUES ('O1', '2026-08-31', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI1', 'O1', 'P1', 1, 100.0)")

    # Run 1: Week 36 populated
    run_metrics_pipeline(test_db_path)

    with get_db_connection(test_db_path) as conn:
        rows_w36 = conn.execute(
            "SELECT seller_id, reporting_week, sales_volume FROM mart_seller_weekly_metrics WHERE reporting_week = '2026-W36'"
        ).fetchall()
        assert len(rows_w36) == 2  # S1 and S2 (S2 has 0 sales)
        s1_w36 = [r for r in rows_w36 if r[0] == 'S1'][0]
        assert s1_w36[2] == 100.0

    # Move order date to Week 37 (2026-09-07 is Monday of Week 37)
    with get_db_connection(test_db_path) as conn:
        conn.execute("UPDATE stg_orders SET order_date = '2026-09-07' WHERE order_id = 'O1'")

    # Run 2: Re-run metrics pipeline
    run_metrics_pipeline(test_db_path)

    with get_db_connection(test_db_path) as conn:
        # Week 36 must NOT retain any stale rows since no attributable orders remain in Week 36
        w36_count = conn.execute(
            "SELECT COUNT(*) FROM mart_seller_weekly_metrics WHERE reporting_week = '2026-W36'"
        ).fetchone()[0]
        assert w36_count == 0

        # Week 37 must now contain the active rows
        rows_w37 = conn.execute(
            "SELECT seller_id, reporting_week, sales_volume FROM mart_seller_weekly_metrics WHERE reporting_week = '2026-W37'"
        ).fetchall()
        assert len(rows_w37) == 2
        s1_w37 = [r for r in rows_w37 if r[0] == 'S1'][0]
        assert s1_w37[2] == 100.0

def test_metrics_pipeline_failure_rolls_back_atomically(test_db_path, monkeypatch):
    """Test that if the metrics transformation fails midway, changes roll back and do not leave mart partially updated."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers VALUES ('S1', 'Seller 1')")
        conn.execute("INSERT INTO stg_products VALUES ('P1', 'S1', 'Product 1')")
        conn.execute("INSERT INTO stg_orders VALUES ('O1', '2026-08-31', 'DELIVERED')")
        conn.execute("INSERT INTO stg_order_items VALUES ('OI1', 'O1', 'P1', 1, 100.0)")

    # Initial successful run
    run_metrics_pipeline(test_db_path)

    with get_db_connection(test_db_path) as conn:
        initial_rows = conn.execute("SELECT * FROM mart_seller_weekly_metrics").fetchall()
        assert len(initial_rows) == 1

    # Simulate a transformations script that prunes the mart and then fails on invalid syntax in statement 2
    failing_sql = """
    DELETE FROM mart_seller_weekly_metrics;
    INSERT INTO mart_seller_weekly_metrics_invalid_table_does_not_exist VALUES ('bad');
    """

    import src.pipeline as pipeline_mod
    monkeypatch.setattr(pipeline_mod, "PROJECT_ROOT", test_db_path.parent)
    monkeypatch.setattr(pipeline_mod, "METRICS_SQL_FALLBACK", failing_sql)

    # Attempt running metrics with the failing SQL
    with pytest.raises(sqlite3.OperationalError):
        run_metrics_pipeline(test_db_path)

    # Verify that the initial row was NOT deleted; the entire transaction rolled back
    with get_db_connection(test_db_path) as conn:
        post_fail_rows = conn.execute("SELECT * FROM mart_seller_weekly_metrics").fetchall()
        assert len(post_fail_rows) == 1
        assert post_fail_rows == initial_rows


