"""Tests for staging pipeline."""
import pytest
import sqlite3
import json
from src.pipeline import run_staging_pipeline
from src.db import init_db, get_db_connection
from src.config import PROJECT_ROOT

SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"

@pytest.fixture
def test_db_path(tmp_path):
    db_path = tmp_path / "test_pipeline.db"
    init_db(db_path, SCHEMA_PATH)
    
    # Insert a dummy batch
    with get_db_connection(db_path) as conn:
        conn.execute("INSERT INTO pipeline_batches (batch_id, filename, status) VALUES ('batch1', 'test', 'COMPLETED')")
    return db_path

def test_pipeline_success(test_db_path):
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Seller 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_products VALUES ('P1', 'S1', 'Product 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_orders VALUES ('O1', '2026-10-10', 'DELIVERED', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_order_items VALUES ('OI1', 'O1', 'P1', '2', '10.5', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_returns VALUES ('R1', 'O1', '2026-10-11', 'batch1', CURRENT_TIMESTAMP)")
        
    run_staging_pipeline(test_db_path)
    
    with get_db_connection(test_db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM stg_sellers").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_order_items").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM raw_rejections").fetchone()[0] == 0

def test_duplicate_primary_ids(test_db_path):
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Seller 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Seller 2', 'batch1', CURRENT_TIMESTAMP)")
        
    run_staging_pipeline(test_db_path)
    
    with get_db_connection(test_db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM stg_sellers").fetchone()[0] == 0
        rejections = conn.execute("SELECT rejection_reason FROM raw_rejections").fetchall()
        assert len(rejections) == 2
        assert "Duplicate primary ID" in rejections[0][0]

def test_single_seller_constraint(test_db_path):
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Seller 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_sellers VALUES ('S2', 'Seller 2', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_products VALUES ('P1', 'S1', 'Product 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_products VALUES ('P2', 'S2', 'Product 2', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_orders VALUES ('O1', '2026-10-10', 'DELIVERED', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_order_items VALUES ('OI1', 'O1', 'P1', '2', '10.5', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_order_items VALUES ('OI2', 'O1', 'P2', '1', '5.0', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_returns VALUES ('R1', 'O1', '2026-10-11', 'batch1', CURRENT_TIMESTAMP)")

    run_staging_pipeline(test_db_path)
    
    with get_db_connection(test_db_path) as conn:
        # Sellers and products are valid and should be staged
        assert conn.execute("SELECT COUNT(*) FROM stg_sellers").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM stg_products").fetchone()[0] == 2
        
        # Order, items, and return should be rejected
        assert conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM stg_order_items").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM stg_returns").fetchone()[0] == 0
        
        rejections = conn.execute("SELECT rejection_reason FROM raw_rejections").fetchall()
        reasons = [r[0] for r in rejections]
        assert len(rejections) == 4
        assert all("single-seller" in r.lower() for r in reasons)

def test_invalid_types_and_missing_refs(test_db_path):
    with get_db_connection(test_db_path) as conn:
        # valid seller
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Seller 1', 'batch1', CURRENT_TIMESTAMP)")
        # product referencing non-existent seller
        conn.execute("INSERT INTO raw_products VALUES ('P1', 'S99', 'Product 1', 'batch1', CURRENT_TIMESTAMP)")
        # invalid order status and date
        conn.execute("INSERT INTO raw_orders VALUES ('O1', 'bad-date', 'UNKNOWN', 'batch1', CURRENT_TIMESTAMP)")
        # item referencing valid product but invalid quantity
        conn.execute("INSERT INTO raw_products VALUES ('P2', 'S1', 'Product 2', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_orders VALUES ('O2', '2026-10-10', 'DELIVERED', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_order_items VALUES ('OI1', 'O2', 'P2', 'bad-qty', '10.5', 'batch1', CURRENT_TIMESTAMP)")

    run_staging_pipeline(test_db_path)
    
    with get_db_connection(test_db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM stg_products").fetchone()[0] == 1 # P2 is valid
        assert conn.execute("SELECT COUNT(*) FROM stg_order_items").fetchone()[0] == 0
        
        reasons = [r[0] for r in conn.execute("SELECT rejection_reason FROM raw_rejections").fetchall()]
        assert any("invalid seller reference" in r.lower() for r in reasons)
        assert any("invalid order_date" in r.lower() for r in reasons)
        assert any("invalid quantity" in r.lower() for r in reasons)

def test_atomic_rollback_on_conflict(test_db_path):
    """Test that a forced database failure triggers atomic rollback while rejections persist."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Seller 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_sellers VALUES ('S2', 'Seller 2', 'batch1', CURRENT_TIMESTAMP)")
        # Insert a seller with invalid identifier to generate a rejection
        conn.execute("INSERT INTO raw_sellers VALUES (' ', 'Invalid Seller', 'batch1', CURRENT_TIMESTAMP)")
        
        # Force a database failure during staging insert via an abort trigger
        conn.execute("CREATE TRIGGER force_staging_failure BEFORE INSERT ON stg_sellers BEGIN SELECT RAISE(ABORT, 'forced failure'); END;")
        
    with pytest.raises(sqlite3.IntegrityError, match="forced failure"):
        run_staging_pipeline(test_db_path)
        
    with get_db_connection(test_db_path) as conn:
        # S1 and S2 should not be staged because the whole transaction rolled back
        assert conn.execute("SELECT COUNT(*) FROM stg_sellers WHERE seller_id = 'S2'").fetchone()[0] == 0
        
        # The invalid record should still have been written to raw_rejections!
        rejections = conn.execute("SELECT rejection_reason, raw_record_json, batch_id FROM raw_rejections").fetchall()
        assert len(rejections) == 1
        assert "Missing required identifier" in rejections[0][0]
        
        # Parse JSON and ensure the exact whitespace identifier is preserved
        raw_json = json.loads(rejections[0][1])
        assert raw_json["seller_id"] == " "
        assert raw_json["seller_name"] == "Invalid Seller"
        assert rejections[0][2] == "batch1"

def test_repeated_staging_runs_succeed(test_db_path):
    """Test re-running staging on unchanged raw data does not raise PK errors and skips unchanged rows."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Seller 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_products VALUES ('P1', 'S1', 'Product 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_orders VALUES ('O1', '2026-10-10', 'DELIVERED', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_order_items VALUES ('OI1', 'O1', 'P1', '2', '10.5', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_returns VALUES ('R1', 'O1', '2026-10-11', 'batch1', CURRENT_TIMESTAMP)")
        
    # First staging run
    run_staging_pipeline(test_db_path)
    with get_db_connection(test_db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM stg_sellers").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_products").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_order_items").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_returns").fetchone()[0] == 1

    # Second staging run on identical data - must be idempotent and succeed without PK conflict
    run_staging_pipeline(test_db_path)
    with get_db_connection(test_db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM stg_sellers").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_products").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_order_items").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_returns").fetchone()[0] == 1

def test_staging_changed_records_update(test_db_path):
    """Test modifying a raw record updates the existing staged record."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Initial Seller', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_orders VALUES ('O1', '2026-10-10', 'SHIPPED', 'batch1', CURRENT_TIMESTAMP)")
        
    run_staging_pipeline(test_db_path)
    with get_db_connection(test_db_path) as conn:
        assert conn.execute("SELECT seller_name FROM stg_sellers WHERE seller_id = 'S1'").fetchone()[0] == "Initial Seller"
        assert conn.execute("SELECT status FROM stg_orders WHERE order_id = 'O1'").fetchone()[0] == "SHIPPED"
        
    # Update raw records
    with get_db_connection(test_db_path) as conn:
        conn.execute("UPDATE raw_sellers SET seller_name = 'Updated Seller Name' WHERE seller_id = 'S1'")
        conn.execute("UPDATE raw_orders SET status = 'DELIVERED' WHERE order_id = 'O1'")
        
    run_staging_pipeline(test_db_path)
    with get_db_connection(test_db_path) as conn:
        # Counts remain 1 (no duplicate rows)
        assert conn.execute("SELECT COUNT(*) FROM stg_sellers").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0] == 1
        # Values updated
        assert conn.execute("SELECT seller_name FROM stg_sellers WHERE seller_id = 'S1'").fetchone()[0] == "Updated Seller Name"
        assert conn.execute("SELECT status FROM stg_orders WHERE order_id = 'O1'").fetchone()[0] == "DELIVERED"

def test_product_seller_change_causes_multi_seller_rejection(test_db_path):
    """Test when an incoming product update changes its seller, any affected order becoming multi-seller is rejected."""
    with get_db_connection(test_db_path) as conn:
        # Step 1: Initial state - S1, S2, P1 (S1), P2 (S1)
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Seller 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_sellers VALUES ('S2', 'Seller 2', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_products VALUES ('P1', 'S1', 'Product 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_products VALUES ('P2', 'S1', 'Product 2', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_orders VALUES ('O1', '2026-10-10', 'PENDING', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_order_items VALUES ('OI1', 'O1', 'P1', '1', '10.0', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_order_items VALUES ('OI2', 'O1', 'P2', '1', '20.0', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_returns VALUES ('R1', 'O1', '2026-10-12', 'batch1', CURRENT_TIMESTAMP)")
        
    # Initial staging pass: O1 is single-seller (both items belong to S1)
    run_staging_pipeline(test_db_path)
    
    with get_db_connection(test_db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0] == 1
        assert conn.execute("SELECT status FROM stg_orders WHERE order_id = 'O1'").fetchone()[0] == "PENDING"
        assert conn.execute("SELECT COUNT(*) FROM stg_order_items").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM stg_returns").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM raw_rejections").fetchone()[0] == 0

    # Step 2: Incremental update - P2 is reassigned to S2 in raw_products!
    # O1 in raw_orders also has an incoming status change to 'DELIVERED'
    with get_db_connection(test_db_path) as conn:
        conn.execute("UPDATE raw_products SET seller_id = 'S2' WHERE product_id = 'P2'")
        conn.execute("UPDATE raw_orders SET status = 'DELIVERED' WHERE order_id = 'O1'")
        
    run_staging_pipeline(test_db_path)
    
    with get_db_connection(test_db_path) as conn:
        # Product P2 assignment remains unchanged in stg_products (preserved as S1)
        assert conn.execute("SELECT seller_id FROM stg_products WHERE product_id = 'P2'").fetchone()[0] == "S1"
        assert conn.execute("SELECT seller_id FROM stg_products WHERE product_id = 'P1'").fetchone()[0] == "S1"
        
        # Existing staged order O1 remains consistent in stg_orders (preserved as PENDING)
        assert conn.execute("SELECT status FROM stg_orders WHERE order_id = 'O1'").fetchone()[0] == "PENDING"
        assert conn.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stg_order_items").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM stg_returns").fetchone()[0] == 1
        
        # Verify all staged items in O1 belong strictly to a single seller (S1)
        staged_sellers = conn.execute("""
            SELECT DISTINCT p.seller_id
            FROM stg_order_items oi
            JOIN stg_products p ON oi.product_id = p.product_id
            WHERE oi.order_id = 'O1'
        """).fetchall()
        assert [s[0] for s in staged_sellers] == ["S1"]
        
        # Rejections are durably audited identifying offending product change and affected order
        rejections = conn.execute("SELECT source_table, rejection_reason, raw_record_json FROM raw_rejections").fetchall()
        assert len(rejections) >= 2
        prod_rejs = [r for r in rejections if r[0] == "raw_products"]
        order_rejs = [r for r in rejections if r[0] == "raw_orders"]
        assert len(prod_rejs) == 1
        assert "single-seller" in prod_rejs[0][1].lower()
        assert "P2" in prod_rejs[0][2]
        assert "S2" in prod_rejs[0][2]
        assert len(order_rejs) == 1
        assert "single-seller" in order_rejs[0][1].lower() or "affected" in order_rejs[0][1].lower()

def test_new_multi_seller_order_rejected(test_db_path):
    """Test that a new, not-yet-staged order containing products from multiple sellers is rejected."""
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Seller 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_sellers VALUES ('S2', 'Seller 2', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_products VALUES ('P1', 'S1', 'Product 1', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_products VALUES ('P2', 'S2', 'Product 2', 'batch1', CURRENT_TIMESTAMP)")
        # New not-yet-staged order O2 has items with both P1 (S1) and P2 (S2)
        conn.execute("INSERT INTO raw_orders VALUES ('O2', '2026-10-15', 'DELIVERED', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_order_items VALUES ('OI3', 'O2', 'P1', '1', '15.0', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_order_items VALUES ('OI4', 'O2', 'P2', '2', '25.0', 'batch1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_returns VALUES ('R2', 'O2', '2026-10-16', 'batch1', CURRENT_TIMESTAMP)")

    run_staging_pipeline(test_db_path)

    with get_db_connection(test_db_path) as conn:
        # Products and sellers are successfully staged
        assert conn.execute("SELECT COUNT(*) FROM stg_sellers").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM stg_products").fetchone()[0] == 2
        assert conn.execute("SELECT seller_id FROM stg_products WHERE product_id = 'P1'").fetchone()[0] == "S1"
        assert conn.execute("SELECT seller_id FROM stg_products WHERE product_id = 'P2'").fetchone()[0] == "S2"

        # Multi-seller order O2, its items, and returns are completely rejected from staging
        assert conn.execute("SELECT COUNT(*) FROM stg_orders WHERE order_id = 'O2'").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM stg_order_items WHERE order_id = 'O2'").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM stg_returns WHERE order_id = 'O2'").fetchone()[0] == 0

        # Durable rejections exist for the order, both items, and the return
        rejections = conn.execute("SELECT source_table, rejection_reason FROM raw_rejections").fetchall()
        assert len(rejections) == 4
        reasons = [r[1] for r in rejections]
        assert any("Violates single-seller-per-order constraint" in r for r in reasons)
        assert any("Parent order violates single-seller constraint" in r for r in reasons)
