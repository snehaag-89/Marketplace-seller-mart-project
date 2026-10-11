"""Tests for the database schema and initialization."""
import pytest
import sqlite3
from pathlib import Path
from src.db import init_db, get_db_connection
from src.config import PROJECT_ROOT

SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"

@pytest.fixture
def test_db_path(tmp_path):
    """Provide a temporary database path for testing."""
    return tmp_path / "test_marketplace.db"

def test_schema_initialization(test_db_path):
    """Test that schema initialization succeeds and creates expected tables."""
    init_db(test_db_path, SCHEMA_PATH)
    
    with get_db_connection(test_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        
        expected_tables = {
            "pipeline_batches", "raw_rejections",
            "raw_sellers", "raw_products", "raw_orders", "raw_order_items", "raw_returns",
            "stg_sellers", "stg_products", "stg_orders", "stg_order_items", "stg_returns",
            "mart_seller_weekly_metrics"
        }
        
        assert expected_tables.issubset(tables), f"Missing tables: {expected_tables - tables}"

def test_schema_idempotency(test_db_path):
    """Test that running initialization twice does not fail or erase data."""
    init_db(test_db_path, SCHEMA_PATH)
    
    # Insert some dummy data to verify it survives re-initialization
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers (seller_id, seller_name) VALUES ('S1', 'Test Seller')")
        # Transaction is automatically committed on exit
        
    # Re-initialize
    init_db(test_db_path, SCHEMA_PATH)
    
    # Verify data still exists
    with get_db_connection(test_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT seller_name FROM stg_sellers WHERE seller_id='S1'")
        result = cursor.fetchone()
        assert result is not None
        assert result[0] == "Test Seller"

def test_foreign_key_enforcement(test_db_path):
    """Test that foreign keys are enforced by the connection."""
    init_db(test_db_path, SCHEMA_PATH)
    
    # The IntegrityError should propagate up out of the context manager
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY constraint failed"):
        with get_db_connection(test_db_path) as conn:
            conn.execute("INSERT INTO stg_products (product_id, seller_id, product_name) VALUES ('P1', 'INVALID_SELLER', 'Test Product')")

def test_single_seller_constraint_documentation():
    """
    Verify that the single-seller-per-order constraint is acknowledged as belonging to the pipeline.
    SQLite foreign keys cannot inherently constrain that all order_items for an order_id 
    map to the same seller_id without complex triggers. 
    As per architecture, this is enforced in Python pipeline validation.
    """
    # This test serves as executable documentation that we didn't silently assume FKs handle it.
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema = f.read()
        assert "Single-Seller Constraint" in schema
        assert "enforced via pipeline validation" in schema

def test_transaction_commit(test_db_path):
    """Test that data inserted inside a successful block persists."""
    init_db(test_db_path, SCHEMA_PATH)
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO stg_sellers (seller_id, seller_name) VALUES ('T1', 'Commit Seller')")
    
    # Verify outside the block
    with get_db_connection(test_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT seller_name FROM stg_sellers WHERE seller_id='T1'")
        result = cursor.fetchone()
        assert result is not None
        assert result[0] == "Commit Seller"

def test_transaction_rollback_and_exception_propagation(test_db_path):
    """Test that an exception causes rollback and the exception is re-raised."""
    init_db(test_db_path, SCHEMA_PATH)
    
    class DummyError(Exception):
        pass
    
    with pytest.raises(DummyError, match="Force failure"):
        with get_db_connection(test_db_path) as conn:
            conn.execute("INSERT INTO stg_sellers (seller_id, seller_name) VALUES ('T2', 'Rollback Seller')")
            # Force an exception before block exits
            raise DummyError("Force failure")
            
    # Verify data was NOT persisted
    with get_db_connection(test_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT seller_name FROM stg_sellers WHERE seller_id='T2'")
        result = cursor.fetchone()
        assert result is None
