"""Tests for Phase 4: Raw CSV Ingestion."""
import pytest
import json
import sqlite3
import csv
from pathlib import Path

from src.db import init_db, get_db_connection
from src.ingestion import ingest_file, ingest_all_files, EXPECTED_HEADERS
from src.config import PROJECT_ROOT

SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"

@pytest.fixture
def test_db_path(tmp_path):
    db_path = tmp_path / "test_ingestion.db"
    init_db(db_path, SCHEMA_PATH)
    return db_path

@pytest.fixture
def test_raw_dir(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    # Create valid dummy files
    for fname, headers in EXPECTED_HEADERS.items():
        with open(raw_dir / fname, "w", encoding="utf-8") as f:
            f.write(",".join(headers) + "\n")
            # Write one valid row
            f.write(",".join([f"val_{i}" for i in range(len(headers))]) + "\n")
    return raw_dir

def test_ingest_all_files_success(test_db_path, test_raw_dir):
    """Test successful ingestion of all five files."""
    results = ingest_all_files(test_db_path, test_raw_dir)
    assert len(results) == 5
    for fname, res in results.items():
        assert res["status"] == "COMPLETED"
        assert res["accepted"] == 1
        assert res["rejected"] == 0

    with get_db_connection(test_db_path) as conn:
        for table in ["raw_sellers", "raw_products", "raw_orders", "raw_order_items", "raw_returns"]:
            cur = conn.execute(f"SELECT * FROM {table}")
            assert len(cur.fetchall()) == 1
            
def test_reingest_creates_new_batch(test_db_path, test_raw_dir):
    """Test re-ingesting a file creates a new auditable batch."""
    res1 = ingest_file(test_db_path, test_raw_dir / "sellers.csv")
    res2 = ingest_file(test_db_path, test_raw_dir / "sellers.csv")
    
    assert res1["batch_id"] != res2["batch_id"]
    with get_db_connection(test_db_path) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM pipeline_batches WHERE filename = 'sellers.csv'")
        assert cur.fetchone()[0] == 2
        
        cur = conn.execute("SELECT COUNT(*) FROM raw_sellers")
        assert cur.fetchone()[0] == 2  # Deduplication is out of scope

def test_missing_file_failure(test_db_path, tmp_path):
    """Test missing file creates a FAILED batch record."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    res = ingest_file(test_db_path, empty_dir / "sellers.csv")
    assert res["status"] == "FAILED"
    
    with get_db_connection(test_db_path) as conn:
        cur = conn.execute("SELECT status, error_details FROM pipeline_batches WHERE batch_id = ?", (res["batch_id"],))
        batch = cur.fetchone()
        assert batch[0] == "FAILED"
        assert "missing" in batch[1].lower()

def test_malformed_header_failure(test_db_path, tmp_path):
    """Test malformed header is caught and recorded in batch audit."""
    bad_csv = tmp_path / "sellers.csv"
    bad_csv.write_text("bad_col1,bad_col2\nval1,val2")
    res = ingest_file(test_db_path, bad_csv)
    assert res["status"] == "FAILED"
    
    with get_db_connection(test_db_path) as conn:
        cur = conn.execute("SELECT status, error_details FROM pipeline_batches WHERE batch_id = ?", (res["batch_id"],))
        batch = cur.fetchone()
        assert batch[0] == "FAILED"
        assert "mismatch" in batch[1].lower()

def test_invalid_row_rejection(test_db_path, tmp_path):
    """Test that invalid rows are diverted to raw_rejections and valid ones succeed."""
    csv_path = tmp_path / "sellers.csv"
    # 1 valid row, 1 missing column row, 1 missing ID row
    content = "seller_id,seller_name\nS1,Seller 1\nS2\n,Seller 3"
    csv_path.write_text(content)
    
    res = ingest_file(test_db_path, csv_path)
    assert res["status"] == "COMPLETED"
    assert res["accepted"] == 1
    assert res["rejected"] == 2
    
    with get_db_connection(test_db_path) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM raw_sellers")
        assert cur.fetchone()[0] == 1
        
        cur = conn.execute("SELECT rejection_reason, raw_record_json FROM raw_rejections WHERE batch_id = ?", (res["batch_id"],))
        rejections = cur.fetchall()
        assert len(rejections) == 2
        reasons = [r[0] for r in rejections]
        assert any("count" in r.lower() for r in reasons)
        assert any("missing" in r.lower() for r in reasons)

def test_atomic_rollback_on_db_error(test_db_path, tmp_path):
    """Test that a DB error rolls back valid rows but preserves the batch and rejections."""
    csv_path = tmp_path / "sellers.csv"
    # 1 valid row, 1 invalid row
    csv_path.write_text("seller_id,seller_name\nS1,Seller 1\nS2")
    
    # Intentionally corrupt the schema to force a DB error during valid row insert
    with get_db_connection(test_db_path) as conn:
        conn.execute("DROP TABLE raw_sellers")
        
    res = ingest_file(test_db_path, csv_path)
    assert res["status"] == "FAILED"
    assert res["rejected"] == 1
    assert res["accepted"] == 0
    
    with get_db_connection(test_db_path) as conn:
        # Batch record is FAILED but survives
        cur = conn.execute("SELECT status, error_details FROM pipeline_batches WHERE batch_id = ?", (res["batch_id"],))
        batch = cur.fetchone()
        assert batch[0] == "FAILED"
        assert "no such table" in batch[1].lower()
        
        # Rejection record survives
        cur = conn.execute("SELECT COUNT(*) FROM raw_rejections WHERE batch_id = ?", (res["batch_id"],))
        assert cur.fetchone()[0] == 1

def test_missing_product_seller_id(test_db_path, tmp_path):
    csv_path = tmp_path / "products.csv"
    csv_path.write_text("product_id,seller_id,product_name\nP1,,Product 1")
    res = ingest_file(test_db_path, csv_path)
    assert res["rejected"] == 1
    assert res["accepted"] == 0

def test_missing_order_item_ids(test_db_path, tmp_path):
    csv_path = tmp_path / "order_items.csv"
    csv_path.write_text("order_item_id,order_id,product_id,quantity,price\nOI1,,P1,1,10.0")
    res = ingest_file(test_db_path, csv_path)
    assert res["rejected"] == 1
    
def test_missing_product_id(test_db_path, tmp_path):
    csv_path = tmp_path / "order_items.csv"
    csv_path.write_text("order_item_id,order_id,product_id,quantity,price\nOI1,O1,   ,1,10.0")
    res = ingest_file(test_db_path, csv_path)
    assert res["rejected"] == 1
    assert res["accepted"] == 0

def test_missing_return_order_id(test_db_path, tmp_path):
    csv_path = tmp_path / "returns.csv"
    csv_path.write_text("return_id,order_id,return_date\nR1,,2026-09-10")
    res = ingest_file(test_db_path, csv_path)
    assert res["rejected"] == 1

def test_whitespace_identifier(test_db_path, tmp_path):
    csv_path = tmp_path / "sellers.csv"
    csv_path.write_text("seller_id,seller_name\n   ,Seller 1")
    res = ingest_file(test_db_path, csv_path)
    assert res["rejected"] == 1
    assert res["accepted"] == 0

def test_malformed_csv_syntax(test_db_path, tmp_path):
    csv_path = tmp_path / "sellers.csv"
    # Row 1: Valid
    # Row 2: Invalid (missing ID)
    # Row 3: Malformed quote syntax (extra data after quote triggers strict error)
    csv_content = 'seller_id,seller_name\nS1,Seller 1\n,Seller 2\nS3,"Seller 3"extra'
    csv_path.write_text(csv_content)
    
    res = ingest_file(test_db_path, csv_path)
        
    assert res["status"] == "FAILED"
    assert res["accepted"] == 0
    assert res["rejected"] == 1
    
    with get_db_connection(test_db_path) as conn:
        batch = conn.execute("SELECT status, error_details FROM pipeline_batches WHERE batch_id = ?", (res["batch_id"],)).fetchone()
        assert batch[0] == "FAILED"
        assert "parsing error" in batch[1].lower()
        
        # Verify no business rows inserted
        assert conn.execute("SELECT COUNT(*) FROM raw_sellers").fetchone()[0] == 0
        
        # Verify earlier invalid row was preserved in raw_rejections
        rejections = conn.execute("SELECT raw_record_json, batch_id FROM raw_rejections WHERE batch_id = ?", (res["batch_id"],)).fetchall()
        assert len(rejections) == 1
        assert "Seller 2" in rejections[0][0]
        assert rejections[0][1] == res["batch_id"]
        
def test_raw_rejections_fk(test_db_path, tmp_path):
    """Verify that raw_rejections enforces FK constraints to pipeline_batches."""
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY constraint failed"):
        with get_db_connection(test_db_path) as conn:
            conn.execute(
                "INSERT INTO raw_rejections (rejection_id, batch_id, source_table, raw_record_json, rejection_reason) VALUES (?, ?, ?, ?, ?)",
                ("R1", "INVALID_BATCH_ID", "raw_sellers", "{}", "reason")
            )
