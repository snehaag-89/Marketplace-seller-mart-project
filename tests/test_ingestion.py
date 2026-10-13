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
    """Test re-ingesting a file creates a new auditable batch while avoiding duplicate raw rows."""
    res1 = ingest_file(test_db_path, test_raw_dir / "sellers.csv")
    res2 = ingest_file(test_db_path, test_raw_dir / "sellers.csv")
    
    assert res1["batch_id"] != res2["batch_id"]
    with get_db_connection(test_db_path) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM pipeline_batches WHERE filename = 'sellers.csv'")
        assert cur.fetchone()[0] == 2
        
        # Deduplication via SHA-256 prevents duplicate raw business rows
        cur = conn.execute("SELECT COUNT(*) FROM raw_sellers")
        assert cur.fetchone()[0] == 1

def test_reingest_identical_records_skipped(test_db_path, test_raw_dir):
    """Test identical records are detected and skipped without unnecessary writes."""
    res1 = ingest_file(test_db_path, test_raw_dir / "sellers.csv")
    assert res1["inserted"] == 1
    assert res1["unchanged"] == 0
    
    res2 = ingest_file(test_db_path, test_raw_dir / "sellers.csv")
    assert res2["inserted"] == 0
    assert res2["updated"] == 0
    assert res2["unchanged"] == 1
    
    with get_db_connection(test_db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM raw_sellers").fetchone()[0] == 1

def test_reingest_changed_record_updates_existing(test_db_path, tmp_path):
    """Test modifying a record updates the existing raw row rather than creating a duplicate."""
    raw_csv = tmp_path / "sellers.csv"
    raw_csv.write_text("seller_id,seller_name\nS1,Original Seller\n")
    
    res1 = ingest_file(test_db_path, raw_csv)
    assert res1["inserted"] == 1
    assert res1["updated"] == 0
    
    with get_db_connection(test_db_path) as conn:
        row = conn.execute("SELECT seller_name, batch_id FROM raw_sellers WHERE seller_id = 'S1'").fetchone()
        assert row[0] == "Original Seller"
        assert row[1] == res1["batch_id"]
        
    # Update CSV content with same business identifier
    raw_csv.write_text("seller_id,seller_name\nS1,Updated Seller Name\n")
    res2 = ingest_file(test_db_path, raw_csv)
    assert res2["inserted"] == 0
    assert res2["updated"] == 1
    assert res2["unchanged"] == 0
    
    with get_db_connection(test_db_path) as conn:
        # Exactly 1 record remains in raw table (no duplicate created)
        assert conn.execute("SELECT COUNT(*) FROM raw_sellers").fetchone()[0] == 1
        row = conn.execute("SELECT seller_name, batch_id FROM raw_sellers WHERE seller_id = 'S1'").fetchone()
        assert row[0] == "Updated Seller Name"
        assert row[1] == res2["batch_id"]

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

def test_ingest_duplicate_ids_identical_values_rejected(test_db_path, tmp_path):
    """Test repeated primary identifiers with identical values in an incoming CSV are all rejected."""
    csv_path = tmp_path / "sellers.csv"
    # S1 repeated twice with identical values, S2 is valid and unique
    csv_path.write_text("seller_id,seller_name\nS1,Seller 1\nS1,Seller 1\nS2,Seller 2\n")
    
    res = ingest_file(test_db_path, csv_path)
    assert res["status"] == "COMPLETED"
    assert res["accepted"] == 1  # S2 accepted
    assert res["rejected"] == 2  # Both S1 rows rejected
    
    with get_db_connection(test_db_path) as conn:
        # S1 was not ingested (neither row won)
        assert conn.execute("SELECT COUNT(*) FROM raw_sellers WHERE seller_id = 'S1'").fetchone()[0] == 0
        # S2 was successfully ingested
        assert conn.execute("SELECT COUNT(*) FROM raw_sellers WHERE seller_id = 'S2'").fetchone()[0] == 1
        
        rejections = conn.execute("SELECT rejection_reason, raw_record_json FROM raw_rejections WHERE batch_id = ?", (res["batch_id"],)).fetchall()
        assert len(rejections) == 2
        assert all("Duplicate primary ID" in r[0] for r in rejections)

def test_ingest_duplicate_ids_conflicting_values_rejected(test_db_path, tmp_path):
    """Test repeated primary identifiers with conflicting values in an incoming CSV are all rejected."""
    csv_path = tmp_path / "sellers.csv"
    # S1 repeated twice with conflicting values, S2 is valid and unique
    csv_path.write_text("seller_id,seller_name\nS1,First Name\nS1,Conflicting Name\nS2,Seller 2\n")
    
    res = ingest_file(test_db_path, csv_path)
    assert res["status"] == "COMPLETED"
    assert res["accepted"] == 1  # S2 accepted
    assert res["rejected"] == 2  # Both S1 rows rejected
    
    with get_db_connection(test_db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM raw_sellers WHERE seller_id = 'S1'").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM raw_sellers WHERE seller_id = 'S2'").fetchone()[0] == 1
        
        rejections = conn.execute("SELECT rejection_reason, raw_record_json FROM raw_rejections WHERE batch_id = ?", (res["batch_id"],)).fetchall()
        assert len(rejections) == 2
        raw_jsons = [r[1] for r in rejections]
        assert any("First Name" in j for j in raw_jsons)
        assert any("Conflicting Name" in j for j in raw_jsons)

def test_ingest_with_legacy_duplicate_in_raw(test_db_path, tmp_path):
    """Test handling of legacy duplicate identifiers existing in the target raw table."""
    # Seed legacy duplicate rows for S1 in raw_sellers
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Legacy Seller A', 'legacy_batch_1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Legacy Seller B', 'legacy_batch_2', CURRENT_TIMESTAMP)")
        
    csv_path = tmp_path / "sellers.csv"
    # Incoming CSV has S1 and S2
    csv_path.write_text("seller_id,seller_name\nS1,New Seller S1\nS2,Clean Seller S2\n")
    
    res = ingest_file(test_db_path, csv_path)
    assert res["status"] == "COMPLETED"
    assert res["accepted"] == 1  # S2 accepted
    assert res["rejected"] == 1  # S1 rejected due to ambiguous legacy duplicates
    
    with get_db_connection(test_db_path) as conn:
        # Legacy duplicates are NOT silently updated, deleted, or reduced
        s1_rows = conn.execute("SELECT seller_name FROM raw_sellers WHERE seller_id = 'S1'").fetchall()
        assert len(s1_rows) == 2
        names = {r[0] for r in s1_rows}
        assert names == {"Legacy Seller A", "Legacy Seller B"}
        
        # S2 was successfully inserted
        assert conn.execute("SELECT COUNT(*) FROM raw_sellers WHERE seller_id = 'S2'").fetchone()[0] == 1
        
        # Rejection reason clearly identifies legacy duplicate conflict
        rejections = conn.execute("SELECT rejection_reason, raw_record_json FROM raw_rejections WHERE batch_id = ?", (res["batch_id"],)).fetchall()
        assert len(rejections) == 1
        assert "Legacy duplicate" in rejections[0][0]
        assert "New Seller S1" in rejections[0][1]

def test_ingest_duplicate_product_ids_with_missing_seller_id(test_db_path, tmp_path):
    """Test duplicate product IDs where one row has a missing seller ID and the other is otherwise valid."""
    csv_path = tmp_path / "products.csv"
    # Row 1: P1 with missing seller_id
    # Row 2: P1 with valid seller_id S1
    # Row 3: P2 with valid seller_id S1 (unrelated valid row)
    content = "product_id,seller_id,product_name\nP1,,Product 1 Missing Seller\nP1,S1,Product 1 Valid Seller\nP2,S1,Product 2 Unrelated Valid\n"
    csv_path.write_text(content)
    
    res = ingest_file(test_db_path, csv_path)
    assert res["status"] == "COMPLETED"
    assert res["accepted"] == 1  # P2 accepted
    assert res["rejected"] == 2  # Both P1 rows rejected
    
    with get_db_connection(test_db_path) as conn:
        # P1 was not ingested into raw_products (neither row won)
        assert conn.execute("SELECT COUNT(*) FROM raw_products WHERE product_id = 'P1'").fetchone()[0] == 0
        # P2 was successfully ingested
        assert conn.execute("SELECT COUNT(*) FROM raw_products WHERE product_id = 'P2'").fetchone()[0] == 1
        
        # Both P1 rows have rejection records preserving their original values
        rejections = conn.execute(
            "SELECT rejection_reason, raw_record_json FROM raw_rejections WHERE batch_id = ?",
            (res["batch_id"],)
        ).fetchall()
        assert len(rejections) == 2
        assert all("Duplicate primary ID 'P1'" in r[0] for r in rejections)
        raw_jsons = [json.loads(r[1]) for r in rejections]
        # Row 1 preserves empty seller_id
        assert any(j["product_id"] == "P1" and j["seller_id"] == "" for j in raw_jsons)
        # Row 2 preserves S1 seller_id
        assert any(j["product_id"] == "P1" and j["seller_id"] == "S1" for j in raw_jsons)

def test_legacy_duplicate_audit_durable_on_transaction_failure(test_db_path, tmp_path):
    """Test legacy-duplicate rejection is durably persisted when raw business transaction fails."""
    # Seed legacy duplicate rows for S1 in raw_sellers
    with get_db_connection(test_db_path) as conn:
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Legacy Seller A', 'legacy_batch_1', CURRENT_TIMESTAMP)")
        conn.execute("INSERT INTO raw_sellers VALUES ('S1', 'Legacy Seller B', 'legacy_batch_2', CURRENT_TIMESTAMP)")
        # Create an abort trigger that forces a database failure on inserting valid rows into raw_sellers
        conn.execute("CREATE TRIGGER force_insert_failure BEFORE INSERT ON raw_sellers BEGIN SELECT RAISE(ABORT, 'forced business failure'); END;")
        
    csv_path = tmp_path / "sellers.csv"
    # S1 triggers legacy duplicate detection; S2 is a valid row that triggers the insert abort trigger
    csv_path.write_text("seller_id,seller_name\nS1,New Seller S1\nS2,Clean Seller S2\n")
    
    res = ingest_file(test_db_path, csv_path)
    # Business transaction failed
    assert res["status"] == "FAILED"
    assert res["accepted"] == 0
    assert res["rejected"] == 1  # S1 legacy duplicate rejection
    
    with get_db_connection(test_db_path) as conn:
        # Batch record is FAILED and durably persisted
        batch = conn.execute("SELECT status, error_details FROM pipeline_batches WHERE batch_id = ?", (res["batch_id"],)).fetchone()
        assert batch[0] == "FAILED"
        assert "forced business failure" in batch[1]
        
        # Raw business table was NOT modified (S2 not inserted, S1 legacy duplicates intact)
        assert conn.execute("SELECT COUNT(*) FROM raw_sellers WHERE seller_id = 'S2'").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM raw_sellers WHERE seller_id = 'S1'").fetchone()[0] == 2
        
        # The legacy duplicate rejection for S1 SURVIVED durably in raw_rejections
        rejections = conn.execute(
            "SELECT rejection_reason, raw_record_json, batch_id FROM raw_rejections WHERE batch_id = ?",
            (res["batch_id"],)
        ).fetchall()
        assert len(rejections) == 1
        assert "Legacy duplicate" in rejections[0][0]
        assert "New Seller S1" in rejections[0][1]
        assert rejections[0][2] == res["batch_id"]
