"""Phase 4: Raw CSV Ingestion."""
import csv
import json
import uuid
from pathlib import Path
from typing import Optional

from src.db import get_db_connection
from src.config import DB_PATH, RAW_DIR

EXPECTED_HEADERS = {
    "sellers.csv": ["seller_id", "seller_name"],
    "products.csv": ["product_id", "seller_id", "product_name"],
    "orders.csv": ["order_id", "order_date", "status"],
    "order_items.csv": ["order_item_id", "order_id", "product_id", "quantity", "price"],
    "returns.csv": ["return_id", "order_id", "return_date"]
}

REQUIRED_IDENTIFIERS = {
    "sellers.csv": ["seller_id"],
    "products.csv": ["product_id", "seller_id"],
    "orders.csv": ["order_id"],
    "order_items.csv": ["order_item_id", "order_id", "product_id"],
    "returns.csv": ["return_id", "order_id"]
}

TABLE_MAP = {
    "sellers.csv": "raw_sellers",
    "products.csv": "raw_products",
    "orders.csv": "raw_orders",
    "order_items.csv": "raw_order_items",
    "returns.csv": "raw_returns"
}

def ingest_file(db_path: Path, csv_path: Path) -> dict:
    """Ingest a single CSV file into the raw layer with batch tracking."""
    filename = csv_path.name
    batch_id = str(uuid.uuid4())
    
    # 1. Audit Connection: Create batch record durably
    with get_db_connection(db_path) as audit_conn:
        audit_conn.execute(
            "INSERT INTO pipeline_batches (batch_id, filename, status) VALUES (?, ?, ?)",
            (batch_id, filename, "STARTED")
        )
        
    expected_headers = EXPECTED_HEADERS.get(filename)
    if not expected_headers:
        error_msg = f"Unknown file {filename}"
        _update_batch_failed(db_path, batch_id, error_msg)
        return {"batch_id": batch_id, "accepted": 0, "rejected": 0, "status": "FAILED"}

    if not csv_path.exists():
        error_msg = f"File {csv_path} missing"
        _update_batch_failed(db_path, batch_id, error_msg)
        return {"batch_id": batch_id, "accepted": 0, "rejected": 0, "status": "FAILED"}

    valid_rows = []
    rejected_rows = []
    
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, strict=True)
            try:
                headers = next(reader)
            except StopIteration:
                headers = []
                
            if headers != expected_headers:
                error_msg = f"Header mismatch. Expected {expected_headers}, got {headers}"
                _update_batch_failed(db_path, batch_id, error_msg)
                return {"batch_id": batch_id, "accepted": 0, "rejected": 0, "status": "FAILED"}
                
            req_cols = REQUIRED_IDENTIFIERS.get(filename, [])
            req_indices = [headers.index(c) for c in req_cols if c in headers]
            
            for row in reader:
                if len(row) != len(headers):
                    # For rejection serialization, map existing cols safely or dump list
                    row_dict = dict(zip(headers, row)) if len(row) <= len(headers) else {"raw_row": row}
                    rejected_rows.append((json.dumps(row_dict), "Invalid field count"))
                    continue
                
                # Check for missing/whitespace required identifiers
                missing_id = False
                for idx in req_indices:
                    if idx >= len(row) or not row[idx] or row[idx].strip() == "":
                        missing_id = True
                        break
                
                if missing_id:
                    rejected_rows.append((json.dumps(dict(zip(headers, row))), "Missing required identifier"))
                    continue
                
                valid_rows.append(tuple(row) + (batch_id,))
    except csv.Error as e:
        _write_rejections_to_db(db_path, batch_id, filename, rejected_rows)
        _update_batch_failed(db_path, batch_id, f"CSV parsing error: {str(e)}")
        return {"batch_id": batch_id, "accepted": 0, "rejected": len(rejected_rows), "status": "FAILED"}
    except Exception as e:
        _write_rejections_to_db(db_path, batch_id, filename, rejected_rows)
        _update_batch_failed(db_path, batch_id, f"File read error: {str(e)}")
        return {"batch_id": batch_id, "accepted": 0, "rejected": len(rejected_rows), "status": "FAILED"}

    # 2. Audit Connection: Insert rejections independently
    _write_rejections_to_db(db_path, batch_id, filename, rejected_rows)

    # 3. Business Connection: Atomic insert of valid rows
    if valid_rows:
        table_name = TABLE_MAP[filename]
        placeholders = ",".join(["?"] * (len(expected_headers) + 1))  # +1 for batch_id
        columns = ",".join(expected_headers + ["batch_id"])
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        try:
            with get_db_connection(db_path) as conn:
                conn.executemany(insert_sql, valid_rows)
        except Exception as e:
            # If the business insert fails, the audit logs (batch STARTED and rejections) still survive.
            _update_batch_failed(db_path, batch_id, f"Database insert error: {str(e)}")
            return {"batch_id": batch_id, "accepted": 0, "rejected": len(rejected_rows), "status": "FAILED"}
        
    # 4. Audit Connection: Mark completion
    with get_db_connection(db_path) as audit_conn:
        audit_conn.execute(
            "UPDATE pipeline_batches SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP WHERE batch_id = ?",
            (batch_id,)
        )
        
    return {"batch_id": batch_id, "accepted": len(valid_rows), "rejected": len(rejected_rows), "status": "COMPLETED"}

def _update_batch_failed(db_path: Path, batch_id: str, error_msg: str):
    """Safely update a batch to FAILED status in an independent transaction."""
    with get_db_connection(db_path) as audit_conn:
        audit_conn.execute(
            "UPDATE pipeline_batches SET status = 'FAILED', error_details = ?, completed_at = CURRENT_TIMESTAMP WHERE batch_id = ?",
            (error_msg, batch_id)
        )

def _write_rejections_to_db(db_path: Path, batch_id: str, filename: str, rejected_rows: list[tuple[str, str]]):
    """Safely insert rejection records into the audit database."""
    if not rejected_rows:
        return
    with get_db_connection(db_path) as audit_conn:
        rejection_data = [
            (str(uuid.uuid4()), batch_id, TABLE_MAP[filename], raw_json, reason)
            for raw_json, reason in rejected_rows
        ]
        audit_conn.executemany(
            "INSERT INTO raw_rejections (rejection_id, batch_id, source_table, raw_record_json, rejection_reason) VALUES (?, ?, ?, ?, ?)",
            rejection_data
        )

def ingest_all_files(db_path: Path = DB_PATH, raw_dir: Path = RAW_DIR) -> dict:
    """Ingest the five documented raw files in order."""
    order = ["sellers.csv", "products.csv", "orders.csv", "order_items.csv", "returns.csv"]
    results = {}
    for filename in order:
        csv_path = raw_dir / filename
        res = ingest_file(db_path, csv_path)
        results[filename] = res
    return results

if __name__ == "__main__":
    res = ingest_all_files()
    print(json.dumps(res, indent=2))
