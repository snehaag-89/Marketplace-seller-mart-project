"""Phase 4: Raw CSV Ingestion."""
import csv
import json
import uuid
from pathlib import Path
from typing import Optional

from src.db import get_db_connection
from src.config import DB_PATH, RAW_DIR
from src.hash_utils import compute_row_hash

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

PRIMARY_KEY_MAP = {
    "sellers.csv": "seller_id",
    "products.csv": "product_id",
    "orders.csv": "order_id",
    "order_items.csv": "order_item_id",
    "returns.csv": "return_id"
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
            
            pk_col = PRIMARY_KEY_MAP[filename]
            pk_idx = headers.index(pk_col)
            sized_rows = []
            try:
                for row in reader:
                    if len(row) != len(headers):
                        # For rejection serialization, map existing cols safely or dump list
                        row_dict = dict(zip(headers, row)) if len(row) <= len(headers) else {"raw_row": row}
                        rejected_rows.append((json.dumps(row_dict), "Invalid field count"))
                        continue
                    sized_rows.append(tuple(row))
            except csv.Error as e:
                # If CSV parsing fails mid-stream, validate rows read before the error so earlier invalid rows survive
                _, pre_rejected = _validate_sized_rows(headers, filename, sized_rows)
                rejected_rows.extend(pre_rejected)
                _write_rejections_to_db(db_path, batch_id, filename, rejected_rows)
                _update_batch_failed(db_path, batch_id, f"CSV parsing error: {str(e)}")
                return {"batch_id": batch_id, "accepted": 0, "rejected": len(rejected_rows), "status": "FAILED"}
            except Exception as e:
                _, pre_rejected = _validate_sized_rows(headers, filename, sized_rows)
                rejected_rows.extend(pre_rejected)
                _write_rejections_to_db(db_path, batch_id, filename, rejected_rows)
                _update_batch_failed(db_path, batch_id, f"File read error: {str(e)}")
                return {"batch_id": batch_id, "accepted": 0, "rejected": len(rejected_rows), "status": "FAILED"}

            valid_rows, new_rejected = _validate_sized_rows(headers, filename, sized_rows)
            rejected_rows.extend(new_rejected)
    except Exception as e:
        _write_rejections_to_db(db_path, batch_id, filename, rejected_rows)
        _update_batch_failed(db_path, batch_id, f"File read error: {str(e)}")
        return {"batch_id": batch_id, "accepted": 0, "rejected": len(rejected_rows), "status": "FAILED"}

    # 2. Audit Connection: Insert rejections independently
    _write_rejections_to_db(db_path, batch_id, filename, rejected_rows)

    # 3. Business Connection: Idempotent insert/update of valid rows with SHA-256 change detection
    insert_count = 0
    update_count = 0
    unchanged_count = 0
    legacy_rejections = []

    if valid_rows:
        table_name = TABLE_MAP[filename]
        pk_col = PRIMARY_KEY_MAP[filename]
        
        try:
            with get_db_connection(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT {', '.join(expected_headers)} FROM {table_name}")
                existing_rows = cursor.fetchall()
                
                existing_by_pk = {}
                for er in existing_rows:
                    er_dict = dict(zip(expected_headers, er))
                    er_pk = er_dict[pk_col]
                    existing_by_pk.setdefault(er_pk, []).append((er_dict, compute_row_hash(er_dict)))
                
                insert_rows = []
                update_rows = []
                
                for row in valid_rows:
                    row_dict = dict(zip(expected_headers, row))
                    pk_val = row_dict[pk_col]
                    incoming_hash = compute_row_hash(row_dict)
                    
                    if pk_val in existing_by_pk:
                        existing_entries = existing_by_pk[pk_val]
                        if len(existing_entries) > 1:
                            # Issue 2: Detect legacy duplicates in raw table
                            # Reject the incoming row explicitly and audibly without silently picking one
                            # or silently deleting legacy data.
                            legacy_rejections.append((json.dumps(row_dict), f"Legacy duplicate records in {table_name} for primary identifier '{pk_val}'"))
                            continue
                            
                        matching = [entry for entry in existing_entries if entry[1] == incoming_hash]
                        if matching:
                            # Identical record: bypass without duplicate or unnecessary write
                            unchanged_count += 1
                        else:
                            # Changed record: update existing raw record
                            update_params = [row_dict[c] for c in expected_headers if c != pk_col] + [batch_id, pk_val]
                            update_rows.append(update_params)
                            existing_by_pk[pk_val] = [(row_dict, incoming_hash)]
                            update_count += 1
                    else:
                        # New record: insert into raw table
                        insert_params = tuple(row_dict[c] for c in expected_headers) + (batch_id,)
                        insert_rows.append(insert_params)
                        existing_by_pk[pk_val] = [(row_dict, incoming_hash)]
                        insert_count += 1
                        
                # Persist legacy duplicate rejections via independent audit transaction BEFORE business writes
                if legacy_rejections:
                    _write_rejections_to_db(db_path, batch_id, filename, legacy_rejections)

                if insert_rows:
                    placeholders = ",".join(["?"] * (len(expected_headers) + 1))
                    columns = ",".join(expected_headers + ["batch_id"])
                    conn.executemany(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", insert_rows)
                    
                if update_rows:
                    set_clause = ", ".join([f"{c} = ?" for c in expected_headers if c != pk_col] + ["batch_id = ?", "ingested_at = CURRENT_TIMESTAMP"])
                    conn.executemany(f"UPDATE {table_name} SET {set_clause} WHERE {pk_col} = ?", update_rows)
                    
        except Exception as e:
            # If the business transaction fails, the audit logs (batch STARTED and rejections) still survive.
            total_rejected = len(rejected_rows) + len(legacy_rejections)
            _update_batch_failed(db_path, batch_id, f"Database insert error: {str(e)}")
            return {"batch_id": batch_id, "accepted": 0, "rejected": total_rejected, "status": "FAILED"}

    # 4. Audit Connection: Mark completion
    with get_db_connection(db_path) as audit_conn:
        audit_conn.execute(
            "UPDATE pipeline_batches SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP WHERE batch_id = ?",
            (batch_id,)
        )
        
    total_rejected = len(rejected_rows) + len(legacy_rejections)
    accepted_count = len(valid_rows) - len(legacy_rejections)
    return {
        "batch_id": batch_id, 
        "accepted": accepted_count, 
        "inserted": insert_count,
        "updated": update_count,
        "unchanged": unchanged_count,
        "rejected": total_rejected, 
        "status": "COMPLETED"
    }

def _validate_sized_rows(headers: list[str], filename: str, sized_rows: list[tuple]) -> tuple[list[tuple], list[tuple[str, str]]]:
    """Validate sized rows, detecting duplicate primary IDs across all rows before checking required identifiers."""
    from collections import Counter
    pk_col = PRIMARY_KEY_MAP[filename]
    pk_idx = headers.index(pk_col)
    req_cols = REQUIRED_IDENTIFIERS.get(filename, [])
    req_indices = [headers.index(c) for c in req_cols if c in headers]

    pk_counts = Counter(r[pk_idx].strip() for r in sized_rows if r[pk_idx] and r[pk_idx].strip())
    duplicate_pks = {pk for pk, count in pk_counts.items() if count > 1}

    valid_rows = []
    rejected_rows = []

    for row in sized_rows:
        row_dict = dict(zip(headers, row))
        raw_pk = row[pk_idx]
        clean_pk = raw_pk.strip() if raw_pk else ""

        # If row participates in duplicate primary ID group, reject regardless of other fields
        if clean_pk and clean_pk in duplicate_pks:
            rejected_rows.append((json.dumps(row_dict), f"Duplicate primary ID '{clean_pk}' in incoming CSV"))
            continue

        # Check for missing/whitespace required identifiers
        missing_id = False
        for idx in req_indices:
            if idx >= len(row) or not row[idx] or row[idx].strip() == "":
                missing_id = True
                break

        if missing_id:
            rejected_rows.append((json.dumps(row_dict), "Missing required identifier"))
            continue

        valid_rows.append(row)

    return valid_rows, rejected_rows

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
