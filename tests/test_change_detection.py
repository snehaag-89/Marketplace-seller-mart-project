"""Tests for deterministic SHA-256 change detection hashing."""
import pytest
from src.hash_utils import compute_row_hash, serialize_record_canonically

def test_deterministic_hash_same_record():
    """Identical records produce the exact same hash."""
    rec1 = {"seller_id": "S001", "seller_name": "Acme Corp"}
    rec2 = {"seller_id": "S001", "seller_name": "Acme Corp"}
    
    assert compute_row_hash(rec1) == compute_row_hash(rec2)

def test_deterministic_hash_key_order_independent():
    """Key ordering in dictionaries does not affect the hash."""
    rec1 = {"seller_id": "S001", "seller_name": "Acme Corp"}
    rec2 = {"seller_name": "Acme Corp", "seller_id": "S001"}
    
    assert compute_row_hash(rec1) == compute_row_hash(rec2)
    assert serialize_record_canonically(rec1) == serialize_record_canonically(rec2)

def test_metadata_keys_excluded():
    """Operational metadata like batch_id and ingested_at are excluded from hash."""
    rec1 = {"seller_id": "S001", "seller_name": "Acme Corp"}
    rec2 = {
        "seller_id": "S001", 
        "seller_name": "Acme Corp", 
        "batch_id": "b-12345", 
        "ingested_at": "2026-10-11 10:00:00"
    }
    
    assert compute_row_hash(rec1) == compute_row_hash(rec2)

def test_different_business_values_different_hash():
    """Changing any business field results in a different hash."""
    rec1 = {"seller_id": "S001", "seller_name": "Acme Corp"}
    rec2 = {"seller_id": "S001", "seller_name": "Acme Corp Updated"}
    rec3 = {"seller_id": "S002", "seller_name": "Acme Corp"}
    
    h1 = compute_row_hash(rec1)
    h2 = compute_row_hash(rec2)
    h3 = compute_row_hash(rec3)
    
    assert h1 != h2
    assert h1 != h3
    assert h2 != h3

def test_null_vs_empty_string_policy():
    """None and empty string are explicitly distinguished in canonical serialization."""
    rec_null = {"seller_id": "S001", "product_name": None}
    rec_empty = {"seller_id": "S001", "product_name": ""}
    
    assert serialize_record_canonically(rec_null) != serialize_record_canonically(rec_empty)
    assert compute_row_hash(rec_null) != compute_row_hash(rec_empty)

def test_string_policy_preserves_case_and_spacing():
    """Business values are not destructively normalized or stripped."""
    rec_lower = {"seller_id": "S001", "seller_name": "acme"}
    rec_upper = {"seller_id": "S001", "seller_name": "ACME"}
    rec_spaced = {"seller_id": "S001", "seller_name": " acme "}
    
    h_lower = compute_row_hash(rec_lower)
    h_upper = compute_row_hash(rec_upper)
    h_spaced = compute_row_hash(rec_spaced)
    
    assert h_lower != h_upper
    assert h_lower != h_spaced
    assert h_upper != h_spaced
