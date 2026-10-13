"""Deterministic SHA-256 hashing utility for incremental change detection.

Policy:
- Operational metadata (batch_id, ingested_at, row_hash, rejection_id, rejected_at) is excluded.
- Stable field ordering: dictionary keys are sorted alphabetically.
- Explicit null policy: None is explicitly serialized as JSON null, distinguishing it from empty string "".
- Explicit string policy: business values are preserved verbatim without destructive case changes or trimming.
- Serialization is compact and deterministic with no extra whitespace.
"""
import hashlib
import json
from typing import Any, Mapping

METADATA_KEYS = frozenset({
    "batch_id",
    "ingested_at",
    "row_hash",
    "rejection_id",
    "rejected_at",
    "created_at",
    "updated_at",
})

def serialize_record_canonically(
    record: Mapping[str, Any], 
    exclude_keys: frozenset[str] = METADATA_KEYS
) -> str:
    """Serialize a record into a canonical, deterministic JSON string."""
    clean_dict = {
        str(k): v
        for k, v in record.items()
        if k not in exclude_keys
    }
    return json.dumps(clean_dict, sort_keys=True, ensure_ascii=True, separators=(",", ":"))

def compute_row_hash(
    record: Mapping[str, Any], 
    exclude_keys: frozenset[str] = METADATA_KEYS
) -> str:
    """Compute the deterministic SHA-256 hexadecimal hash of a record's business fields."""
    canonical_str = serialize_record_canonically(record, exclude_keys=exclude_keys)
    return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()
