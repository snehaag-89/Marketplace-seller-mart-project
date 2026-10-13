"""Validation helpers for the staging pipeline."""
from datetime import datetime
import math
from typing import Any

def validate_date(date_str: Any) -> bool:
    if not isinstance(date_str, str) or len(date_str) != 10:
        return False
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d") == date_str
    except ValueError:
        return False

def validate_int(val: Any) -> bool:
    try:
        int(val)
        return True
    except (ValueError, TypeError):
        return False

def validate_float(val: Any) -> bool:
    try:
        f = float(val)
        return math.isfinite(f)
    except (ValueError, TypeError):
        return False

def validate_status(status: Any) -> bool:
    return status in {"DELIVERED", "SHIPPED", "CANCELLED", "PENDING"}
