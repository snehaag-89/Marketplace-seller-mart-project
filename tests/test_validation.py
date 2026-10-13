"""Tests for staging validation logic."""
from src.validation import validate_date, validate_int, validate_float, validate_status

def test_validate_date():
    assert validate_date("2026-10-10") is True
    assert validate_date("2024-02-29") is True
    assert validate_date("2026-15-10") is False
    assert validate_date("2026-1-1") is False
    assert validate_date("2026- 1-01") is False # length 10, parses but formats differently
    assert validate_date("invalid") is False
    assert validate_date(None) is False

def test_validate_int():
    assert validate_int("10") is True
    assert validate_int("10.5") is False
    assert validate_int("invalid") is False

def test_validate_float():
    assert validate_float("10.5") is True
    assert validate_float("10") is True
    assert validate_float("NaN") is False
    assert validate_float("inf") is False
    assert validate_float("-inf") is False
    assert validate_float(float("nan")) is False
    assert validate_float(float("inf")) is False
    assert validate_float(float("-inf")) is False
    assert validate_float("invalid") is False

def test_validate_status():
    assert validate_status("DELIVERED") is True
    assert validate_status("SHIPPED") is True
    assert validate_status("PENDING") is True
    assert validate_status("CANCELLED") is True
    assert validate_status("delivered") is False
    assert validate_status("UNKNOWN") is False
    assert validate_status(None) is False
