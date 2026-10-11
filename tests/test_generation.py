"""Tests for the synthetic data generation."""
import csv
import pytest
from datetime import datetime
from pathlib import Path
from src.generate_data import main

@pytest.fixture
def test_raw_dir(tmp_path):
    """Provide a temporary directory for raw CSV output."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    return raw_dir

def read_csv(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def test_generate_data_files_exist(test_raw_dir):
    """Test that all expected CSVs are created."""
    main(output_dir=test_raw_dir, seed=1)
    
    expected_files = ["sellers.csv", "products.csv", "orders.csv", "order_items.csv", "returns.csv"]
    for f in expected_files:
        assert (test_raw_dir / f).exists(), f"File {f} is missing."

def test_generate_data_determinism(test_raw_dir):
    """Test that the generator produces identical records for the same seed."""
    dir1 = test_raw_dir / "run1"
    dir2 = test_raw_dir / "run2"
    dir1.mkdir()
    dir2.mkdir()
    
    main(output_dir=dir1, seed=42)
    main(output_dir=dir2, seed=42)
    
    for f in ["sellers.csv", "products.csv", "orders.csv", "order_items.csv", "returns.csv"]:
        content1 = (dir1 / f).read_text(encoding="utf-8")
        content2 = (dir2 / f).read_text(encoding="utf-8")
        assert content1 == content2, f"File {f} is not deterministic."

def test_single_seller_per_order_constraint(test_raw_dir):
    """Test that every order contains items from exactly one seller."""
    main(output_dir=test_raw_dir, seed=123)
    
    products = read_csv(test_raw_dir / "products.csv")
    order_items = read_csv(test_raw_dir / "order_items.csv")
    
    product_seller_map = {p["product_id"]: p["seller_id"] for p in products}
    
    order_sellers = {}
    for item in order_items:
        order_id = item["order_id"]
        seller_id = product_seller_map[item["product_id"]]
        if order_id not in order_sellers:
            order_sellers[order_id] = set()
        order_sellers[order_id].add(seller_id)
        
    for order_id, sellers in order_sellers.items():
        assert len(sellers) == 1, f"Order {order_id} has items from multiple sellers: {sellers}"

def test_foreign_key_references(test_raw_dir):
    """Test that all CSV foreign-key-like references are valid."""
    main(output_dir=test_raw_dir, seed=123)
    
    sellers = {s["seller_id"] for s in read_csv(test_raw_dir / "sellers.csv")}
    products = {p["product_id"] for p in read_csv(test_raw_dir / "products.csv")}
    orders = {o["order_id"] for o in read_csv(test_raw_dir / "orders.csv")}
    
    for p in read_csv(test_raw_dir / "products.csv"):
        assert p["seller_id"] in sellers, f"Invalid seller {p['seller_id']} in product {p['product_id']}"
        
    for oi in read_csv(test_raw_dir / "order_items.csv"):
        assert oi["order_id"] in orders, f"Invalid order {oi['order_id']} in item {oi['order_item_id']}"
        assert oi["product_id"] in products, f"Invalid product {oi['product_id']} in item {oi['order_item_id']}"
        
    returns = read_csv(test_raw_dir / "returns.csv")
    assert len(returns) > 0, "Expected at least one return record."
    for r in returns:
        assert r["order_id"] in orders, f"Invalid order {r['order_id']} in return {r['return_id']}"

def test_data_constraints(test_raw_dir):
    """Test that generated values and dates meet documented constraints."""
    main(output_dir=test_raw_dir, seed=123)
    
    orders = {o["order_id"]: o for o in read_csv(test_raw_dir / "orders.csv")}
    
    valid_statuses = {"DELIVERED", "SHIPPED", "CANCELLED", "PENDING"}
    for o in orders.values():
        assert o["status"] in valid_statuses, f"Invalid status {o['status']}"
        # date should parse correctly and fall in expected range
        dt = datetime.strptime(o["order_date"], "%Y-%m-%d")
        assert datetime(2026, 9, 1) <= dt <= datetime(2026, 10, 1), f"Order date {dt} out of expected range"
        
    for r in read_csv(test_raw_dir / "returns.csv"):
        # return date should be after or equal to order date
        order_date = datetime.strptime(orders[r["order_id"]]["order_date"], "%Y-%m-%d")
        return_date = datetime.strptime(r["return_date"], "%Y-%m-%d")
        assert return_date >= order_date, "Return date cannot be before order date."
        
    for oi in read_csv(test_raw_dir / "order_items.csv"):
        assert int(oi["quantity"]) > 0, "Quantity must be positive."
        assert float(oi["price"]) > 0, "Price must be positive."

def test_exact_headers(test_raw_dir):
    """Verify that CSV headers match exactly what the architecture expects."""
    main(output_dir=test_raw_dir, seed=1)
    
    expected_headers = {
        "sellers.csv": ["seller_id", "seller_name"],
        "products.csv": ["product_id", "seller_id", "product_name"],
        "orders.csv": ["order_id", "order_date", "status"],
        "order_items.csv": ["order_item_id", "order_id", "product_id", "quantity", "price"],
        "returns.csv": ["return_id", "order_id", "return_date"]
    }
    
    for filename, headers in expected_headers.items():
        with open(test_raw_dir / filename, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            actual_headers = next(reader)
            assert actual_headers == headers, f"Headers mismatch in {filename}"

def test_overwrite_protection(test_raw_dir):
    """Verify that the generator refuses to overwrite existing files by default."""
    main(output_dir=test_raw_dir, seed=1)
    # Running again without overwrite=True should raise an error
    with pytest.raises(FileExistsError, match="already exists"):
        main(output_dir=test_raw_dir, seed=2)
    # Running with overwrite=True should succeed
    main(output_dir=test_raw_dir, seed=3, overwrite=True)

def test_preflight_overwrite_protection(test_raw_dir):
    """Verify preflight check prevents partial writes if a later file exists."""
    # Create the 'returns.csv' file beforehand
    (test_raw_dir / "returns.csv").write_text("dummy content", encoding="utf-8")
    
    with pytest.raises(FileExistsError, match="already exists"):
        main(output_dir=test_raw_dir, seed=1)
        
    # Verify earlier files were NOT created
    assert not (test_raw_dir / "sellers.csv").exists(), "sellers.csv should not have been created"
    assert not (test_raw_dir / "products.csv").exists(), "products.csv should not have been created"
