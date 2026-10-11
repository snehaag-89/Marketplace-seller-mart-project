"""Module to generate deterministic synthetic data for the Marketplace Seller Mart."""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from src.config import RAW_DIR

def generate_sellers(rng: random.Random, num_sellers: int = 10) -> list[dict]:
    sellers = []
    for i in range(1, num_sellers + 1):
        sellers.append({
            "seller_id": f"S{i:03d}",
            "seller_name": f"Seller_{chr(64 + (i % 26 or 26))}_{i}"
        })
    return sellers

def generate_products(rng: random.Random, sellers: list[dict], products_per_seller: int = 5) -> list[dict]:
    products = []
    pid = 1
    for seller in sellers:
        for _ in range(products_per_seller):
            products.append({
                "product_id": f"P{pid:04d}",
                "seller_id": seller["seller_id"],
                "product_name": f"Product_{pid:04d}"
            })
            pid += 1
    return products

def generate_orders_and_items(
    rng: random.Random, 
    sellers: list[dict], 
    products: list[dict], 
    num_orders: int = 100, 
    start_date: datetime = datetime(2026, 9, 1)
) -> tuple[list[dict], list[dict]]:
    orders = []
    order_items = []
    
    products_by_seller = {}
    for p in products:
        products_by_seller.setdefault(p["seller_id"], []).append(p)
    
    item_id = 1
    for i in range(1, num_orders + 1):
        # Pick a single seller for the entire order
        seller = rng.choice(sellers)
        seller_products = products_by_seller[seller["seller_id"]]
        
        order_date = start_date + timedelta(days=rng.randint(0, 30))
        # mostly delivered/shipped, some cancelled
        status = rng.choices(["DELIVERED", "SHIPPED", "CANCELLED", "PENDING"], weights=[50, 30, 10, 10])[0]
        
        order_id = f"O{i:05d}"
        orders.append({
            "order_id": order_id,
            "order_date": order_date.strftime("%Y-%m-%d"),
            "status": status
        })
        
        num_items = rng.randint(1, 4)
        chosen_products = rng.sample(seller_products, k=min(num_items, len(seller_products)))
        
        for p in chosen_products:
            order_items.append({
                "order_item_id": f"OI{item_id:05d}",
                "order_id": order_id,
                "product_id": p["product_id"],
                "quantity": rng.randint(1, 5),
                "price": round(rng.uniform(10.0, 500.0), 2)
            })
            item_id += 1
            
    return orders, order_items

def generate_returns(rng: random.Random, orders: list[dict], return_ratio: float = 0.1) -> list[dict]:
    returns = []
    # return_ratio determines the initial set of eligible orders that will have at least one return.
    # Optional additional return events can increase the total number of return records.
    eligible_orders = [o for o in orders if o["status"] in ("DELIVERED", "SHIPPED")]
    num_returns = int(len(eligible_orders) * return_ratio)
    
    chosen_orders = rng.sample(eligible_orders, k=min(num_returns, len(eligible_orders)))
    ret_id = 1
    for order in chosen_orders:
        order_date = datetime.strptime(order["order_date"], "%Y-%m-%d")
        return_date = order_date + timedelta(days=rng.randint(1, 14))
        returns.append({
            "return_id": f"R{ret_id:04d}",
            "order_id": order["order_id"],
            "return_date": return_date.strftime("%Y-%m-%d")
        })
        ret_id += 1
        
        # Occasional multiple returns for the same order (e.g. returning a second item later)
        if rng.random() < 0.1:
            return_date_2 = return_date + timedelta(days=rng.randint(1, 5))
            returns.append({
                "return_id": f"R{ret_id:04d}",
                "order_id": order["order_id"],
                "return_date": return_date_2.strftime("%Y-%m-%d")
            })
            ret_id += 1
            
    return returns

def write_csv(data: list[dict], path: Path, fieldnames: list[str], overwrite: bool = False) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"File {path} already exists. Use overwrite=True to replace it.")
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def main(output_dir: Optional[Path] = None, seed: int = 42, overwrite: bool = False):
    """Generate synthetic CSVs."""
    output_dir = output_dir or RAW_DIR
    
    # Preflight check to prevent partial writes
    file_names = ["sellers.csv", "products.csv", "orders.csv", "order_items.csv", "returns.csv"]
    if not overwrite:
        for fname in file_names:
            if (output_dir / fname).exists():
                raise FileExistsError(f"File {output_dir / fname} already exists. Use overwrite=True to replace it.")

    rng = random.Random(seed)
    
    sellers = generate_sellers(rng, num_sellers=10)
    products = generate_products(rng, sellers, products_per_seller=5)
    orders, order_items = generate_orders_and_items(rng, sellers, products, num_orders=200)
    returns = generate_returns(rng, orders, return_ratio=0.15)
    
    write_csv(sellers, output_dir / "sellers.csv", ["seller_id", "seller_name"], overwrite=overwrite)
    write_csv(products, output_dir / "products.csv", ["product_id", "seller_id", "product_name"], overwrite=overwrite)
    write_csv(orders, output_dir / "orders.csv", ["order_id", "order_date", "status"], overwrite=overwrite)
    write_csv(order_items, output_dir / "order_items.csv", ["order_item_id", "order_id", "product_id", "quantity", "price"], overwrite=overwrite)
    write_csv(returns, output_dir / "returns.csv", ["return_id", "order_id", "return_date"], overwrite=overwrite)
    
    print(f"Generated data in {output_dir}")
    print(f"Sellers: {len(sellers)}, Products: {len(products)}, Orders: {len(orders)}, Items: {len(order_items)}, Returns: {len(returns)}")

if __name__ == "__main__":
    main()
