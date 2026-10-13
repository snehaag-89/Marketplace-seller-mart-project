"""Phase 5: Staging Transformations and Data-Quality Validation."""
import json
import uuid
from pathlib import Path

from src.db import get_db_connection
from src.config import DB_PATH, PROJECT_ROOT
from src.validation import validate_date, validate_int, validate_float, validate_status
from src.hash_utils import compute_row_hash

def run_staging_pipeline(db_path: Path = DB_PATH):
    """Run the staging pipeline to validate and transform raw data into the staging layer."""
    rejections = []
    try:
        with get_db_connection(db_path) as conn:
            # 1. Load Raw Data
            sellers = conn.execute("SELECT seller_id, seller_name, batch_id FROM raw_sellers").fetchall()
            products = conn.execute("SELECT product_id, seller_id, product_name, batch_id FROM raw_products").fetchall()
            orders = conn.execute("SELECT order_id, order_date, status, batch_id FROM raw_orders").fetchall()
            order_items = conn.execute("SELECT order_item_id, order_id, product_id, quantity, price, batch_id FROM raw_order_items").fetchall()
            returns = conn.execute("SELECT return_id, order_id, return_date, batch_id FROM raw_returns").fetchall()

            # --- Sellers ---
            valid_sellers = {}
            seen_sellers = set()
            duplicate_sellers = set()
            for r in sellers:
                if r[0] in seen_sellers:
                    duplicate_sellers.add(r[0])
                seen_sellers.add(r[0])
                
            for r in sellers:
                s_id, s_name, b_id = r
                raw_json = json.dumps({"seller_id": s_id, "seller_name": s_name})
                if not s_id or not str(s_id).strip():
                    rejections.append((b_id, "raw_sellers", raw_json, "Missing required identifier"))
                    continue
                if s_id in duplicate_sellers:
                    rejections.append((b_id, "raw_sellers", raw_json, "Duplicate primary ID"))
                    continue
                if not s_name or not str(s_name).strip():
                    rejections.append((b_id, "raw_sellers", raw_json, "Missing seller_name"))
                    continue
                valid_sellers[s_id] = r
                
            # --- Products ---
            known_sellers = set(valid_sellers.keys()) | {r[0] for r in conn.execute("SELECT seller_id FROM stg_sellers").fetchall()}
            valid_products = {}
            seen_products = set()
            duplicate_products = set()
            for r in products:
                if r[0] in seen_products:
                    duplicate_products.add(r[0])
                seen_products.add(r[0])
                
            for r in products:
                p_id, s_id, p_name, b_id = r
                raw_json = json.dumps({"product_id": p_id, "seller_id": s_id, "product_name": p_name})
                if not p_id or not str(p_id).strip() or not s_id or not str(s_id).strip():
                    rejections.append((b_id, "raw_products", raw_json, "Missing required identifier"))
                    continue
                if p_id in duplicate_products:
                    rejections.append((b_id, "raw_products", raw_json, "Duplicate primary ID"))
                    continue
                if s_id not in known_sellers:
                    rejections.append((b_id, "raw_products", raw_json, "Missing or invalid seller reference"))
                    continue
                valid_products[p_id] = r
                
            # --- Check Product Seller Reassignment Against Staged Orders ---
            stg_products_map = {r[0]: r[1] for r in conn.execute("SELECT product_id, seller_id FROM stg_products").fetchall()}
            stg_order_items_rows = conn.execute("SELECT order_id, product_id FROM stg_order_items").fetchall()
            
            stg_order_to_prods = {}
            for stg_oid, stg_pid in stg_order_items_rows:
                stg_order_to_prods.setdefault(stg_oid, set()).add(stg_pid)

            affected_conflicted_orders = set()
            invalid_reassigned_products = set()

            for p_id, r in list(valid_products.items()):
                new_s_id = r[1]
                if p_id in stg_products_map and stg_products_map[p_id] != new_s_id:
                    # Check every staged order containing this product
                    for stg_oid, prods_in_order in stg_order_to_prods.items():
                        if p_id in prods_in_order:
                            sellers_in_order = set()
                            for other_p in prods_in_order:
                                if other_p == p_id:
                                    sellers_in_order.add(new_s_id)
                                elif other_p in valid_products and other_p not in invalid_reassigned_products:
                                    sellers_in_order.add(valid_products[other_p][1])
                                else:
                                    sellers_in_order.add(stg_products_map.get(other_p))
                            sellers_in_order.discard(None)
                            if len(sellers_in_order) > 1:
                                invalid_reassigned_products.add(p_id)
                                affected_conflicted_orders.add(stg_oid)
                                raw_json = json.dumps({"product_id": p_id, "seller_id": new_s_id, "product_name": r[2]})
                                rejections.append((r[3], "raw_products", raw_json, f"Product seller reassignment violates single-seller-per-order constraint for existing staged order '{stg_oid}'"))
                                break

            for p_id in invalid_reassigned_products:
                del valid_products[p_id]

            # --- Single-Seller Constraint Check ---
            # 1. Start with existing staged products for records not being replaced
            known_products = dict(stg_products_map)
            # 2. Validated incoming product seller assignments take precedence
            known_products.update({p_id: r[1] for p_id, r in valid_products.items()})
            
            order_to_sellers = {}
            for r in order_items:
                o_id, p_id = r[1], r[2]
                if p_id in known_products:
                    seller_id = known_products[p_id]
                    order_to_sellers.setdefault(o_id, set()).add(seller_id)
                    
            for r in conn.execute("SELECT order_id, product_id FROM stg_order_items").fetchall():
                stg_o_id, stg_p_id = r[0], r[1]
                if stg_p_id in known_products:
                    seller_id = known_products[stg_p_id]
                    order_to_sellers.setdefault(stg_o_id, set()).add(seller_id)
                    
            multi_seller_orders = {o_id for o_id, s_set in order_to_sellers.items() if len(s_set) > 1} | affected_conflicted_orders
            
            # --- Orders ---
            valid_orders = {}
            seen_orders = set()
            duplicate_orders = set()
            for r in orders:
                if r[0] in seen_orders:
                    duplicate_orders.add(r[0])
                seen_orders.add(r[0])
                
            for r in orders:
                o_id, o_date, status, b_id = r
                raw_json = json.dumps({"order_id": o_id, "order_date": o_date, "status": status})
                if not o_id or not str(o_id).strip():
                    rejections.append((b_id, "raw_orders", raw_json, "Missing required identifier"))
                    continue
                if o_id in duplicate_orders:
                    rejections.append((b_id, "raw_orders", raw_json, "Duplicate primary ID"))
                    continue
                if o_id in multi_seller_orders:
                    rejections.append((b_id, "raw_orders", raw_json, "Violates single-seller-per-order constraint"))
                    continue
                if not validate_date(o_date):
                    rejections.append((b_id, "raw_orders", raw_json, "Invalid order_date"))
                    continue
                if not validate_status(status):
                    rejections.append((b_id, "raw_orders", raw_json, "Invalid order status"))
                    continue
                valid_orders[o_id] = r
                
            # --- Order Items ---
            known_orders = (set(valid_orders.keys()) | {r[0] for r in conn.execute("SELECT order_id FROM stg_orders").fetchall()}) - multi_seller_orders
            valid_items = {}
            seen_items = set()
            duplicate_items = set()
            for r in order_items:
                if r[0] in seen_items:
                    duplicate_items.add(r[0])
                seen_items.add(r[0])
                
            for r in order_items:
                oi_id, o_id, p_id, qty, price, b_id = r
                raw_json = json.dumps({"order_item_id": oi_id, "order_id": o_id, "product_id": p_id, "quantity": qty, "price": price})
                if not oi_id or not str(oi_id).strip() or not o_id or not str(o_id).strip() or not p_id or not str(p_id).strip():
                    rejections.append((b_id, "raw_order_items", raw_json, "Missing required identifier"))
                    continue
                if oi_id in duplicate_items:
                    rejections.append((b_id, "raw_order_items", raw_json, "Duplicate primary ID"))
                    continue
                if o_id in multi_seller_orders:
                    rejections.append((b_id, "raw_order_items", raw_json, "Parent order violates single-seller constraint"))
                    continue
                if o_id not in known_orders:
                    rejections.append((b_id, "raw_order_items", raw_json, "Missing or invalid order reference"))
                    continue
                if p_id not in known_products:
                    rejections.append((b_id, "raw_order_items", raw_json, "Missing or invalid product reference"))
                    continue
                if not validate_int(qty):
                    rejections.append((b_id, "raw_order_items", raw_json, "Invalid quantity"))
                    continue
                if not validate_float(price):
                    rejections.append((b_id, "raw_order_items", raw_json, "Invalid price"))
                    continue
                valid_items[oi_id] = r
                
            # --- Returns ---
            valid_returns = {}
            seen_returns = set()
            duplicate_returns = set()
            for r in returns:
                if r[0] in seen_returns:
                    duplicate_returns.add(r[0])
                seen_returns.add(r[0])
                
            for r in returns:
                ret_id, o_id, ret_date, b_id = r
                raw_json = json.dumps({"return_id": ret_id, "order_id": o_id, "return_date": ret_date})
                if not ret_id or not str(ret_id).strip() or not o_id or not str(o_id).strip():
                    rejections.append((b_id, "raw_returns", raw_json, "Missing required identifier"))
                    continue
                if ret_id in duplicate_returns:
                    rejections.append((b_id, "raw_returns", raw_json, "Duplicate primary ID"))
                    continue
                if o_id in multi_seller_orders:
                    rejections.append((b_id, "raw_returns", raw_json, "Parent order violates single-seller constraint"))
                    continue
                if o_id not in known_orders:
                    rejections.append((b_id, "raw_returns", raw_json, "Missing or invalid order reference"))
                    continue
                if not validate_date(ret_date):
                    rejections.append((b_id, "raw_returns", raw_json, "Invalid return_date"))
                    continue
                valid_returns[ret_id] = r

            # --- Staging Incremental Loads (SHA-256 Change Detection) ---
            # 1. Sellers
            existing_sellers = {r[0]: compute_row_hash({"seller_id": r[0], "seller_name": r[1]}) for r in conn.execute("SELECT seller_id, seller_name FROM stg_sellers").fetchall()}
            insert_sellers = []
            update_sellers = []
            for s_id, r in valid_sellers.items():
                s_name = r[1]
                h = compute_row_hash({"seller_id": s_id, "seller_name": s_name})
                if s_id in existing_sellers:
                    if existing_sellers[s_id] != h:
                        update_sellers.append((s_name, s_id))
                else:
                    insert_sellers.append((s_id, s_name))
            if insert_sellers:
                conn.executemany("INSERT INTO stg_sellers (seller_id, seller_name) VALUES (?, ?)", insert_sellers)
            if update_sellers:
                conn.executemany("UPDATE stg_sellers SET seller_name = ? WHERE seller_id = ?", update_sellers)

            # 2. Products
            existing_products = {r[0]: compute_row_hash({"product_id": r[0], "seller_id": r[1], "product_name": r[2]}) for r in conn.execute("SELECT product_id, seller_id, product_name FROM stg_products").fetchall()}
            insert_products = []
            update_products = []
            for p_id, r in valid_products.items():
                s_id, p_name = r[1], r[2]
                h = compute_row_hash({"product_id": p_id, "seller_id": s_id, "product_name": p_name})
                if p_id in existing_products:
                    if existing_products[p_id] != h:
                        update_products.append((s_id, p_name, p_id))
                else:
                    insert_products.append((p_id, s_id, p_name))
            if insert_products:
                conn.executemany("INSERT INTO stg_products (product_id, seller_id, product_name) VALUES (?, ?, ?)", insert_products)
            if update_products:
                conn.executemany("UPDATE stg_products SET seller_id = ?, product_name = ? WHERE product_id = ?", update_products)

            # 3. Orders
            existing_orders = {r[0]: compute_row_hash({"order_id": r[0], "order_date": r[1], "status": r[2]}) for r in conn.execute("SELECT order_id, order_date, status FROM stg_orders").fetchall()}
            insert_orders = []
            update_orders = []
            for o_id, r in valid_orders.items():
                o_date, status = r[1], r[2]
                h = compute_row_hash({"order_id": o_id, "order_date": o_date, "status": status})
                if o_id in existing_orders:
                    if existing_orders[o_id] != h:
                        update_orders.append((o_date, status, o_id))
                else:
                    insert_orders.append((o_id, o_date, status))
            if insert_orders:
                conn.executemany("INSERT INTO stg_orders (order_id, order_date, status) VALUES (?, ?, ?)", insert_orders)
            if update_orders:
                conn.executemany("UPDATE stg_orders SET order_date = ?, status = ? WHERE order_id = ?", update_orders)

            # 4. Order Items
            existing_items = {r[0]: compute_row_hash({"order_item_id": r[0], "order_id": r[1], "product_id": r[2], "quantity": r[3], "price": r[4]}) for r in conn.execute("SELECT order_item_id, order_id, product_id, quantity, price FROM stg_order_items").fetchall()}
            insert_items = []
            update_items = []
            for oi_id, r in valid_items.items():
                o_id, p_id, qty, price = r[1], r[2], int(r[3]), float(r[4])
                h = compute_row_hash({"order_item_id": oi_id, "order_id": o_id, "product_id": p_id, "quantity": qty, "price": price})
                if oi_id in existing_items:
                    if existing_items[oi_id] != h:
                        update_items.append((o_id, p_id, qty, price, oi_id))
                else:
                    insert_items.append((oi_id, o_id, p_id, qty, price))
            if insert_items:
                conn.executemany("INSERT INTO stg_order_items (order_item_id, order_id, product_id, quantity, price) VALUES (?, ?, ?, ?, ?)", insert_items)
            if update_items:
                conn.executemany("UPDATE stg_order_items SET order_id = ?, product_id = ?, quantity = ?, price = ? WHERE order_item_id = ?", update_items)

            # 5. Returns
            existing_returns = {r[0]: compute_row_hash({"return_id": r[0], "order_id": r[1], "return_date": r[2]}) for r in conn.execute("SELECT return_id, order_id, return_date FROM stg_returns").fetchall()}
            insert_returns = []
            update_returns = []
            for ret_id, r in valid_returns.items():
                o_id, ret_date = r[1], r[2]
                h = compute_row_hash({"return_id": ret_id, "order_id": o_id, "return_date": ret_date})
                if ret_id in existing_returns:
                    if existing_returns[ret_id] != h:
                        update_returns.append((o_id, ret_date, ret_id))
                else:
                    insert_returns.append((ret_id, o_id, ret_date))
            if insert_returns:
                conn.executemany("INSERT INTO stg_returns (return_id, order_id, return_date) VALUES (?, ?, ?)", insert_returns)
            if update_returns:
                conn.executemany("UPDATE stg_returns SET order_id = ?, return_date = ? WHERE return_id = ?", update_returns)
                
    finally:
        if rejections:
            with get_db_connection(db_path) as audit_conn:
                rejection_data = [
                    (str(uuid.uuid4()), b_id, table, raw_json, reason)
                    for b_id, table, raw_json, reason in rejections
                ]
                audit_conn.executemany(
                    "INSERT INTO raw_rejections (rejection_id, batch_id, source_table, raw_record_json, rejection_reason) VALUES (?, ?, ?, ?, ?)",
                    rejection_data
                )

METRICS_SQL_FALLBACK = """
WITH order_seller AS (
    SELECT 
        oi.order_id,
        p.seller_id
    FROM stg_order_items oi
    JOIN stg_products p ON oi.product_id = p.product_id
    GROUP BY oi.order_id
),
distinct_weeks AS (
    SELECT DISTINCT 
        strftime('%Y', date(o.order_date, '-3 days', 'weekday 4')) || '-W' || 
        printf('%02d', (strftime('%j', date(o.order_date, '-3 days', 'weekday 4')) - 1) / 7 + 1) AS reporting_week
    FROM stg_orders o
    JOIN order_seller os ON o.order_id = os.order_id
),
valid_seller_weeks AS (
    SELECT s.seller_id, w.reporting_week
    FROM stg_sellers s
    CROSS JOIN distinct_weeks w
)
DELETE FROM mart_seller_weekly_metrics
WHERE (seller_id, reporting_week) NOT IN (
    SELECT seller_id, reporting_week FROM valid_seller_weeks
);

INSERT INTO mart_seller_weekly_metrics (
    seller_id,
    reporting_week,
    sales_volume,
    order_return_rate,
    cancellation_rate
)
WITH order_seller AS (
    -- Single-seller invariant guarantees all items in an order belong to the same seller
    SELECT 
        oi.order_id,
        p.seller_id
    FROM stg_order_items oi
    JOIN stg_products p ON oi.product_id = p.product_id
    GROUP BY oi.order_id
),
distinct_weeks AS (
    SELECT DISTINCT 
        strftime('%Y', date(o.order_date, '-3 days', 'weekday 4')) || '-W' || 
        printf('%02d', (strftime('%j', date(o.order_date, '-3 days', 'weekday 4')) - 1) / 7 + 1) AS reporting_week
    FROM stg_orders o
    JOIN order_seller os ON o.order_id = os.order_id
),
seller_weeks AS (
    SELECT s.seller_id, w.reporting_week
    FROM stg_sellers s
    CROSS JOIN distinct_weeks w
),
order_sales AS (
    SELECT 
        oi.order_id,
        SUM(oi.quantity * oi.price) AS eligible_sales
    FROM stg_order_items oi
    JOIN stg_orders o ON oi.order_id = o.order_id
    WHERE o.status IN ('SHIPPED', 'DELIVERED')
    GROUP BY oi.order_id
),
order_returns AS (
    SELECT DISTINCT 
        order_id,
        1 AS has_return
    FROM stg_returns
),
order_summary AS (
    SELECT 
        o.order_id,
        os.seller_id,
        o.order_date,
        strftime('%Y', date(o.order_date, '-3 days', 'weekday 4')) || '-W' || 
        printf('%02d', (strftime('%j', date(o.order_date, '-3 days', 'weekday 4')) - 1) / 7 + 1) AS reporting_week,
        o.status,
        COALESCE(sales.eligible_sales, 0.0) AS sales_volume,
        CASE WHEN o.status IN ('SHIPPED', 'DELIVERED') THEN 1 ELSE 0 END AS is_eligible_order,
        CASE WHEN o.status IN ('SHIPPED', 'DELIVERED') AND ret.has_return = 1 THEN 1 ELSE 0 END AS is_returned_eligible_order,
        CASE WHEN o.status = 'CANCELLED' THEN 1 ELSE 0 END AS is_cancelled_order
    FROM stg_orders o
    JOIN order_seller os ON o.order_id = os.order_id
    LEFT JOIN order_sales sales ON o.order_id = sales.order_id
    LEFT JOIN order_returns ret ON o.order_id = ret.order_id
),
aggregated_orders AS (
    SELECT 
        seller_id,
        reporting_week,
        SUM(sales_volume) AS sales_volume,
        COUNT(order_id) AS total_orders,
        SUM(is_eligible_order) AS eligible_orders,
        SUM(is_returned_eligible_order) AS returned_eligible_orders,
        SUM(is_cancelled_order) AS cancelled_orders
    FROM order_summary
    GROUP BY seller_id, reporting_week
)
SELECT 
    sw.seller_id,
    sw.reporting_week,
    COALESCE(CAST(ao.sales_volume AS REAL), 0.0) AS sales_volume,
    CASE 
        WHEN ao.eligible_orders IS NULL OR ao.eligible_orders = 0 THEN NULL
        ELSE CAST(ao.returned_eligible_orders AS REAL) / ao.eligible_orders
    END AS order_return_rate,
    CASE 
        WHEN ao.total_orders IS NULL OR ao.total_orders = 0 THEN NULL
        ELSE CAST(ao.cancelled_orders AS REAL) / ao.total_orders
    END AS cancellation_rate
FROM seller_weeks sw
LEFT JOIN aggregated_orders ao 
    ON sw.seller_id = ao.seller_id AND sw.reporting_week = ao.reporting_week
ON CONFLICT(seller_id, reporting_week) DO UPDATE SET
    sales_volume = excluded.sales_volume,
    order_return_rate = excluded.order_return_rate,
    cancellation_rate = excluded.cancellation_rate;
"""

def run_metrics_pipeline(db_path: Path = DB_PATH) -> int:
    """Run the seller-weekly metrics pipeline to compute and persist mart_seller_weekly_metrics."""
    sql_path = PROJECT_ROOT / "sql" / "transformations.sql"
    if sql_path.exists():
        sql_query = sql_path.read_text(encoding="utf-8")
    else:
        sql_query = METRICS_SQL_FALLBACK

    with get_db_connection(db_path) as conn:
        for stmt in sql_query.split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)
        count = conn.execute("SELECT COUNT(*) FROM mart_seller_weekly_metrics").fetchone()[0]
        return count

def compute_iso_week(date_str: str) -> str:
    """Compute canonical ISO 8601 reporting week (YYYY-Www) for an order date."""
    from datetime import date
    d = date.fromisoformat(date_str)
    year, week, _ = d.isocalendar()
    return f"{year}-W{week:02d}"

if __name__ == "__main__":
    run_staging_pipeline()
    run_metrics_pipeline()

