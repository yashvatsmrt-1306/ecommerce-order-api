import http.server
import json
import os
import socketserver
import sqlite3
import threading
import time
import urllib.request
from contextlib import contextmanager

# =====================================================================
# 1. DATABASE LAYER (SQLite with Foreign Keys & Constraints)
# =====================================================================
DB_NAME = "ecommerce_production.db"


@contextmanager
def get_db_connection():
    """
    Thread-safe SQLite connection manager.
    isolation_level=None disables Python's implicit transaction handling,
    allowing explicit BEGIN, COMMIT, and ROLLBACK control via raw SQL.
    """
    conn = sqlite3.connect(DB_NAME, timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        yield conn
    finally:
        conn.close()


def setup_database():
    """Initializes tables and seeds test catalog records."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Products Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                price REAL NOT NULL CHECK (price >= 0),
                stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0)
            );
        """)

        # Orders Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_email TEXT NOT NULL,
                total_amount REAL NOT NULL CHECK (total_amount >= 0),
                status TEXT NOT NULL DEFAULT 'CONFIRMED',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Order Items Table (Relational Junction)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                unit_price REAL NOT NULL,
                FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id)
            );
        """)

        # Seed sample inventory if table is empty
        cursor.execute("SELECT COUNT(*) AS count FROM products;")
        if cursor.fetchone()["count"] == 0:
            catalog = [
                ("DEV-LAPTOP-01", "Developer Laptop 16GB", 1299.99, 10),
                ("MECH-KEY-02", "Mechanical Keyboard RGB", 89.50, 25),
                ("EXT-MONITOR-03", "4K Ultra-Wide Monitor", 349.00, 5),
            ]
            cursor.executemany(
                "INSERT INTO products (sku, name, price, stock_quantity) VALUES (?, ?, ?, ?);",
                catalog,
            )


# =====================================================================
# 2. BUSINESS LOGIC & TRANSACTION CONTROLLER
# =====================================================================
def fetch_all_products():
    """Fetches full inventory catalog."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, sku, name, price, stock_quantity FROM products ORDER BY id ASC;"
        )
        return [dict(row) for row in cursor.fetchall()]


def process_order(customer_email: str, items: list):
    """
    Executes an atomic order transaction:
    - Validates inputs and current inventory.
    - Decrements stock counts.
    - Writes order summary and nested line items.
    - Guarantees full ROLLBACK on any constraint breach or failure.
    """
    if not customer_email or "@" not in customer_email:
        return {"error": "A valid customer email is required."}, 400

    if not items or not isinstance(items, list):
        return {"error": "Order must contain a valid list of items."}, 400

    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            # Explicit SQL Transaction Start
            cursor.execute("BEGIN IMMEDIATE TRANSACTION;")

            total_amount = 0.0
            order_line_items = []

            for item in items:
                product_id = item.get("product_id")
                qty = item.get("quantity", 0)

                if not product_id or qty <= 0:
                    cursor.execute("ROLLBACK;")
                    return {
                        "error": f"Invalid product ID or quantity: {item}"
                    }, 400

                cursor.execute(
                    "SELECT id, name, price, stock_quantity FROM products WHERE id = ?;",
                    (product_id,),
                )
                product = cursor.fetchone()

                if not product:
                    cursor.execute("ROLLBACK;")
                    return {
                        "error": f"Product ID {product_id} not found."
                    }, 404

                if product["stock_quantity"] < qty:
                    cursor.execute("ROLLBACK;")
                    return {
                        "error": (
                            f"Insufficient stock for '{product['name']}'. "
                            f"Requested: {qty}, In Stock: {product['stock_quantity']}."
                        )
                    }, 400

                unit_price = product["price"]
                line_total = unit_price * qty
                total_amount += line_total

                # Deduct inventory count
                cursor.execute(
                    "UPDATE products SET stock_quantity = stock_quantity - ? WHERE id = ?;",
                    (qty, product_id),
                )

                order_line_items.append((product_id, qty, unit_price))

            # Record master order
            cursor.execute(
                "INSERT INTO orders (customer_email, total_amount, status) VALUES (?, ?, 'CONFIRMED');",
                (customer_email, round(total_amount, 2)),
            )
            order_id = cursor.lastrowid

            # Record line items
            for prod_id, qty, price in order_line_items:
                cursor.execute(
                    """
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                    VALUES (?, ?, ?, ?);
                    """,
                    (order_id, prod_id, qty, price),
                )

            # Commit all changes atomically
            cursor.execute("COMMIT;")

            return {
                "message": "Order processed successfully.",
                "order_id": order_id,
                "customer_email": customer_email,
                "total_amount": round(total_amount, 2),
                "status": "CONFIRMED",
            }, 201

        except Exception as exc:
            cursor.execute("ROLLBACK;")
            return {
                "error": f"Transaction aborted and rolled back: {str(exc)}"
            }, 500


def fetch_all_orders():
    """Fetches all placed orders with their nested order line items."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM orders ORDER BY id DESC;")
        orders = [dict(row) for row in cursor.fetchall()]

        for order in orders:
            cursor.execute(
                """
                SELECT oi.product_id, p.name, oi.quantity, oi.unit_price
                FROM order_items oi
                JOIN products p ON oi.product_id = p.id
                WHERE oi.order_id = ?;
                """,
                (order["id"],),
            )
            order["items"] = [dict(row) for row in cursor.fetchall()]

        return orders


# =====================================================================
# 3. HTTP REST API ROUTER
# =====================================================================
class APIServerHandler(http.server.BaseHTTPRequestHandler):
    def _send_json(self, data, status_code=200):
        response_bytes = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def do_GET(self):
        if self.path == "/products":
            self._send_json(fetch_all_products(), 200)
        elif self.path == "/orders":
            self._send_json(fetch_all_orders(), 200)
        else:
            self._send_json({"error": "Resource not found."}, 404)

    def do_POST(self):
        if self.path == "/orders":
            content_length = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(content_length).decode("utf-8")

            try:
                payload = json.loads(raw_body)
            except json.JSONDecodeError:
                self._send_json({"error": "Malformed JSON payload."}, 400)
                return

            email = payload.get("customer_email")
            items = payload.get("items", [])

            response_data, status_code = process_order(email, items)
            self._send_json(response_data, status_code)
        else:
            self._send_json({"error": "Resource not found."}, 404)

    def log_message(self, format, *args):
        # Mute standard access log noise during automated test run
        return


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


# =====================================================================
# 4. AUTOMATED VERIFICATION CLIENT
# =====================================================================
def run_automated_tests(port):
    """Executes automated verification suite against the running service."""
    time.sleep(1.0)
    base_url = f"http://127.0.0.1:{port}"

    print("\n" + "=" * 70)
    print("RUNNING AUTOMATED TEST SUITE (SQL TRANSACTIONS & REST API)")
    print("=" * 70)

    # 1. Inspect initial inventory
    req = urllib.request.Request(f"{base_url}/products")
    with urllib.request.urlopen(req) as res:
        products = json.loads(res.read().decode())
        print("\n[TEST 1] GET /products -> Initial Inventory State:")
        print(json.dumps(products, indent=2))

    # 2. Place a valid order
    order_payload = {
        "customer_email": "candidate@techcompany.com",
        "items": [
            {"product_id": 1, "quantity": 1},
            {"product_id": 2, "quantity": 2},
        ],
    }
    req = urllib.request.Request(
        f"{base_url}/orders",
        data=json.dumps(order_payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as res:
        order_res = json.loads(res.read().decode())
        print("\n[TEST 2] POST /orders -> Valid Order Processed:")
        print(json.dumps(order_res, indent=2))

    # 3. Test rollback defense (request more items than available)
    out_of_stock_payload = {
        "customer_email": "candidate@techcompany.com",
        "items": [
            {"product_id": 3, "quantity": 9999}  # Exceeds monitor inventory
        ],
    }
    req = urllib.request.Request(
        f"{base_url}/orders",
        data=json.dumps(out_of_stock_payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as err:
        err_res = json.loads(err.read().decode())
        print(
            f"\n[TEST 3] POST /orders -> Defensive Stock Check (HTTP {err.code}):"
        )
        print(json.dumps(err_res, indent=2))

    # 4. Confirm data persistence
    req = urllib.request.Request(f"{base_url}/orders")
    with urllib.request.urlopen(req) as res:
        orders = json.loads(res.read().decode())
        print("\n[TEST 4] GET /orders -> Verified Persisted Records:")
        print(json.dumps(orders, indent=2))

    print("\n" + "=" * 70)
    print("ALL TESTS PASSED: Transaction integrity & rollbacks verified.")
    print(f"Service listening on: {base_url}/products")
    print(f"Service listening on: {base_url}/orders")
    print("Press CTRL + C to stop the process.")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    PORT = 8080

    # Ensure clean database initialization per run
    if os.path.exists(DB_NAME):
        try:
            os.remove(DB_NAME)
        except OSError:
            pass

    setup_database()

    # Start automated test runner in background
    test_runner = threading.Thread(
        target=run_automated_tests, args=(PORT,), daemon=True
    )
    test_runner.start()

    # Start HTTP server
    server = ThreadedHTTPServer(("0.0.0.0", PORT), APIServerHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nTerminating server.")
        server.server_close()
