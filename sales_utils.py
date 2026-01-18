# sales_utils.py (upgraded for cart-based transactions and voiding)
import sqlite3
from datetime import datetime, timedelta
from fpdf import FPDF
import hashlib
import os
import json
import bcrypt
from typing import List, Dict, Tuple, Optional
import sys
from pathlib import Path

# Resolve DB file path - use current working directory (set by launcher to persistent location)
# This ensures the DB stays in the same folder as the EXE across runs
DB_NAME = os.environ.get('BAR_SALES_DB')
if not DB_NAME:
    # Use bar_sales.db in the same directory as this script
    DB_NAME = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bar_sales.db')

# =========================
# Database initialization and migration
# =========================

def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table})")
        return [row[1] for row in cur.fetchall()]
    except sqlite3.OperationalError:
        return []

def ensure_cart_schema():
    """Ensure new cart-based schema exists. If legacy 'sales' exists with item columns,
    rename to 'sales_legacy' and create new 'sales' header + 'sale_items' tables."""
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        # Detect legacy 'sales' table (row-level schema)
        legacy_sales_cols = _table_columns(conn, 'sales')
        legacy_signature = {'username', 'item', 'quantity', 'price_per_unit', 'total', 'timestamp'}
        is_legacy = legacy_signature.issubset(set(legacy_sales_cols))

        # Rename legacy table if not yet migrated
        if is_legacy:
            cur.execute("ALTER TABLE sales RENAME TO sales_legacy")

        # Create new header table 'sales'
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id TEXT UNIQUE NOT NULL,
                cashier TEXT NOT NULL,
                total REAL NOT NULL,
                timestamp TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                void_reason TEXT DEFAULT NULL,
                void_authorized_by TEXT DEFAULT NULL,
                voided_at TEXT DEFAULT NULL,
                payment_method TEXT DEFAULT 'Cash',
                mobile_ref TEXT DEFAULT NULL
            )
            """
        )
        # Ensure new columns exist when upgrading from earlier schema
        cols = set(_table_columns(conn, 'sales'))
        if 'payment_method' not in cols:
            try:
                cur.execute("ALTER TABLE sales ADD COLUMN payment_method TEXT DEFAULT 'Cash'")
            except Exception:
                pass
        if 'mobile_ref' not in cols:
            try:
                cur.execute("ALTER TABLE sales ADD COLUMN mobile_ref TEXT DEFAULT NULL")
            except Exception:
                pass

        # Create items table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sale_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sale_id INTEGER NOT NULL,
                item TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                subtotal REAL NOT NULL,
                FOREIGN KEY (sale_id) REFERENCES sales(id) ON DELETE CASCADE
            )
            """
        )

        # Indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_ts ON sales(timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_cashier ON sales(cashier)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_status_ts ON sales(status, timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sale_items_sale_id ON sale_items(sale_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sale_items_item ON sale_items(item)")

        conn.commit()
    finally:
        conn.close()

def ensure_reconciliation_schema():
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS item_reconciliation (
                item TEXT NOT NULL,
                date TEXT NOT NULL,
                old_stock INTEGER DEFAULT NULL,
                new_stock INTEGER NOT NULL DEFAULT 0,
                loss_drawn INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (item, date)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_item_recon_date ON item_reconciliation(date)")
    finally:
        conn.close()


def init_db():
    """Initialize DB (legacy tables for backward compat) and ensure cart-based schema."""
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    
    # Enable WAL mode for better concurrency
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=NORMAL")
    cur.execute("PRAGMA cache_size=-20000")  # 20MB cache for better performance
    cur.execute("PRAGMA temp_store=MEMORY")
    cur.execute("PRAGMA busy_timeout=30000")
    cur.execute("PRAGMA foreign_keys=ON")
    cur.execute("PRAGMA mmap_size=268435456")  # 256MB memory mapping
    
    # Legacy row-level sales table (kept for compat; may be renamed by ensure_cart_schema)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            item TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price_per_unit REAL NOT NULL,
            total REAL NOT NULL,
            timestamp TEXT NOT NULL
        )
    ''')
    # Users
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL
        )
    ''')
    # Inventory
    cur.execute('''
        CREATE TABLE IF NOT EXISTS inventory (
            item TEXT PRIMARY KEY,
            quantity INTEGER NOT NULL,
            cost_price REAL DEFAULT 0,
            selling_price REAL DEFAULT 0,
            category TEXT DEFAULT ''
        )
    ''')
    # Notes table for communication
    cur.execute('''
        CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT NOT NULL,
            receiver TEXT NOT NULL,
            message TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            is_read INTEGER DEFAULT 0
        )
    ''')
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notes_receiver ON notes(receiver)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_notes_receiver_isread ON notes(receiver, is_read)")
    # Performance indexes for inventory (commonly searched fields)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inventory_item ON inventory(item)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_inventory_category ON inventory(category)")
    # Seed minimal inventory when inventory is empty (useful for first-run and tests)
    try:
        cur.execute("SELECT COUNT(*) FROM inventory")
        inv_count = cur.fetchone()[0] or 0
        if inv_count == 0:
            # Do not seed drinks by default; start with an empty inventory for clean deployments
            pass
    except Exception:
        pass
    conn.commit()
    conn.close()

    # Do NOT clear the default-password notice here. The marker should be removed
    # only when stock is explicitly added by the user (e.g., via restock/update).
    # This prevents the launcher message from being removed immediately after
    # startup when the app auto-seeds minimal inventory for first-run.
    #try:
    #    if inv_count > 0:
    #        clear_default_password_marker()
    #except Exception:
    #    pass

    # Create/migrate to cart schema
    ensure_cart_schema()
    ensure_reconciliation_schema()
    ensure_adjustments_schema()

# =========================
# Auth helpers
# =========================

def hash_password(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def check_password(password, password_hash):
    return bcrypt.checkpw(password.encode(), password_hash.encode())

def create_user(username, password, role):
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    password_hash = hash_password(password)
    try:
        cur.execute("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)", (username, password_hash, role))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return False
    finally:
        conn.close()
    return True

def get_user(username):
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute("SELECT username, password_hash, role FROM users WHERE username=?", (username,))
        row = cur.fetchone()
    finally:
        conn.close()
    if row:
        return {'username': row[0], 'password_hash': row[1], 'role': row[2]}
    return None

# =========================
# Inventory helpers
# =========================

def get_stock(item):
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute('SELECT quantity FROM inventory WHERE item=?', (item,))
        row = cur.fetchone()
    finally:
        conn.close()
    return row[0] if row else 0

def update_stock(item, change):
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    marker_removed = False
    try:
        cur.execute('SELECT quantity FROM inventory WHERE item=?', (item,))
        row = cur.fetchone()
        if row:
            new_qty = row[0] + change
            cur.execute('UPDATE inventory SET quantity=? WHERE item=?', (new_qty, item))
            # If quantity moved from 0 to positive, consider it "stock added"
            if row[0] <= 0 and new_qty > 0:
                marker_removed = True
        else:
            # New item being inserted — treat as stock added
            cur.execute('INSERT INTO inventory (item, quantity) VALUES (?, ?)', (item, max(0, change)))
            if change > 0:
                marker_removed = True
        conn.commit()
    finally:
        conn.close()

    # After the DB commit, try to remove the default-password marker if stock was added
    if marker_removed:
        try:
            from pathlib import Path
            marker = Path(__file__).resolve().parent / 'data' / '.default_password_notice_pending'
            if marker.exists():
                try:
                    marker.unlink()
                except Exception:
                    # If we can't delete marker, ignore silently — not critical
                    pass
        except Exception:
            pass

def get_all_stock():
    """Backward-compatible: return list of tuples (item, qty, category).
    If bag schema exists (tables 'bags' and 'items') flatten items with bag name as category.
    Otherwise fallback to legacy 'inventory' table.
    """
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        # Detect new schema by checking for 'items' table
        try:
            cur.execute("PRAGMA table_info(items)")
            cols = cur.fetchall()
        except sqlite3.OperationalError:
            cols = []

        if cols:
            # New bag-based schema detected - flatten items with bag name as category
            cur.execute('''
                SELECT i.item_name, i.stock, COALESCE(b.bag_name, '')
                FROM items i
                LEFT JOIN bags b ON i.bag_id = b.id
                ORDER BY COALESCE(b.bag_name, ''), i.item_name
            ''')
            rows = cur.fetchall()
            return rows
        else:
            # Fallback to legacy inventory table
            cur.execute('SELECT item, quantity, category FROM inventory ORDER BY item')
            rows = cur.fetchall()
            return rows
    finally:
        conn.close()


# -------------------------
# Bag-based inventory helpers
# -------------------------

def _ensure_bag_schema():
    """Ensure 'bags' and 'items' tables exist (idempotent)."""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bag_name TEXT UNIQUE NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bag_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                price REAL NOT NULL DEFAULT 0,
                stock INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (bag_id) REFERENCES bags(id) ON DELETE CASCADE
            )
        """)
        # Stock history tracking table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS item_stock_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                bag_name TEXT,
                old_stock INTEGER NOT NULL,
                new_stock INTEGER NOT NULL,
                change_amount INTEGER NOT NULL,
                change_type TEXT NOT NULL,
                reason TEXT,
                changed_by TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                sale_id INTEGER,
                transaction_id TEXT,
                FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_history_item ON item_stock_history(item_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_history_timestamp ON item_stock_history(timestamp)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_history_type ON item_stock_history(change_type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_history_user ON item_stock_history(changed_by)")
        conn.commit()
    finally:
        conn.close()


def _log_stock_history(cur, item_id: int, item_name: str, bag_name: str, old_stock: int,
                       new_stock: int, change_type: str, changed_by: str, reason: str = None,
                       sale_id: int = None, transaction_id: str = None):
    """Internal function to log stock changes to history. Uses existing cursor."""
    change_amount = new_stock - old_stock
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("""
        INSERT INTO item_stock_history 
        (item_id, item_name, bag_name, old_stock, new_stock, change_amount, 
         change_type, reason, changed_by, timestamp, sale_id, transaction_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (item_id, item_name, bag_name, old_stock, new_stock, change_amount,
          change_type, reason, changed_by, timestamp, sale_id, transaction_id))


def log_stock_change(item_id: int, old_stock: int, new_stock: int, change_type: str,
                    changed_by: str, reason: str = None, sale_id: int = None,
                    transaction_id: str = None):
    """Public function to log stock changes. Opens its own connection.

    change_type options: 'RESTOCK', 'SALE', 'ADJUSTMENT', 'CORRECTION', 'INITIAL'
    """
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        # Get item details
        cur.execute("""
            SELECT i.item_name, b.bag_name 
            FROM items i 
            JOIN bags b ON i.bag_id = b.id 
            WHERE i.id = ?
        """, (item_id,))
        row = cur.fetchone()
        if row:
            item_name, bag_name = row
            _log_stock_history(cur, item_id, item_name, bag_name, old_stock, new_stock,
                             change_type, changed_by, reason, sale_id, transaction_id)
            conn.commit()
    finally:
        conn.close()


def get_stock_history(item_id: int = None, days: int = 30, change_type: str = None) -> List[Dict]:
    """Get stock history with optional filters.

    Returns list of dicts with: id, item_id, item_name, bag_name, old_stock, new_stock,
    change_amount, change_type, reason, changed_by, timestamp, sale_id, transaction_id
    """
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        query = """
            SELECT id, item_id, item_name, bag_name, old_stock, new_stock, change_amount,
                   change_type, reason, changed_by, timestamp, sale_id, transaction_id
            FROM item_stock_history
            WHERE timestamp >= ?
        """
        params = [cutoff_date]

        if item_id is not None:
            query += " AND item_id = ?"
            params.append(item_id)

        if change_type:
            query += " AND change_type = ?"
            params.append(change_type)

        query += " ORDER BY timestamp DESC"

        cur.execute(query, params)
        rows = cur.fetchall()

        return [{
            'id': r[0], 'item_id': r[1], 'item_name': r[2], 'bag_name': r[3],
            'old_stock': r[4], 'new_stock': r[5], 'change_amount': r[6],
            'change_type': r[7], 'reason': r[8], 'changed_by': r[9],
            'timestamp': r[10], 'sale_id': r[11], 'transaction_id': r[12]
        } for r in rows]
    finally:
        conn.close()


def get_stock_summary_by_item(days: int = 30) -> List[Dict]:
    """Get summary of stock changes grouped by item."""
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        cur.execute("""
            SELECT 
                item_id,
                item_name,
                bag_name,
                MIN(old_stock) as min_stock,
                MAX(new_stock) as max_stock,
                SUM(CASE WHEN change_amount > 0 THEN change_amount ELSE 0 END) as total_added,
                SUM(CASE WHEN change_amount < 0 THEN ABS(change_amount) ELSE 0 END) as total_removed,
                COUNT(*) as change_count
            FROM item_stock_history
            WHERE timestamp >= ?
            GROUP BY item_id, item_name, bag_name
            ORDER BY total_removed DESC
        """, (cutoff_date,))

        rows = cur.fetchall()
        return [{
            'item_id': r[0], 'item_name': r[1], 'bag_name': r[2],
            'min_stock': r[3], 'max_stock': r[4], 'total_added': r[5],
            'total_removed': r[6], 'change_count': r[7]
        } for r in rows]
    finally:
        conn.close()


def get_bags() -> List[Tuple[int, str]]:
    """Return list of bags as (id, bag_name). Creates table if missing."""
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute('SELECT id, bag_name FROM bags ORDER BY bag_name')
        return cur.fetchall()
    finally:
        conn.close()


def create_bag(bag_name: str) -> int:
    """Create a new bag and return its id. If exists, return existing id."""
    if not bag_name or not bag_name.strip():
        raise ValueError('Bag name cannot be empty')
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        try:
            cur.execute('INSERT INTO bags (bag_name) VALUES (?)', (bag_name.strip(),))
            conn.commit()
            return cur.lastrowid
        except sqlite3.IntegrityError:
            cur.execute('SELECT id FROM bags WHERE bag_name=?', (bag_name.strip(),))
            row = cur.fetchone()
            return row[0] if row else -1
    finally:
        conn.close()


def add_item_to_bag(bag_id: int, item_name: str, amount: int, price: float, username: str = 'admin') -> int:
    """Add new item into a bag or increment existing. Returns item id.
    Validates inputs and ensures tables exist. Logs stock history.
    """
    if not item_name or not item_name.strip():
        raise ValueError('Item name cannot be empty')
    if amount is None or not isinstance(amount, int):
        raise ValueError('Amount must be integer')
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        # Verify bag exists and get bag name
        cur.execute('SELECT bag_name FROM bags WHERE id=?', (bag_id,))
        bag_row = cur.fetchone()
        if not bag_row:
            raise ValueError('Bag not found')
        bag_name = bag_row[0]

        # Check if item exists in bag (by name)
        cur.execute('SELECT id, stock FROM items WHERE bag_id=? AND item_name=?', (bag_id, item_name.strip()))
        row = cur.fetchone()
        if row:
            item_id, cur_stock = row
            new_stock = cur_stock + amount
            cur.execute('UPDATE items SET stock=?, price=? WHERE id=?', (new_stock, float(price or 0), item_id))
            # Log the stock addition
            _log_stock_history(cur, item_id, item_name.strip(), bag_name, cur_stock, new_stock,
                             'RESTOCK', username, f'Added {amount} units')
            conn.commit()
            return item_id
        else:
            cur.execute('INSERT INTO items (bag_id, item_name, price, stock) VALUES (?, ?, ?, ?)',
                       (bag_id, item_name.strip(), float(price or 0), int(amount)))
            item_id = cur.lastrowid
            # Log initial stock
            _log_stock_history(cur, item_id, item_name.strip(), bag_name, 0, int(amount),
                             'INITIAL', username, f'Initial stock creation')
            conn.commit()
            return item_id
    finally:
        conn.close()


def get_items_in_bag(bag_id: int, search: str = None) -> List[Tuple[int, str, float, int]]:
    """Return list of items in a bag as (id, item_name, price, stock). Optional search filters by name."""
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        if search:
            s = f"%{search.lower()}%"
            cur.execute('SELECT id, item_name, price, stock FROM items WHERE bag_id=? AND LOWER(item_name) LIKE ? ORDER BY item_name', (bag_id, s))
        else:
            cur.execute('SELECT id, item_name, price, stock FROM items WHERE bag_id=? ORDER BY item_name', (bag_id,))
        return cur.fetchall()
    finally:
        conn.close()


def increment_item_stock(item_id: int, change: int) -> int:
    """Atomically change stock for an item, returning new stock. Raises ValueError on insufficient stock."""
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute('SELECT stock FROM items WHERE id=?', (item_id,))
        row = cur.fetchone()
        if not row:
            raise ValueError('Item not found')
        new_stock = row[0] + int(change)
        if new_stock < 0:
            raise ValueError('Insufficient stock')
        cur.execute('UPDATE items SET stock=? WHERE id=?', (new_stock, item_id))
        conn.commit()
        return new_stock
    finally:
        conn.close()


def update_bag_item(item_id: int, price: float = None, stock: int = None, username: str = 'admin', reason: str = None) -> bool:
    """Update price and/or stock for a bag item by id. Returns True if updated. Logs stock changes."""
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        # Get current item details
        cur.execute("""
            SELECT i.stock, i.item_name, b.bag_name 
            FROM items i 
            JOIN bags b ON i.bag_id = b.id 
            WHERE i.id = ?
        """, (int(item_id),))
        item_row = cur.fetchone()
        if not item_row:
            return False

        old_stock, item_name, bag_name = item_row

        # Build dynamic update
        parts = []
        params = []
        if price is not None:
            parts.append('price=?')
            params.append(float(price))
        if stock is not None:
            parts.append('stock=?')
            params.append(int(stock))
        if not parts:
            return False
        params.append(int(item_id))
        sql = f"UPDATE items SET {', '.join(parts)} WHERE id=?"
        cur.execute(sql, tuple(params))

        # Log stock change if stock was updated
        if stock is not None and int(stock) != old_stock:
            change_type = 'RESTOCK' if int(stock) > old_stock else 'ADJUSTMENT'
            if not reason:
                if int(stock) > old_stock:
                    reason = f'Stock increased by {int(stock) - old_stock}'
                else:
                    reason = f'Stock decreased by {old_stock - int(stock)}'
            _log_stock_history(cur, int(item_id), item_name, bag_name, old_stock, int(stock),
                             change_type, username, reason)

        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def delete_bag_item(item_id: int) -> bool:
    """Delete an item from a bag by item id. Returns True if deleted."""
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute('DELETE FROM items WHERE id=?', (int(item_id),))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def rename_bag(bag_id: int, new_name: str) -> bool:
    """Rename a bag. Returns True if updated."""
    if not new_name or not new_name.strip():
        raise ValueError('Bag name cannot be empty')
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute('UPDATE bags SET bag_name=? WHERE id=?', (new_name.strip(), int(bag_id)))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def delete_bag(bag_id: int) -> bool:
    """Delete a bag and its items. Returns True if deleted."""
    _ensure_bag_schema()
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        # Deleting bag will cascade to items because FK defined with ON DELETE CASCADE
        cur.execute('DELETE FROM bags WHERE id=?', (int(bag_id),))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()

def set_item_prices(item, cost, sell):
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        if cost is not None and sell is not None:
            cur.execute('UPDATE inventory SET cost_price=?, selling_price=? WHERE item=?', (cost, sell, item))
        elif cost is not None:
            cur.execute('UPDATE inventory SET cost_price=? WHERE item=?', (cost, item))
        elif sell is not None:
            cur.execute('UPDATE inventory SET selling_price=? WHERE item=?', (sell, item))
        conn.commit()
    finally:
        conn.close()

def set_item_category(item, category):
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute('UPDATE inventory SET category=? WHERE item=?', (category, item))
        conn.commit()
    finally:
        conn.close()

def get_item_category(item):
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute('SELECT category FROM inventory WHERE item=?', (item,))
        row = cur.fetchone()
    finally:
        conn.close()
    return row[0] if row else ''

def get_categories():
    """Get all unique categories from inventory"""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute('SELECT DISTINCT category FROM inventory WHERE category IS NOT NULL AND category != "" ORDER BY category')
        rows = cur.fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()

def delete_item(item):
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute('DELETE FROM inventory WHERE item=?', (item,))
        conn.commit()
    finally:
        conn.close()

def get_item_prices(item) -> Tuple[Optional[float], Optional[float]]:
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute('SELECT cost_price, selling_price FROM inventory WHERE item=?', (item,))
        row = cur.fetchone()
    finally:
        conn.close()
    return row if row else (None, None)

# =========================
# Reconciliation helpers
# =========================

def _ensure_old_stock_row(cur: sqlite3.Cursor, item: str, date: str, current_qty: Optional[int] = None):
    cur.execute("SELECT old_stock FROM item_reconciliation WHERE item=? AND date=?", (item, date))
    row = cur.fetchone()
    if not row:
        if current_qty is None:
            cur.execute("SELECT quantity FROM inventory WHERE item=?", (item,))
            q = cur.fetchone()
            current_qty = q[0] if q else 0
        cur.execute("INSERT INTO item_reconciliation (item, date, old_stock, new_stock, loss_drawn) VALUES (?, ?, ?, 0, 0)", (item, date, int(current_qty)))
    elif row[0] is None:
        if current_qty is None:
            cur.execute("SELECT quantity FROM inventory WHERE item=?", (item,))
            q = cur.fetchone()
            current_qty = q[0] if q else 0
        cur.execute("UPDATE item_reconciliation SET old_stock=? WHERE item=? AND date=?", (int(current_qty), item, date))

def record_restock(item: str, qty: int):
    if qty < 0:
        raise ValueError("Quantity must be non-negative")
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        today = datetime.now().strftime('%Y-%m-%d')
        cur.execute('SELECT quantity FROM inventory WHERE item=?', (item,))
        row = cur.fetchone()
        cur_qty = row[0] if row else 0
        _ensure_old_stock_row(cur, item, today, cur_qty)
        new_qty = cur_qty + int(qty)
        if row:
            cur.execute('UPDATE inventory SET quantity=? WHERE item=?', (new_qty, item))
        else:
            cur.execute('INSERT INTO inventory (item, quantity) VALUES (?, ?)', (item, new_qty))
        cur.execute("""
            INSERT INTO item_reconciliation(item, date, old_stock, new_stock, loss_drawn)
            VALUES (?, ?, NULL, ?, 0)
            ON CONFLICT(item, date) DO UPDATE SET new_stock = new_stock + excluded.new_stock
        """, (item, today, int(qty)))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

    # After successful commit, clear the default-password marker if stock was added
    try:
        clear_default_password_marker()
    except Exception:
        pass

def record_loss_drawn(item: str, qty: int):
    if qty < 0:
        raise ValueError("Quantity must be non-negative")
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        today = datetime.now().strftime('%Y-%m-%d')
        cur.execute('SELECT quantity FROM inventory WHERE item=?', (item,))
        row = cur.fetchone()
        cur_qty = row[0] if row else 0
        if int(qty) > cur_qty:
            raise ValueError(f"Loss/drawn quantity {qty} exceeds current stock {cur_qty} for {item}")
        _ensure_old_stock_row(cur, item, today, cur_qty)
        new_qty = cur_qty - int(qty)
        cur.execute('UPDATE inventory SET quantity=? WHERE item=?', (new_qty, item))
        cur.execute("""
            INSERT INTO item_reconciliation(item, date, old_stock, new_stock, loss_drawn)
            VALUES (?, ?, NULL, 0, ?)
            ON CONFLICT(item, date) DO UPDATE SET loss_drawn = loss_drawn + excluded.loss_drawn
        """, (item, today, int(qty)))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# =========================
# Loss/Drawings events (adjustments ledger)
# =========================

def ensure_adjustments_schema():
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS inventory_adjustments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item TEXT NOT NULL,
                qty INTEGER NOT NULL,
                adj_type TEXT NOT NULL, -- e.g., 'LOSS'
                occurred_at TEXT NOT NULL,
                reported_by TEXT NOT NULL,
                reason TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING/APPROVED/REJECTED
                created_at TEXT NOT NULL,
                approved_by TEXT,
                approved_at TEXT,
                applied INTEGER NOT NULL DEFAULT 1 -- 1 if inventory/reconciliation already adjusted
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_adj_date ON inventory_adjustments(occurred_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_adj_status ON inventory_adjustments(status)")
    finally:
        conn.close()


def report_loss_event(item: str, qty: int, reported_by: str, occurred_at: Optional[str] = None, reason: str = '', notes: str = '', apply_immediately: bool = True) -> int:
    """Record a loss/drawing event. By default, apply immediately (deduct inventory and increment reconciliation), status=PENDING.
    Returns the new event id.
    """
    if qty <= 0:
        raise ValueError("Quantity must be > 0")
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        ts_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        occurred_ts = occurred_at or ts_now
        occ_date = occurred_ts.split(' ')[0]
        applied = 1 if apply_immediately else 0
        if apply_immediately:
            # Initialize old stock for that date and apply deduction now
            cur.execute('SELECT quantity FROM inventory WHERE item=?', (item,))
            row = cur.fetchone()
            cur_qty = row[0] if row else 0
            if qty > cur_qty:
                raise ValueError(f"Loss qty {qty} exceeds current stock {cur_qty} for {item}")
            _ensure_old_stock_row(cur, item, occ_date, cur_qty)
            new_qty = cur_qty - int(qty)
            cur.execute('UPDATE inventory SET quantity=? WHERE item=?', (new_qty, item))
            # Aggregate into reconciliation for that date
            cur.execute(
                """
                INSERT INTO item_reconciliation(item, date, old_stock, new_stock, loss_drawn)
                VALUES (?, ?, NULL, 0, ?)
                ON CONFLICT(item, date) DO UPDATE SET loss_drawn = loss_drawn + excluded.loss_drawn
                """,
                (item, occ_date, int(qty))
            )
        # Insert adjustment event
        cur.execute(
            """
            INSERT INTO inventory_adjustments(item, qty, adj_type, occurred_at, reported_by, reason, notes, status, created_at, applied)
            VALUES (?, ?, 'LOSS', ?, ?, ?, ?, 'PENDING', ?, ?)
            """,
            (item, int(qty), occurred_ts, reported_by, reason or '', notes or '', ts_now, applied)
        )
        event_id = cur.lastrowid
        conn.commit()
        return event_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def approve_loss_event(event_id: int, approver: str) -> Tuple[bool, str]:
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT item, qty, occurred_at, status, applied FROM inventory_adjustments WHERE id=? AND adj_type='LOSS'", (event_id,))
        row = cur.fetchone()
        if not row:
            return False, "Loss event not found"
        item, qty, occurred_at, status, applied = row
        if status == 'APPROVED':
            return True, "Already approved"
        occ_date = occurred_at.split(' ')[0]
        if not applied:
            # Apply now
            cur.execute('SELECT quantity FROM inventory WHERE item=?', (item,))
            inv = cur.fetchone()
            cur_qty = inv[0] if inv else 0
            if int(qty) > cur_qty:
                return False, f"Insufficient stock to apply loss ({qty} > {cur_qty})"
            _ensure_old_stock_row(cur, item, occ_date, cur_qty)
            cur.execute('UPDATE inventory SET quantity=? WHERE item=?', (cur_qty - int(qty), item))
            cur.execute(
                """
                INSERT INTO item_reconciliation(item, date, old_stock, new_stock, loss_drawn)
                VALUES (?, ?, NULL, 0, ?)
                ON CONFLICT(item, date) DO UPDATE SET loss_drawn = loss_drawn + excluded.loss_drawn
                """,
                (item, occ_date, int(qty))
            )
            applied = 1
        cur.execute("UPDATE inventory_adjustments SET status='APPROVED', approved_by=?, approved_at=?, applied=? WHERE id=?",
                    (approver, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), applied, event_id))
        conn.commit()
        return True, "Approved"
    except Exception as e:
        conn.rollback()
        return False, f"Error approving: {e}"
    finally:
        conn.close()


def reject_loss_event(event_id: int, approver: str) -> Tuple[bool, str]:
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT item, qty, occurred_at, status, applied FROM inventory_adjustments WHERE id=? AND adj_type='LOSS'", (event_id,))
        row = cur.fetchone()
        if not row:
            return False, "Loss event not found"
        item, qty, occurred_at, status, applied = row
        if status == 'REJECTED':
            return True, "Already rejected"
        occ_date = occurred_at.split(' ')[0]
        if applied:
            # Compensate: restore inventory and decrement reconciliation loss_drawn
            cur.execute('SELECT quantity FROM inventory WHERE item=?', (item,))
            inv = cur.fetchone()
            cur_qty = inv[0] if inv else 0
            cur.execute('UPDATE inventory SET quantity=? WHERE item=?', (cur_qty + int(qty), item))
            cur.execute(
                """
                UPDATE item_reconciliation
                SET loss_drawn = CASE WHEN loss_drawn > ? THEN loss_drawn - ? ELSE 0 END
                WHERE item=? AND date=?
                """,
                (int(qty), int(qty), item, occ_date)
            )
        cur.execute("UPDATE inventory_adjustments SET status='REJECTED', approved_by=?, approved_at=?, applied=0 WHERE id=?",
                    (approver, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), event_id))
        conn.commit()
        return True, "Rejected"
    except Exception as e:
        conn.rollback()
        return False, f"Error rejecting: {e}"
    finally:
        conn.close()


def get_loss_events(status: Optional[str] = None, limit: int = 200) -> List[Tuple]:
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        if status:
            cur.execute(
                """
                SELECT id, item, qty, occurred_at, reported_by, reason, notes, status, created_at, approved_by, approved_at, applied
                FROM inventory_adjustments
                WHERE adj_type='LOSS' AND status=?
                ORDER BY datetime(occurred_at) DESC
                LIMIT ?
                """,
                (status, limit)
            )
        else:
            cur.execute(
                """
                SELECT id, item, qty, occurred_at, reported_by, reason, notes, status, created_at, approved_by, approved_at, applied
                FROM inventory_adjustments
                WHERE adj_type='LOSS'
                ORDER BY datetime(occurred_at) DESC
                LIMIT ?
                """,
                (limit,)
            )
        return cur.fetchall()
    finally:
        conn.close()

# =========================
# Notes/Communication System
# =========================

def send_note(sender, receiver, message):
    """Send a note from sender to receiver"""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        cur.execute("INSERT INTO notes (sender, receiver, message, timestamp) VALUES (?, ?, ?, ?)",
                    (sender, receiver, message, timestamp))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error sending note: {e}")
        return False
    finally:
        conn.close()

def get_notes_for_user(username, unread_only=False):
    """Get notes for a user"""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        if unread_only:
            cur.execute("SELECT id, sender, message, timestamp FROM notes WHERE receiver=? AND is_read=0 ORDER BY timestamp DESC",
                        (username,))
        else:
            cur.execute("SELECT id, sender, message, timestamp, is_read FROM notes WHERE receiver=? ORDER BY timestamp DESC",
                        (username,))
        rows = cur.fetchall()
        return rows
    finally:
        conn.close()

def mark_note_as_read(note_id):
    """Mark a note as read"""
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute("UPDATE notes SET is_read=1 WHERE id=?", (note_id,))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error marking note as read: {e}")
        return False
    finally:
        conn.close()

# =========================
# Legacy single-line sale API (compat mode)
# =========================

def record_sale(username, item, quantity, price_per_unit):
    """Compatibility layer. If cart tables exist, record as a one-item sale. If not, write to legacy table."""
    # Stock check
    stock = get_stock(item)
    if quantity > stock:
        raise ValueError(f"Not enough stock for {item}. In stock: {stock}, requested: {quantity}")

    conn = sqlite3.connect(DB_NAME, timeout=10)
    has_header = 'transaction_id' in _table_columns(conn, 'sales')
    has_items = len(_table_columns(conn, 'sale_items')) > 0
    conn.close()

    if has_header and has_items:
        # Create cart sale with one item
        _, _, total = create_sale_with_items(username, [{
            'item': item,
            'quantity': quantity,
            'unit_price': price_per_unit
        }])
        return total
    else:
        # Legacy write
        total = quantity * price_per_unit
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn2 = sqlite3.connect(DB_NAME, timeout=10)
        cur2 = conn2.cursor()
        cur2.execute("INSERT INTO sales (username, item, quantity, price_per_unit, total, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                     (username, item, quantity, price_per_unit, total, timestamp))
        conn2.commit()
        conn2.close()
        update_stock(item, -quantity)
        return total

# =========================
# Cart-based sale API
# =========================

def _new_transaction_id() -> str:
    dt = datetime.now().strftime('%Y%m%d%H%M%S')
    rnd = hashlib.sha256(os.urandom(16)).hexdigest()[:6]
    return f"TX-{dt}-{rnd}"

def create_sale_with_items(cashier: str, items: List[Dict], payment_method: str = 'Cash', mobile_ref: Optional[str] = None) -> Tuple[int, str, float]:
    """Create sale header + multiple sale_items with proper transaction handling.
    items: list of dicts with keys: item, quantity, unit_price
    Returns (sale_id, transaction_id, total)

    Behaviour:
    - Validates availability using combined inventory + bag items
    - Deducts stock from inventory first, then from bag rows (distributing across rows)
    - Runs all DB updates inside a single transaction to avoid race conditions
    """
    ensure_cart_schema()
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()

    try:
        cur.execute("BEGIN IMMEDIATE")

        # Normalize input and compute total
        normalized = []
        total = 0.0
        for it in items:
            name = it.get('item')
            qty = int(it.get('quantity') or 0)
            unit_price = float(it.get('unit_price') or 0.0)
            if qty <= 0:
                raise ValueError(f"Invalid quantity for {name}: {qty}")
            subtotal = qty * unit_price
            total += subtotal
            normalized.append({'item': name, 'quantity': qty, 'unit_price': unit_price, 'subtotal': subtotal})

        # Check availability for each distinct item (inventory + bags)
        stock_data = {}
        for it in normalized:
            name = it['item']
            if name in stock_data:
                continue
            cur.execute('SELECT COALESCE(quantity,0) FROM inventory WHERE item=?', (name,))
            r = cur.fetchone()
            inv_qty = int(r[0]) if r and r[0] is not None else 0
            cur.execute('SELECT COALESCE(SUM(stock),0) FROM items WHERE item_name=?', (name,))
            r = cur.fetchone()
            bag_qty = int(r[0]) if r and r[0] is not None else 0
            stock_data[name] = {'inventory': inv_qty, 'bags': bag_qty, 'total': inv_qty + bag_qty}

        # Validate requested quantities
        for it in normalized:
            if it['quantity'] > stock_data[it['item']]['total']:
                raise ValueError(f"Not enough stock for {it['item']}. Available: {stock_data[it['item']]['total']}, requested: {it['quantity']}")

        # Insert sale header
        tx_id = _new_transaction_id()
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cols = set(_table_columns(conn, 'sales'))
        if 'payment_method' in cols and 'mobile_ref' in cols:
            cur.execute(
                "INSERT INTO sales (transaction_id, cashier, total, timestamp, status, payment_method, mobile_ref) VALUES (?, ?, ?, ?, 'ACTIVE', ?, ?)",
                (tx_id, cashier, total, ts, payment_method, mobile_ref)
            )
        else:
            cur.execute(
                "INSERT INTO sales (transaction_id, cashier, total, timestamp, status) VALUES (?, ?, ?, ?, 'ACTIVE')",
                (tx_id, cashier, total, ts)
            )
        sale_id = cur.lastrowid

        sale_date = ts.split(' ')[0]

        # Insert sale_items and deduct stock (inventory first, then bag rows)
        for it in normalized:
            item_name = it['item']
            qty_to_deduct = int(it['quantity'])
            unit_price = float(it['unit_price'])
            subtotal = float(it['subtotal'])

            cur.execute(
                "INSERT INTO sale_items (sale_id, item, quantity, unit_price, subtotal) VALUES (?, ?, ?, ?, ?)",
                (sale_id, item_name, qty_to_deduct, unit_price, subtotal)
            )

            # Best-effort reconciliation snapshot
            try:
                _ensure_old_stock_row(cur, item_name, sale_date, stock_data[item_name]['total'])
            except Exception:
                pass

            remaining = qty_to_deduct

            # Deduct from inventory
            if stock_data[item_name]['inventory'] > 0 and remaining > 0:
                cur.execute('SELECT COALESCE(quantity,0) FROM inventory WHERE item=?', (item_name,))
                r = cur.fetchone()
                cur_qty = int(r[0]) if r and r[0] is not None else 0
                take = min(cur_qty, remaining)
                if take > 0:
                    new_inv = cur_qty - take
                    cur.execute('UPDATE inventory SET quantity=? WHERE item=?', (new_inv, item_name))
                    remaining -= take

            # Deduct from bag rows if still remaining
            if remaining > 0 and stock_data[item_name]['bags'] > 0:
                cur.execute('SELECT id, stock, item_name FROM items WHERE item_name=? AND stock>0 ORDER BY stock DESC', (item_name,))
                bag_rows = cur.fetchall()
                for brow in bag_rows:
                    if remaining <= 0:
                        break
                    b_id = brow[0]
                    b_stock = int(brow[1]) if brow[1] is not None else 0
                    b_item_name = brow[2]
                    if b_stock <= 0:
                        continue
                    take = min(b_stock, remaining)
                    new_b = b_stock - take
                    cur.execute('UPDATE items SET stock=? WHERE id=?', (new_b, b_id))

                    # Log the sale deduction in stock history
                    cur.execute("""
                        SELECT b.bag_name 
                        FROM bags b 
                        JOIN items i ON i.bag_id = b.id 
                        WHERE i.id = ?
                    """, (b_id,))
                    bag_name_row = cur.fetchone()
                    bag_name = bag_name_row[0] if bag_name_row else 'Unknown'
                    _log_stock_history(cur, b_id, b_item_name, bag_name, b_stock, new_b,
                                     'SALE', cashier, f'Sold {take} units', sale_id, tx_id)

                    remaining -= take

            if remaining > 0:
                # Defensive: should not happen due to earlier validation
                raise ValueError(f"Stock deduction failed for {item_name}. Remaining to deduct: {remaining}")

        # Commit the entire sale
        conn.commit()
        return sale_id, tx_id, float(total)

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# =========================
# Queries and reporting
# =========================

def get_recent_sales_headers(limit: int = 20) -> List[Tuple]:
    ensure_cart_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, transaction_id, cashier, total, timestamp, status FROM sales ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows

def get_sale_items(sale_id: int) -> List[Tuple]:
    ensure_cart_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT item, quantity, unit_price, subtotal FROM sale_items WHERE sale_id=? ORDER BY id",
            (sale_id,)
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows

def get_daily_summary(day: Optional[str] = None) -> Dict:
    ensure_cart_schema()
    if day is None:
        day = datetime.now().strftime('%Y-%m-%d')
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(SUM(total),0) FROM sales WHERE status!='VOIDED' AND DATE(timestamp)=?", (day,))
        total = cur.fetchone()[0] or 0
        cur.execute(
            """
            SELECT si.item, SUM(si.quantity) as qty
            FROM sale_items si
            JOIN sales s ON s.id=si.sale_id
            WHERE s.status!='VOIDED' AND DATE(s.timestamp)=?
            GROUP BY si.item
            ORDER BY qty DESC
            LIMIT 5
            """,
            (day,)
        )
        top_items = cur.fetchall()
        cur.execute(
            """
            SELECT cashier, COALESCE(SUM(total),0) as tot
            FROM sales
            WHERE status!='VOIDED' AND DATE(timestamp)=?
            GROUP BY cashier
            ORDER BY tot DESC
            """,
            (day,)
        )
        cashier_perf = cur.fetchall()
    finally:
        conn.close()
    return {
        'date': day,
        'total_sales': total,
        'top_items': top_items,
        'cashier_performance': cashier_perf
    }

def get_weekly_summary() -> Dict:
    ensure_cart_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        today = datetime.now().date()
        start = (today - timedelta(days=6)).strftime('%Y-%m-%d')
        end = today.strftime('%Y-%m-%d')
        cur.execute(
            """
            SELECT DATE(timestamp) d, COALESCE(SUM(total),0)
            FROM sales
            WHERE status!='VOIDED' AND DATE(timestamp) BETWEEN ? AND ?
            GROUP BY DATE(timestamp)
            ORDER BY DATE(timestamp)
            """,
            (start, end)
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return {'range': (start, end), 'daily_totals': rows}

# =========================
# Exports and backup
# =========================

def export_to_csv():
    import csv
    ensure_cart_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT s.id, s.transaction_id, s.cashier, s.timestamp, s.status,
                   si.item, si.quantity, si.unit_price, si.subtotal, s.total
            FROM sales s
            LEFT JOIN sale_items si ON si.sale_id = s.id
            ORDER BY s.timestamp DESC, si.id ASC
        """)
        rows = cur.fetchall()
        headers = [
            "id","transaction_id","cashier","timestamp","status",
            "item","quantity","unit_price","subtotal","sale_total"
        ]
    finally:
        conn.close()

    if not os.path.exists("exports"):
        os.makedirs("exports")

    # Keep filename as sales.csv for compatibility
    with open("exports/sales.csv", "w", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)

def get_total_sales():
    ensure_cart_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(SUM(total),0) FROM sales WHERE status!='VOIDED'")
        total = cur.fetchone()[0]
    finally:
        conn.close()
    return total if total else 0

# =========================
# Receipts
# =========================

def generate_pdf_receipt(receipt_id, username, item, quantity, price_per_unit, total, timestamp):
    """Legacy single-item receipt (kept for compatibility with existing UI).
    Use settings from business_settings so admin-saved personal/business info appears on receipts.
    """
    # Local import to avoid circular import at module load
    try:
        import business_settings
        settings = business_settings.get_receipt_settings()
        business_name = settings.get('business_name', 'Gorgeous Brides Boutique')
        business_address = settings.get('address', 'Shop F14 Upstairs, Downtown Shopping Mall')
        phone = settings.get('phone_primary', '+260779370289')
        tpin = settings.get('tpin', '1018786730')
        contact_info = f"Contact: {phone} | TPIN: {tpin}"
        logo_path = settings.get('business_logo_path') or os.path.join("assets", "logo.png")
        currency_symbol = settings.get('currency_symbol', 'ZMW')
    except Exception:
        business_name = "Gorgeous Brides Boutique"
        business_address = "Shop F14 Upstairs, Downtown Shopping Mall"
        contact_info = "Contact: +260779370289 | TPIN: 1018786730"
        logo_path = os.path.join("assets", "logo.png")
        currency_symbol = "ZMW"
    signature_data = f"{receipt_id}|{username}|{item}|{quantity}|{price_per_unit}|{total}|{timestamp}"
    signature = hashlib.sha256(signature_data.encode()).hexdigest()
    pdf = FPDF()
    pdf.add_page()
    if os.path.exists(logo_path):
        try:
            pdf.image(logo_path, x=10, y=8, w=33)
            pdf.set_xy(50, 10)
        except Exception:
            pdf.set_xy(10, 10)
    else:
        pdf.set_xy(10, 10)
    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, business_name, ln=1)
    pdf.set_font("Arial", "", 10)
    pdf.cell(0, 6, business_address, ln=1)
    pdf.cell(0, 6, contact_info, ln=1)
    pdf.ln(10)
    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, f"Receipt ID: {receipt_id}", ln=1)
    pdf.set_font("Arial", "", 12)
    pdf.cell(0, 10, f"Date/Time: {timestamp}", ln=1)
    pdf.cell(0, 10, f"Cashier: {username}", ln=1)
    pdf.ln(5)
    pdf.cell(0, 10, f"Item: {item}", ln=1)
    pdf.cell(0, 10, f"Quantity: {quantity}", ln=1)
    pdf.cell(0, 10, f"Price per Unit: {currency_symbol} {price_per_unit:.2f}", ln=1)
    pdf.cell(0, 10, f"Total: {currency_symbol} {total:.2f}", ln=1)
    pdf.ln(10)
    pdf.set_font("Arial", "I", 10)
    pdf.multi_cell(0, 8, f"Digital Signature:\n{signature}")
    if not os.path.exists("exports"):
        os.makedirs("exports")
    pdf_path = os.path.join("exports", f"receipt_{receipt_id}.pdf")
    pdf.output(pdf_path)
    return pdf_path

def generate_pdf_receipt_for_sale(sale_id: int) -> str:
    """Generate a multi-item receipt for a sale header+items."""
    ensure_cart_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        # Try to include payment fields if present
        try:
            cur.execute("SELECT transaction_id, cashier, total, timestamp, status, payment_method, mobile_ref FROM sales WHERE id=?", (sale_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("Sale not found")
            tx_id, cashier, total, ts, status, pay_meth, mob_ref = row
        except Exception:
            cur.execute("SELECT transaction_id, cashier, total, timestamp, status FROM sales WHERE id=?", (sale_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("Sale not found")
            tx_id, cashier, total, ts, status = row
            pay_meth, mob_ref = None, None
        cur.execute("SELECT item, quantity, unit_price, subtotal FROM sale_items WHERE sale_id=? ORDER BY id", (sale_id,))
        items = cur.fetchall()
    finally:
        conn.close()

    # Use settings saved in admin settings where possible
    try:
        import business_settings
        settings = business_settings.get_receipt_settings()
        business_name = settings.get('business_name', 'Gorgeous Brides Boutique')
        business_address = settings.get('address', 'Shop F14 Upstairs, Downtown Shopping Mall')
        phone = settings.get('phone_primary', '+260779370289')
        tpin = settings.get('tpin', '1018786730')
        contact_info = f"Contact: {phone} | TPIN: {tpin}"
        logo_path = settings.get('business_logo_path') or os.path.join("assets", "logo.png")
        currency_symbol = settings.get('currency_symbol', 'ZMW')
        receipt_footer = settings.get('receipt_footer', 'Thank you for your business!')
    except Exception:
        business_name = "Gorgeous Brides Boutique"
        business_address = "Shop F14 Upstairs, Downtown Shopping Mall"
        contact_info = "Contact: +260779370289 | TPIN: 1018786730"
        logo_path = os.path.join("assets", "logo.png")
        currency_symbol = "ZMW"
        receipt_footer = "Thank you for your business!"
    signature_data = f"{tx_id}|{cashier}|{total}|{ts}|{len(items)}"
    signature = hashlib.sha256(signature_data.encode()).hexdigest()

    pdf = FPDF()
    pdf.add_page()
    if os.path.exists(logo_path):
        try:
            pdf.image(logo_path, x=10, y=8, w=33)
            pdf.set_xy(50, 10)
        except Exception:
            pdf.set_xy(10, 10)
    else:
        pdf.set_xy(10, 10)

    pdf.set_font("Arial", "B", 16)
    pdf.cell(0, 10, business_name, ln=1)
    pdf.set_font("Arial", "", 10)
    pdf.cell(0, 6, business_address, ln=1)
    pdf.cell(0, 6, contact_info, ln=1)
    pdf.ln(5)

    pdf.set_font("Arial", "B", 14)
    pdf.cell(0, 10, f"Receipt: {tx_id}", ln=1)
    pdf.set_font("Arial", "", 12)
    pdf.cell(0, 8, f"Date/Time: {ts}", ln=1)
    pdf.cell(0, 8, f"Cashier: {cashier}", ln=1)
    pdf.cell(0, 8, f"Status: {status}", ln=1)
    pdf.ln(5)

    # Table header
    pdf.set_font("Arial", "B", 12)
    pdf.cell(80, 8, "Item", 1)
    pdf.cell(25, 8, "Qty", 1, 0, 'R')
    pdf.cell(35, 8, "Unit", 1, 0, 'R')
    pdf.cell(40, 8, "Subtotal", 1, 1, 'R')
    pdf.set_font("Arial", "", 12)

    for it, qty, unit, sub in items:
        pdf.cell(80, 8, str(it), 1)
        pdf.cell(25, 8, str(qty), 1, 0, 'R')
        pdf.cell(35, 8, f"{unit:.2f}", 1, 0, 'R')
        pdf.cell(40, 8, f"{sub:.2f}", 1, 1, 'R')

    pdf.ln(3)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(140, 8, "TOTAL", 1)
    pdf.cell(40, 8, f"{total:.2f}", 1, 1, 'R')

    pdf.ln(8)
    pdf.set_font("Arial", "I", 9)
    pdf.multi_cell(0, 6, f"Digital Signature:\n{signature}")

    if not os.path.exists("exports"):
        os.makedirs("exports")
    pdf_path = os.path.join("exports", f"receipt_{tx_id}.pdf")
    pdf.output(pdf_path)
    return pdf_path

def print_sales_receipt_thermal(sale_id: int, dry_run: bool = False) -> bool:
    """Build and send thermal receipt for a given sale id using the thermal_printer helper.
    Returns True if successfully sent to a configured printer, False otherwise.
    If dry_run=True the function will not attempt to send to hardware and will return the
    generated payload bytes saved to a file under exports/ for inspection.
    """
    try:
        from thermal_printer import build_receipt, send_payload
        from settings_persistence import load_settings
    except Exception:
        return False

    # Load sale details from DB
    ensure_cart_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute("SELECT transaction_id, cashier, total, timestamp, payment_method, mobile_ref FROM sales WHERE id=?", (sale_id,))
        row = cur.fetchone()
        if not row:
            return False
        tx_id, cashier, total, ts, pay_meth, mob_ref = row
        cur.execute("SELECT item, quantity, unit_price, subtotal FROM sale_items WHERE sale_id=? ORDER BY id", (sale_id,))
        items_rows = cur.fetchall()
    finally:
        conn.close()

    items = []
    subtotal = 0.0
    for it in items_rows:
        item_name, qty, unit_price, subtotal_item = it
        subtotal += subtotal_item
        items.append({
            'name': item_name,
            'quantity': qty,
            'unit_price': unit_price
        })

    settings = load_settings()
    printer_cfg = settings.get('printer', {})
    paper = int(printer_cfg.get('paper_mm', 58))
    # Build shop info dynamically from admin settings
    try:
        import business_settings
        biz_settings = business_settings.get_receipt_settings()
        shop_info = {
            'name': biz_settings.get('business_name') or "Gorgeous Brides Boutique",
            'address': biz_settings.get('address') or "",
            'phone': biz_settings.get('phone_primary') or "",
            'tpin': biz_settings.get('tpin') or "",
            'footer': biz_settings.get('receipt_footer') or "Thank you for your business!"
        }
    except Exception:
        shop_info = {
            'name': "Gorgeous Brides Boutique",
            'address': "Shop F14 Upstairs, Downtown Shopping Mall",
            'footer': "Thank you for your business!"
        }

    totals = {
        'subtotal': subtotal,
        'tax': 0.0,
        'total': total
    }

    payload = build_receipt(shop_info, items, totals, paper=paper)

    if dry_run:
        # Save payload to exports for debugging
        try:
            if not os.path.exists('exports'):
                os.makedirs('exports')
            path = os.path.join('exports', f'receipt_payload_{sale_id}.bin')
            with open(path, 'wb') as f:
                f.write(payload)
        except Exception:
            pass
        return True

    # Attempt to send to configured printer
    device = printer_cfg.get('device')
    baud = printer_cfg.get('baudrate', 19200)
    try:
        sent = send_payload(payload, printer_device=device, baudrate=baud)
        return bool(sent)
    except Exception:
        return False

# =========================
# Backups, audit, exports
# =========================

def backup_today_sales():
    ensure_cart_schema()
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM sales WHERE DATE(timestamp)=?", (today,))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    finally:
        conn.close()
    sales_list = [dict(zip(columns, row)) for row in rows]
    if not os.path.exists("data"):
        os.makedirs("data")
    backup_path = os.path.join("data", f"backup_{today}.json")
    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump(sales_list, f, indent=2)
    return backup_path

def log_audit_event(event):
    if not os.path.exists("data"):
        os.makedirs("data")
    log_path = os.path.join("data", "audit.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {event}\n")

def export_all_sales_to_csv(start_date, end_date, selected_columns, export_format='CSV'):
    import csv
    ensure_cart_schema()

    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        # Updated query to JOIN with sale_items to get item details
        query = """
        SELECT s.id, s.transaction_id, s.cashier, s.total, s.timestamp, s.status,
               si.item, si.quantity, si.unit_price, si.subtotal
        FROM sales s
        LEFT JOIN sale_items si ON s.id = si.sale_id
        WHERE DATE(s.timestamp) BETWEEN ? AND ?
        AND s.status != 'VOIDED'
        ORDER BY s.timestamp DESC, s.id, si.item
        """
        cur.execute(query, (start_date, end_date))
        rows = cur.fetchall()

        # Updated headers to include item information
        headers = ['Sale ID', 'Transaction ID', 'Cashier', 'Sale Total', 'Timestamp', 'Status',
                   'Item Name', 'Quantity', 'Unit Price', 'Line Total']

    finally:
        conn.close()

    if not os.path.exists("exports"):
        os.makedirs("exports")
    today = datetime.now().strftime('%Y-%m-%d')

    # Generate timestamp for unique filenames
    timestamp = datetime.now().strftime('%H%M%S')

    if export_format == 'Excel':
        from excel_styler import excel_styler

        xlsx_path = os.path.join("exports", f"sales_with_items_{start_date}_to_{end_date}_{today}_{timestamp}.xlsx")

        # Create professionally styled workbook
        wb, ws = excel_styler.create_workbook("Sales Report")

        # Add business header
        current_row = excel_styler.add_business_header(
            ws,
            "Sales Report with Items",
            f"{start_date} to {end_date}",
            "Sales System"
        )

        # Add sales section title with automatic blue coloring
        current_row = excel_styler.format_section_title_auto(
            ws,
            "📊 SALES TRANSACTIONS DATA",
            current_row,
            len(headers)
        )

        # Format and add headers
        current_row = excel_styler.format_header(ws, headers, current_row)

        # Add data rows with automatic sales formatting (blue theme)
        current_row = excel_styler.format_data_rows_auto(
            ws,
            rows,
            current_row,
            "sales transactions"
        )

        # Calculate and add total row
        total_sales = sum(float(row[3]) if row[3] else 0 for row in rows)
        total_items = len([row for row in rows if row[6]])  # Count non-empty item names

        total_row = ['', 'TOTAL', '', f'{total_sales:.2f}', '', f'{total_items} Items', '', '', '', '']
        excel_styler.format_total_row(ws, total_row, current_row, 'sales')

        # Auto-size columns
        excel_styler.auto_size_columns(ws)

        # Save workbook
        excel_styler.save_workbook(wb, os.path.basename(xlsx_path))
        return xlsx_path
    else:
        csv_path = os.path.join("exports", f"sales_with_items_{start_date}_to_{end_date}_{today}_{timestamp}.csv")
        with open(csv_path, "w", newline='', encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(rows)
        return csv_path

def export_sales_with_expenses(start_date, end_date, export_format='CSV'):
    """Export sales data with expenses summary included"""
    import csv
    ensure_cart_schema()

    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        # Get sales data
        query = """
        SELECT s.id, s.transaction_id, s.cashier, s.total, s.timestamp, s.status,
               si.item, si.quantity, si.unit_price, si.subtotal
        FROM sales s
        LEFT JOIN sale_items si ON s.id = si.sale_id
        WHERE DATE(s.timestamp) BETWEEN ? AND ?
        AND s.status != 'VOIDED'
        ORDER BY s.timestamp DESC, s.id, si.item
        """
        cur.execute(query, (start_date, end_date))
        sales_rows = cur.fetchall()
        
        # Get expenses data
        try:
            cur.execute("""
                SELECT date, category, description, amount, cashier, created_at, notes
                FROM daily_expenses
                WHERE date BETWEEN ? AND ?
                ORDER BY date DESC, created_at DESC
            """, (start_date, end_date))
            expenses_rows = cur.fetchall()
        except Exception:
            # Table might not exist
            expenses_rows = []

    finally:
        conn.close()

    if not os.path.exists("exports"):
        os.makedirs("exports")
    today = datetime.now().strftime('%Y-%m-%d')
    timestamp = datetime.now().strftime('%H%M%S')

    if export_format == 'Excel':
        from excel_styler import excel_styler

        xlsx_path = os.path.join("exports", f"sales_expenses_{start_date}_to_{end_date}_{today}_{timestamp}.xlsx")

        # Create professionally styled workbook
        wb, ws_summary = excel_styler.create_workbook("Executive Summary")

        # === EXECUTIVE SUMMARY SHEET ===
        current_row = excel_styler.add_business_header(
            ws_summary,
            "Sales & Expenses Executive Report",
            f"{start_date} to {end_date}",
            "Management System"
        )

        # Calculate totals first
        total_sales = sum(float(row[3]) for row in sales_rows if row[3])

        # Normalize expenses and calculate total
        normalized_expenses = []
        total_expenses = 0.0
        for row in expenses_rows:
            if len(row) >= 8 and isinstance(row[0], int):
                _, date, category, description, amount_val, cashier_name, created_at, notes = row[:8]
            else:
                try:
                    date, category, description, amount_val, cashier_name, created_at, notes = tuple(row[:7])
                except Exception:
                    continue
            try:
                amt = float(amount_val) if amount_val is not None else 0.0
            except Exception:
                amt = 0.0
            normalized_expenses.append([date, category, description, amt, cashier_name or '', created_at or '', notes or ''])
            total_expenses += amt

        net_profit = total_sales - total_expenses

        # Add summary section with automatic color detection
        current_row = excel_styler.format_section_title_auto(
            ws_summary,
            "💰 FINANCIAL SUMMARY DASHBOARD",
            current_row,
            4
        )

        summary_headers = ['Metric', 'Amount (ZMW)', 'Percentage', 'Status']
        current_row = excel_styler.format_header(ws_summary, summary_headers, current_row)

        summary_data = [
            ['Total Sales Revenue', f'{total_sales:.2f}', '100%', '📈 Revenue'],
            ['Total Expenses', f'{total_expenses:.2f}', f'{(total_expenses/total_sales*100 if total_sales > 0 else 0):.1f}%', '📉 Costs'],
            ['Net Profit/Loss', f'{net_profit:.2f}', f'{(net_profit/total_sales*100 if total_sales > 0 else 0):.1f}%', '💰 Profit' if net_profit >= 0 else '⚠️ Loss']
        ]

        current_row = excel_styler.format_data_rows(ws_summary, summary_data, current_row, 'summary')

        # Add profit/loss highlighting
        profit_row = ['NET RESULT', f'{net_profit:.2f}', '', 'PROFIT' if net_profit >= 0 else 'LOSS']
        excel_styler.format_total_row(ws_summary, profit_row, current_row + 1, 'summary')

        # === DETAILED SALES SHEET ===
        ws_sales = wb.create_sheet("Sales Details")

        current_row = excel_styler.add_business_header(
            ws_sales,
            "Detailed Sales Report",
            f"{start_date} to {end_date}",
            "Sales System"
        )

        current_row = excel_styler.format_section_title_auto(
            ws_sales,
            "📊 SALES TRANSACTIONS DETAIL",
            current_row,
            10
        )

        sales_headers = ['Sale ID', 'Transaction ID', 'Cashier', 'Sale Total', 'Timestamp', 'Status',
                        'Item Name', 'Quantity', 'Unit Price', 'Line Total']
        current_row = excel_styler.format_header(ws_sales, sales_headers, current_row)
        current_row = excel_styler.format_data_rows(ws_sales, sales_rows, current_row, 'sales')

        sales_total_row = ['', 'TOTAL SALES', '', f'{total_sales:.2f}', '', f'{len(sales_rows)} Transactions', '', '', '', '']
        excel_styler.format_total_row(ws_sales, sales_total_row, current_row, 'sales')

        # === EXPENSES SHEET ===
        ws_expenses = wb.create_sheet("Expenses Details")

        current_row = excel_styler.add_business_header(
            ws_expenses,
            "Detailed Expenses Report",
            f"{start_date} to {end_date}",
            "Expense System"
        )

        current_row = excel_styler.format_section_title_auto(
            ws_expenses,
            "💸 EXPENSE RECORDS DETAIL",
            current_row,
            7
        )

        expenses_headers = ['Date', 'Category', 'Description', 'Amount (ZMW)', 'Cashier', 'Created At', 'Notes']
        current_row = excel_styler.format_header(ws_expenses, expenses_headers, current_row)
        current_row = excel_styler.format_data_rows_auto(ws_expenses, normalized_expenses, current_row, "expense records")

        expenses_total_row = ['', 'TOTAL EXPENSES', '', f'{total_expenses:.2f}', '', f'{len(normalized_expenses)} Records', '']
        excel_styler.format_total_row(ws_expenses, expenses_total_row, current_row, 'expenses')

        # Auto-size all sheets
        excel_styler.auto_size_columns(ws_summary)
        excel_styler.auto_size_columns(ws_sales)
        excel_styler.auto_size_columns(ws_expenses)

        # Add expense breakdown by category to summary sheet
        current_row = len([cell for cell in ws_summary['A'] if cell.value]) + 3

        current_row = excel_styler.format_section_title_auto(
            ws_summary,
            "📊 EXPENSE BREAKDOWN BY CATEGORY",
            current_row,
            4
        )

        # Calculate category totals
        category_totals = {}
        for exp_row in normalized_expenses:
            category = exp_row[1]  # Category is at index 1
            amount = float(exp_row[3]) if exp_row[3] else 0  # Amount is at index 3
            category_totals[category] = category_totals.get(category, 0) + amount

        category_headers = ['Category', 'Amount (ZMW)', 'Percentage', 'Count']
        current_row = excel_styler.format_header(ws_summary, category_headers, current_row)

        # Create category breakdown data
        category_data = []
        for category, amount in sorted(category_totals.items(), key=lambda x: x[1], reverse=True):
            count = len([exp for exp in normalized_expenses if exp[1] == category])
            percentage = f"{(amount/total_expenses*100 if total_expenses > 0 else 0):.1f}%"
            category_data.append([category, f"{amount:.2f}", percentage, str(count)])

        excel_styler.format_data_rows(ws_summary, category_data, current_row, 'expenses')

        # Save the professionally styled workbook
        return excel_styler.save_workbook(wb, os.path.basename(xlsx_path))
    else:
        # CSV format - create combined file
        csv_path = os.path.join("exports", f"sales_expenses_{start_date}_to_{end_date}_{today}_{timestamp}.csv")
        with open(csv_path, "w", newline='', encoding="utf-8") as file:
            writer = csv.writer(file)
            
            # Sales section
            writer.writerow(['SALES REPORT', f'{start_date} to {end_date}'])
            writer.writerow([])
            
            sales_headers = ['Sale ID', 'Transaction ID', 'Cashier', 'Sale Total', 'Timestamp', 'Status',
                           'Item Name', 'Quantity', 'Unit Price', 'Line Total']
            writer.writerow(sales_headers)
            writer.writerows(sales_rows)
            
            total_sales = sum(float(row[3]) for row in sales_rows if row[3])
            writer.writerow([])
            writer.writerow(['TOTAL SALES', '', '', total_sales])
            
            # Expenses section
            writer.writerow([])
            writer.writerow(['EXPENSES REPORT', f'{start_date} to {end_date}'])
            writer.writerow([])
            
            expenses_headers = ['Date', 'Category', 'Description', 'Amount (ZMW)', 'Cashier', 'Created At', 'Notes']
            writer.writerow(expenses_headers)

            # Normalize and write expenses rows
            total_expenses = 0.0
            for row in expenses_rows:
                if len(row) >= 8 and isinstance(row[0], int):
                    _, date, category, description, amount_val, cashier_name, created_at, notes = row[:8]
                else:
                    try:
                        date, category, description, amount_val, cashier_name, created_at, notes = tuple(row[:7])
                    except Exception:
                        continue
                try:
                    amt = float(amount_val) if amount_val is not None else 0.0
                except Exception:
                    amt = 0.0
                writer.writerow([date, category, description, f"{amt:.2f}", cashier_name or '', created_at or '', notes or ''])
                total_expenses += amt

            writer.writerow([])
            writer.writerow(['TOTAL EXPENSES', '', '', f"{total_expenses:.2f}"])

        return csv_path

# =========================
# Item sales history (cart tables)
# =========================

def get_sales_history_for_item(item):
    ensure_cart_schema()
    conn = sqlite3.connect(DB_NAME, timeout=10)
    cur = conn.cursor()
    try:
        cur.execute('''
            SELECT s.timestamp, s.cashier, si.quantity, si.unit_price, si.subtotal
            FROM sale_items si
            JOIN sales s ON s.id = si.sale_id
            WHERE si.item = ?
            ORDER BY s.timestamp DESC
        ''', (item,))
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows

def clear_default_password_marker():
    """Remove the one-time default-password notice marker file if present."""
    try:
        marker_path = os.path.join(os.getcwd(), 'data', '.default_password_notice_pending')
        if os.path.exists(marker_path):
            try:
                os.remove(marker_path)
            except Exception:
                # Non-fatal: ignore if cannot delete
                pass
    except Exception:
        # Be defensive
        pass
