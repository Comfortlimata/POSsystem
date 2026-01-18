import os
import sqlite3
import sys
from datetime import datetime

# Central DB name resolution (keeps compatibility with sales_utils DB selection)
DB_NAME = os.environ.get('BAR_SALES_DB') or os.path.join(os.getcwd(), 'bar_sales.db')
if 'unittest' in sys.modules and 'BAR_SALES_DB' not in os.environ:
    DB_NAME = 'test_bar_sales.db'


def get_conn(path=None, timeout=30):
    """Return a sqlite3 connection with common pragmas applied."""
    db = path or DB_NAME
    conn = sqlite3.connect(db, timeout=timeout, check_same_thread=False)
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('PRAGMA busy_timeout=30000')
    except Exception:
        pass
    return conn

