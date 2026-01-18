#!/usr/bin/env python3
"""
Database utilities with improved connection handling to prevent locks
"""

import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Generator

DB_NAME = "bar_sales.db"

# Thread-local storage for connections
_local = threading.local()

def get_connection() -> sqlite3.Connection:
    """Get a thread-local database connection with proper configuration"""
    if not hasattr(_local, 'connection') or _local.connection is None:
        _local.connection = sqlite3.connect(DB_NAME, timeout=30)
        # Configure connection for better concurrency
        _local.connection.execute("PRAGMA journal_mode=WAL")
        _local.connection.execute("PRAGMA synchronous=NORMAL")
        _local.connection.execute("PRAGMA busy_timeout=30000")
        _local.connection.execute("PRAGMA cache_size=10000")
        _local.connection.execute("PRAGMA temp_store=memory")
    return _local.connection

@contextmanager
def db_connection() -> Generator[sqlite3.Connection, None, None]:
    """Context manager for database connections that ensures proper cleanup"""
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME, timeout=30)
        # Configure connection
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        yield conn
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

@contextmanager
def db_transaction() -> Generator[sqlite3.Connection, None, None]:
    """Context manager for database transactions with automatic commit/rollback"""
    with db_connection() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

def execute_query(query: str, params: tuple = ()) -> Any:
    """Execute a query and return results"""
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

def execute_single(query: str, params: tuple = ()) -> Any:
    """Execute a query and return single result"""
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchone()

def execute_write(query: str, params: tuple = ()) -> int:
    """Execute a write query and return lastrowid"""
    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.lastrowid

def close_connection():
    """Close the thread-local connection"""
    if hasattr(_local, 'connection') and _local.connection:
        _local.connection.close()
        _local.connection = None

# Test the connection utilities
if __name__ == "__main__":
    print("Testing database connection utilities...")
    try:
        # Test basic connection
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            print(f"✓ Basic connection test: {result}")
        
        # Test transaction
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER)")
            cursor.execute("INSERT INTO test_table (id) VALUES (1)")
            cursor.execute("SELECT COUNT(*) FROM test_table")
            count = cursor.fetchone()[0]
            print(f"✓ Transaction test: {count} records")
            cursor.execute("DROP TABLE test_table")
        
        print("✅ All database utilities working correctly!")
        
    except Exception as e:
        print(f"❌ Database utilities test failed: {e}")