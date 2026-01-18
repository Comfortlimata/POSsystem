# hiring_system.py (stub)
"""
Hiring feature shim — this file intentionally disables the hiring feature while
preserving the public API so other modules won't raise ImportError/AttributeError.
All functions either do nothing or show a single message informing the user
that the Hiring feature has been removed.
"""

import os
import tkinter as tk
from tkinter import messagebox
from datetime import datetime

# Keep DB_NAME symbol available for compatibility
DB_NAME = os.path.join(os.getcwd(), 'bar_sales.db') if os.getcwd() else 'bar_sales.db'

def _notify_removed(parent=None):
    try:
        if parent is None:
            root = tk.Tk()
            root.withdraw()
            messagebox.showinfo("Feature Removed", "The Hiring feature has been removed from this build.")
            root.destroy()
        else:
            # Show a modal message using the provided parent window
            messagebox.showinfo("Feature Removed", "The Hiring feature has been removed from this build.")
    except Exception:
        # If GUI not available, silently pass
        pass

# Public API (compatibility)
def init_hiring_db():
    """Compatibility shim: no-op."""
    return None

def print_hire_receipt(hire_data: dict):
    """Compatibility shim: printing not supported."""
    return False

def save_hire_record(*args, **kwargs):
    """Compatibility shim: raises to indicate data operations are disabled."""
    raise RuntimeError("Hiring feature has been removed")

def update_hire_status(*args, **kwargs):
    raise RuntimeError("Hiring feature has been removed")

def get_all_hires(*args, **kwargs):
    return []

def search_hires(*args, **kwargs):
    return []

def get_hire_by_id(*args, **kwargs):
    return None

def export_hires_to_csv(*args, **kwargs):
    """Return None to indicate no export was created."""
    return None

def show_hiring_window(parent=None, current_user=None):
    """Show a simple message that hiring was removed (keeps callers safe)."""
    _notify_removed(parent)

# End of shim
