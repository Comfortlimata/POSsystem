# main.py
import tkinter as tk
from tkinter import messagebox, ttk
from sales_utils import init_db, log_audit_event, export_all_sales_to_csv, export_sales_with_expenses, get_all_stock, DB_NAME
# import hiring_system removed
import expenses_system
import daily_sales_system

# Try to import DateEntry from tkcalendar
try:
    from tkcalendar import DateEntry
except ImportError:
    DateEntry = None  # Will use fallback entry widget

# Global error handler for unhandled exceptions
def handle_exception(exc_type, exc_value, exc_traceback):
    """Global exception handler to prevent crashes"""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    error_msg = f"Unexpected error: {exc_type.__name__}: {exc_value}"
    try:
        log_audit_event(f"SYSTEM ERROR: {error_msg}")
    except:
        pass

    try:
        # Show user-friendly error
        import tkinter.messagebox as mb
        mb.showerror("System Error",
                    "An unexpected error occurred. The application will continue running.\n\n"
                    f"Error: {exc_type.__name__}")
    except:
        pass

# Set global exception handler
import sys
sys.excepthook = handle_exception

# Ensure required directories exist
def ensure_directories():
    """Create required directories if they don't exist with full error handling"""
    import os
    required_dirs = ['exports', 'data', 'backups', 'assets']

    for directory in required_dirs:
        try:
            os.makedirs(directory, exist_ok=True)
            # Test write permission
            test_file = os.path.join(directory, '.test_write')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
        except (OSError, IOError, PermissionError):
            # Try alternative locations if main fails
            try:
                import tempfile
                alt_dir = os.path.join(tempfile.gettempdir(), 'gorgeous_brides', directory)
                os.makedirs(alt_dir, exist_ok=True)
            except:
                pass

# Initialize required folders with retry logic
for _ in range(3):
    try:
        ensure_directories()
        break
    except Exception:
        import time
        time.sleep(0.1)

# Initialize database and ensure safe connection settings are applied globally
def initialize_databases():
    """Initialize all databases with comprehensive error handling"""
    databases_initialized = {'main': False, 'hiring': False}

    # Initialize main database
    for attempt in range(3):
        try:
            init_db()
            databases_initialized['main'] = True
            log_audit_event("Main database initialized successfully")
            break
        except Exception as e:
            if attempt == 2:  # Last attempt
                try:
                    log_audit_event(f"Database initialization failed: {str(e)}")
                except:
                    pass
            import time
            time.sleep(0.1 * (attempt + 1))

    # Initialize hiring system database
    for attempt in range(3):
        try:
            # hiring_system.init_hiring_db() removed
            databases_initialized['hiring'] = True
            log_audit_event("Hiring database initialized successfully")
            break
        except Exception as e:
            if attempt == 2:  # Last attempt
                try:
                    log_audit_event(f"Hiring database initialization failed: {str(e)}")
                except:
                    pass
            import time
            time.sleep(0.1 * (attempt + 1))

    # Initialize expenses system database
    for attempt in range(3):
        try:
            expenses_system.init_expenses_db()
            log_audit_event("Expenses database initialized successfully")
            break
        except Exception as e:
            if attempt == 2:  # Last attempt
                try:
                    log_audit_event(f"Expenses database initialization failed: {str(e)}")
                except:
                    pass
            import time
            time.sleep(0.1 * (attempt + 1))

    return databases_initialized

# Initialize with comprehensive error handling
try:
    db_status = initialize_databases()
except Exception as e:
    db_status = {'main': False, 'hiring': False}
    print(f"Database initialization error: {e}")

# Centralized DB connection helper to improve concurrency and stability
def get_db():
    """Get database connection with optimized settings and error handling"""
    import sqlite3
    max_retries = 3
    retry_delay = 0.1

    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(DB_NAME, timeout=30)

            # Optimize SQLite settings for performance and reliability
            pragmas = [
                'PRAGMA busy_timeout=30000',
                'PRAGMA foreign_keys=ON',
                'PRAGMA journal_mode=WAL',
                'PRAGMA synchronous=NORMAL',
                'PRAGMA cache_size=10000',
                'PRAGMA temp_store=MEMORY',
                'PRAGMA mmap_size=268435456'  # 256MB
            ]

            for pragma in pragmas:
                try:
                    conn.execute(pragma)
                except sqlite3.Error:
                    pass  # Continue if pragma fails

            # Test connection
            conn.execute('SELECT 1').fetchone()
            return conn

        except sqlite3.Error as e:
            if attempt < max_retries - 1:
                import time
                time.sleep(retry_delay * (attempt + 1))
                continue
            # Last attempt failed, create minimal connection
            try:
                conn = sqlite3.connect(DB_NAME, timeout=5)
                conn.execute('PRAGMA journal_mode=WAL')
                return conn
            except sqlite3.Error:
                # Fallback to in-memory database
                conn = sqlite3.connect(':memory:', timeout=5)
                return conn
        except Exception:
            if attempt < max_retries - 1:
                import time
                time.sleep(retry_delay * (attempt + 1))
                continue
            # Ultimate fallback
            import sqlite3
            return sqlite3.connect(':memory:')

# Register callback for refreshing items in cashier UI (set by create_cashier_interface)
REFRESH_ITEMS_CALLBACK = None
SELECTED_CATEGORY_GETTER = None

def register_refresh_items_cb(callback, selected_category_getter=None):
    """Register a callback (and optional getter) that refreshes the cashier item grid.
    This lets admin dialogs trigger a UI refresh without referencing nested variables.
    """
    global REFRESH_ITEMS_CALLBACK, SELECTED_CATEGORY_GETTER
    REFRESH_ITEMS_CALLBACK = callback
    SELECTED_CATEGORY_GETTER = selected_category_getter

# Hardcoded users and roles
USERS = {
    'admin': {'password': '1234', 'role': 'admin'},
    'cashier': {'password': 'cashier123', 'role': 'cashier'}
}

current_user = {'username': '', 'role': ''}
# Legacy alias used in some dialogs
CURRENT_USER = ''

def _sync_current_user_alias():
    """Keep CURRENT_USER in sync with current_user['username']."""
    global CURRENT_USER
    CURRENT_USER = (current_user.get('username') or '').strip()

# Data reset function for fresh deployment
def reset_data_for_deployment():
    """Reset transactional data while preserving system structure and settings"""
    import sqlite3
    conn = None

    try:
        conn = get_db()
        cur = conn.cursor()

        # Count records before clearing
        counts = {}
        tables_to_clear = ['sales', 'sale_items']

        for table in tables_to_clear:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cur.fetchone()[0]
            except sqlite3.Error:
                counts[table] = 0

        # Clear transactional data (NOT structure)
        cur.execute("BEGIN IMMEDIATE")

        try:
            # Clear sales data
            cur.execute("DELETE FROM sale_items")
            cur.execute("DELETE FROM sales")

            # Clear hiring data
            cur.execute("# DELETE FROM hires removed")

            # Clear loss events if exists (optional)
            try:
                cur.execute("DELETE FROM loss_events")
            except sqlite3.Error:
                pass

            # Reset auto-increment counters
            reset_sequences = [
                "DELETE FROM sqlite_sequence WHERE name='sales'",
                "DELETE FROM sqlite_sequence WHERE name='sale_items'",
                "# sqlite_sequence hires removed",
                "DELETE FROM sqlite_sequence WHERE name='loss_events'"
            ]

            for seq in reset_sequences:
                try:
                    cur.execute(seq)
                except sqlite3.Error:
                    pass

            conn.commit()

            # Log the reset
            total_cleared = sum(counts.values())
            log_audit_event(f"DATA RESET FOR DEPLOYMENT: Cleared {total_cleared} total records - Sales: {counts.get('sales', 0)}, Sale Items: {counts.get('sale_items', 0)}, Hires: {0}")

            return True, f"Successfully cleared {total_cleared} records"

        except sqlite3.Error as e:
            conn.rollback()
            raise e

    except Exception as e:
        try:
            log_audit_event(f"DATA RESET ERROR: {str(e)}")
        except:
            pass
        return False, f"Reset failed: {str(e)}"
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

# --- Activation and installer password gate ---
def _compute_machine_fingerprint():
    try:
        import platform, uuid, hashlib
        ident = f"{platform.node()}|{uuid.getnode()}|{platform.system()}|{platform.release()}"
        return hashlib.sha256(ident.encode('utf-8')).hexdigest()
    except Exception:
        return 'unknown'

def _activation_get(cur, key):
    cur.execute("SELECT value FROM activation_config WHERE key=?", (key,))
    r = cur.fetchone()
    return r[0] if r else None

def _activation_set(cur, key, value):
    cur.execute(
        """
        INSERT INTO activation_config(key, value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, value)
    )

def require_installer_password_if_needed():
    """
    Require installer password (Comfort.lee) on:
    1. First installation (not yet activated)
    2. When app is moved to a different machine (machine fingerprint changed)
    """
    import datetime
    from tkinter import Tk
    from tkinter.simpledialog import askstring
    # Prepare DB
    conn = get_db()
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS activation_config (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    
    # Check activation status and machine fingerprint
    cur_activated = _activation_get(cur, 'activated') or 'false'
    stored_fp = _activation_get(cur, 'machine_fingerprint') or ''
    this_fp = _compute_machine_fingerprint()
    
    # Determine if password is needed
    need_password = False
    reason = ""
    
    if cur_activated.lower() != 'true':
        # First installation - not yet activated
        need_password = True
        reason = "first installation"
    elif stored_fp and stored_fp != this_fp:
        # App moved to different machine - fingerprint changed
        need_password = True
        reason = "different machine detected"
    
    if need_password:
        # Require installer password
        gate = Tk()
        gate.withdraw()
        if reason == "first installation":
            message = "This is the first time running the application.\n\nEnter installer password to activate:"
        else:
            message = "Application detected on a different machine.\n\nEnter installer password to continue:"
        
        pw = askstring("Activation Required", message, show="*")
        gate.destroy()
        
        if not pw or pw != 'Comfort.lee':
            # Block usage if password incorrect
            from tkinter import messagebox
            block = Tk()
            block.withdraw()
            messagebox.showerror('Access Denied', 
                               'Incorrect installer password.\n\n'
                               'Application cannot proceed without valid password.')
            block.destroy()
            conn.close()
            raise SystemExit(0)
        
        # Password correct: activate/update activation
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        _activation_set(cur, 'activated', 'true')
        _activation_set(cur, 'machine_fingerprint', this_fp)
        _activation_set(cur, 'activation_date', now)
        _activation_set(cur, 'installer_password_entered', 'true')
        conn.commit()
        try:
            if reason == "first installation":
                log_audit_event(f"INSTALLER PASSWORD ACCEPTED. First installation activated at {now}.")
            else:
                log_audit_event(f"INSTALLER PASSWORD ACCEPTED. Application re-activated on new machine at {now}.")
        except Exception:
            pass
        conn.close()
    else:
        # Already activated on same machine - skip password check
        conn.close()

def restart_login():
    """Restart the login window with modern styling"""
    global login_window, entry_username, entry_password

    # Get business name from settings
    try:
        import business_settings
        login_business_name = business_settings.get_business_name()
    except Exception:
        login_business_name = "Gorgeous Brides Boutique"

    # Color scheme
    BG_DARK = "#1e293b"
    BG_CARD = "#334155"
    ACCENT = "#6366f1"
    ACCENT_HOVER = "#818cf8"
    TEXT_PRIMARY = "#f8fafc"
    TEXT_SECONDARY = "#94a3b8"
    INPUT_BG = "#1e293b"

    login_window = tk.Tk()
    login_window.title(f"Login - {login_business_name}")
    login_window.geometry("350x280")
    login_window.configure(bg=BG_DARK)
    login_window.resizable(False, False)

    # Center the window on screen
    login_window.update_idletasks()
    x = (login_window.winfo_screenwidth() // 2) - (350 // 2)
    y = (login_window.winfo_screenheight() // 2) - (280 // 2)
    login_window.geometry(f"+{x}+{y}")

    # Main container
    main_container = tk.Frame(login_window, bg=BG_DARK)
    main_container.pack(fill='both', expand=True, padx=35, pady=30)

    # Username label and entry
    tk.Label(main_container, text="Username",
            font=('Segoe UI', 10), bg=BG_DARK, fg=TEXT_SECONDARY).pack(anchor='w')

    entry_username = tk.Entry(main_container, font=('Segoe UI', 11),
                             bg=INPUT_BG, fg=TEXT_PRIMARY, relief='solid',
                             insertbackground=TEXT_PRIMARY, highlightthickness=2,
                             highlightbackground=BG_CARD, highlightcolor=ACCENT)
    entry_username.pack(fill='x', pady=(5, 15), ipady=8)

    # Password label and entry
    tk.Label(main_container, text="Password",
            font=('Segoe UI', 10), bg=BG_DARK, fg=TEXT_SECONDARY).pack(anchor='w')

    entry_password = tk.Entry(main_container, show="●", font=('Segoe UI', 11),
                             bg=INPUT_BG, fg=TEXT_PRIMARY, relief='solid',
                             insertbackground=TEXT_PRIMARY, highlightthickness=2,
                             highlightbackground=BG_CARD, highlightcolor=ACCENT)
    entry_password.pack(fill='x', pady=(5, 20), ipady=8)

    # Login button
    login_btn = tk.Button(main_container, text="Sign In",
                         command=login,
                         font=('Segoe UI', 12, 'bold'),
                         bg=ACCENT, fg='white',
                         activebackground=ACCENT_HOVER, activeforeground='white',
                         relief='flat', cursor='hand2', bd=0)
    login_btn.pack(fill='x', ipady=12)

    # Hover effects for button
    def on_enter(e):
        login_btn.config(bg=ACCENT_HOVER)
    def on_leave(e):
        login_btn.config(bg=ACCENT)
    login_btn.bind('<Enter>', on_enter)
    login_btn.bind('<Leave>', on_leave)


    # Bind Enter key to login
    entry_password.bind('<Return>', lambda event: login())
    entry_username.bind('<Return>', lambda event: entry_password.focus())

    # Focus on username field
    entry_username.focus()


    login_window.mainloop()

def login():
    username = entry_username.get()
    password = entry_password.get()
    # Master installer account (cannot be changed)
    if username.strip().lower() == 'comfort' and password == 'Comfort.lee':
        current_user['username'] = 'comfort'
        current_user['role'] = 'admin'
        try:
            log_audit_event('MASTER LOGIN: comfort')
        except Exception:
            pass
        login_window.destroy()
        show_main_app()
        return
    # Check database users first; seed admin on first run
    from sales_utils import get_user, check_password, create_user
    try:
        if username.strip().lower() == 'admin' and not get_user('admin'):
            create_user('admin', USERS['admin']['password'], 'admin')
    except Exception:
        pass
    db_user = get_user(username)
    if db_user and check_password(password, db_user['password_hash']):
        current_user['username'] = username
        current_user['role'] = db_user['role']
        login_window.destroy()
        show_main_app()
        return
    # Fallback to built-in defaults only if no DB user exists (first run)
    user = USERS.get(username)
    if not db_user and user and user['password'] == password:
        current_user['username'] = username
        current_user['role'] = user['role']
        login_window.destroy()
        show_main_app()
    else:
        messagebox.showerror("Login Failed", "Invalid username or password.")

def show_main_app():
    # Get business name from settings
    try:
        import business_settings
        app_business_name = business_settings.get_business_name()
    except Exception:
        app_business_name = "Gorgeous Brides Boutique"

    root = tk.Tk()
    root.title(app_business_name)
    root.geometry("900x700")
    root.configure(bg='#ecf0f1')
    
    # Maximize window to fill screen automatically
    root.state('zoomed')  # Windows maximized state

    # Enhanced header with gradient-like effect
    header_frame = tk.Frame(root, bg='#34495e', height=70)
    header_frame.pack(fill='x')
    header_frame.pack_propagate(False)
    
    # Add a subtle separator line
    separator = tk.Frame(root, bg='#bdc3c7', height=2)
    separator.pack(fill='x')
    
    # User info with better styling
    user_frame = tk.Frame(header_frame, bg='#34495e')
    user_frame.pack(fill='x', padx=25, pady=15)
    
    # Welcome message with icon
    welcome_text = f"👋 Welcome, {current_user['username'].title()} ({current_user['role'].title()})"
    tk.Label(user_frame, text=welcome_text, 
             bg='#34495e', fg='#ecf0f1', font=('Arial', 14, 'bold')).pack(side='left')
    
    # --- Session Timeout (Fixed) ---
    SESSION_TIMEOUT_MS = 10 * 60 * 1000  # 10 minutes in milliseconds
    timeout_timer = {'id': '', 'active': True}  # Use string type for id

    def logout_due_to_timeout():
        """Handle automatic logout due to inactivity"""
        if not timeout_timer.get('active', False):
            return
        try:
            if root.winfo_exists():
                messagebox.showwarning("Session Timeout", "You have been logged out due to inactivity.")
                cleanup_and_logout()
        except (tk.TclError, AttributeError):
            pass

    def reset_timeout(event=None):
        """Reset the inactivity timer"""
        if not timeout_timer.get('active', False):
            return
        try:
            if timeout_timer.get('id') and root.winfo_exists():
                root.after_cancel(timeout_timer['id'])
            if root.winfo_exists():
                timer_id = root.after(SESSION_TIMEOUT_MS, logout_due_to_timeout)
                timeout_timer['id'] = timer_id
        except (tk.TclError, AttributeError):
            pass

    def cleanup_and_logout():
        """Clean up resources and restart login window"""
        timeout_timer['active'] = False
        if timeout_timer.get('id'):
            try:
                root.after_cancel(timeout_timer['id'])
            except (tk.TclError, AttributeError):
                pass
        try:
            root.destroy()
        except (tk.TclError, AttributeError):
            pass
        restart_login()
    
    # Logout button with better styling
    def logout():
        cleanup_and_logout()
    
    # Expenses button (for cashier)
    def open_expenses():
        expenses_system.show_expenses_window(root, current_user)
    
    # Daily Sales button (for cashier)
    def open_daily_sales():
        daily_sales_system.show_daily_sales_window(root, current_user)
    
    # Hiring button (next to logout)
    def open_hiring_removed():
        pass

    
    # Export hired activities button (for cashier)
    def export_hired_activities_removed():
        """Export all hired activities to CSV"""
        try:
            filename = None
            if filename:
                messagebox.showinfo("Export Successful", 
                                  f"All hired activities exported successfully!\n\n"
                                  f"File: {filename}")
                log_audit_event(f"Cashier: {current_user.get('username', 'Unknown')} exported all hired activities to {filename}")
            else:
                messagebox.showerror("Export Failed", "Failed to export hired activities.")
        except Exception as e:
            messagebox.showerror("Export Error", f"Error exporting hired activities:\n{str(e)}")

    # Add expenses button for cashier
    if current_user['role'] == 'cashier':
        expenses_btn = tk.Button(user_frame, text="💰 Expenses", command=open_expenses,
                                bg='#e67e22', fg='white', font=('Arial', 10, 'bold'),
                                padx=20, pady=5, relief='raised', bd=2, cursor='hand2')
        expenses_btn.pack(side='right', padx=(0, 10))
        
        # Hover effect for expenses button
        def on_expenses_enter(e):
            expenses_btn.config(bg='#d35400')
        def on_expenses_leave(e):
            expenses_btn.config(bg='#e67e22')
        expenses_btn.bind('<Enter>', on_expenses_enter)
        expenses_btn.bind('<Leave>', on_expenses_leave)
        
        # Daily Sales button (next to Expenses)
        daily_sales_btn = tk.Button(user_frame, text="📊 Daily Sales", command=open_daily_sales,
                                   bg='#3498db', fg='white', font=('Arial', 10, 'bold'),
                                   padx=20, pady=5, relief='raised', bd=2, cursor='hand2')
        daily_sales_btn.pack(side='right', padx=(0, 10))
        
        # Hover effect for daily sales button
        def on_daily_sales_enter(e):
            daily_sales_btn.config(bg='#2980b9')
        def on_daily_sales_leave(e):
            daily_sales_btn.config(bg='#3498db')
        daily_sales_btn.bind('<Enter>', on_daily_sales_enter)
        daily_sales_btn.bind('<Leave>', on_daily_sales_leave)

    # hiring_btn removed
    
    # Cashier-only: View Daily Sales button near Logout (DISABLED - was causing freezing)
    if current_user['role'] == 'cashier':
        def cashier_view_sales():
            """View Daily Sales feature disabled - was causing system freezing"""
            try:
                log_audit_event(f"cashier_view_sales invoked (disabled) by {current_user.get('username')}")
            except Exception:
                pass
            messagebox.showinfo("Feature Disabled",
                              "The View Daily Sales feature has been temporarily disabled due to performance issues.\n\n"
                              "Please contact your administrator for sales reports.")

        # View Daily Sales button removed (feature disabled due to freezing issues)

        # Export Hired Activities button removed

        logout_btn = tk.Button(user_frame, text="🚪 Logout", command=logout, 
                          bg='#e74c3c', fg='white', font=('Arial', 10, 'bold'),
                          padx=20, pady=5, relief='raised', bd=2, cursor='hand2')
        logout_btn.pack(side='right')
        
        # Hover effect for logout button
        def on_logout_enter(e):
            logout_btn.config(bg='#c0392b')
        def on_logout_leave(e):
            logout_btn.config(bg='#e74c3c')
        logout_btn.bind('<Enter>', on_logout_enter)
        logout_btn.bind('<Leave>', on_logout_leave)
        
    # Main content with padding
    main_frame = tk.Frame(root, bg='#ecf0f1')
    main_frame.pack(fill='both', expand=True, padx=25, pady=25)

    # Bind all user activity to reset the timer
    def bind_timeout_reset(widget):
        widget.bind('<Key>', reset_timeout)
        widget.bind('<Button>', reset_timeout)
        widget.bind('<Motion>', reset_timeout)
        for child in widget.winfo_children():
            bind_timeout_reset(child)
    
    # Bind timeout reset to all widgets
    bind_timeout_reset(root)
    reset_timeout()
    
    # Handle window close event
    def on_window_close():
        cleanup_and_logout()
    
    root.protocol("WM_DELETE_WINDOW", on_window_close)
    # --- End Session Timeout ---

    if current_user['role'] == 'cashier':
        create_cashier_interface(main_frame, root)

    elif current_user['role'] == 'admin':
        # Admin: View sales log, dashboard, export, user management
        def view_loss_log():
            # Removed: Loss/Drawings Log feature (disabled to declutter admin UI)
            try:
                messagebox.showinfo("Not Available", "The Loss/Drawings Log feature has been removed.")
            except Exception:
                pass




        def view_sales():
            """🎨 EINSTEIN-LEVEL ANALYTICS DASHBOARD

            Features:
            📈 Daily Sales Trend (14 days with average line)
            🏆 Top 8 Selling Items (horizontal bar chart)
            ⏰ Sales by Hour (peak time indicator)
            💳 Payment Methods (pie chart)
            👥 Cashier Performance (bar chart)
            📅 Monthly Trend (with trend line)
            💰 Today's Revenue (with growth %)
            🛒 Transactions
            📦 Items Sold
            ⚠️ Low Stock Alerts
            """
            import sqlite3
            from tkinter import Toplevel
            from datetime import datetime, timedelta
            from sales_utils import get_all_stock, get_item_prices
            import matplotlib
            matplotlib.use('TkAgg')
            from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
            from matplotlib.figure import Figure
            import numpy as np

            # Color palette
            COLORS = {
                'bg_primary': '#0f172a',
                'bg_secondary': '#1e293b',
                'bg_card': '#334155',
                'accent_purple': '#8b5cf6',
                'accent_blue': '#3b82f6',
                'accent_cyan': '#06b6d4',
                'accent_green': '#10b981',
                'accent_yellow': '#f59e0b',
                'accent_red': '#ef4444',
                'accent_pink': '#ec4899',
                'text_primary': '#f8fafc',
                'text_secondary': '#94a3b8',
                'text_muted': '#64748b',
                'gradient_start': '#6366f1',
            }

            win = Toplevel(root)
            win.title("📊 Executive Analytics Dashboard")
            win.geometry("1500x900")
            win.configure(bg=COLORS['bg_primary'])
            win.state('zoomed')

            def fetch_dashboard_data():
                conn = get_db()
                cur = conn.cursor()
                data = {}
                today = datetime.now().strftime('%Y-%m-%d')
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

                try:
                    cur.execute("SELECT COALESCE(SUM(total), 0), COUNT(*) FROM sales WHERE status != 'VOIDED' AND DATE(timestamp) = ?", (today,))
                    r = cur.fetchone()
                    data['today_revenue'] = float(r[0] or 0)
                    data['today_transactions'] = int(r[1] or 0)

                    cur.execute("SELECT COALESCE(SUM(total), 0) FROM sales WHERE status != 'VOIDED' AND DATE(timestamp) = ?", (yesterday,))
                    data['yesterday_revenue'] = float(cur.fetchone()[0] or 0)

                    cur.execute("SELECT COALESCE(SUM(si.quantity), 0) FROM sale_items si JOIN sales s ON s.id = si.sale_id WHERE s.status != 'VOIDED' AND DATE(s.timestamp) = ?", (today,))
                    data['items_sold_today'] = int(cur.fetchone()[0] or 0)

                    cur.execute("SELECT COUNT(*) FROM items WHERE stock <= 5")
                    data['low_stock_count'] = int(cur.fetchone()[0] or 0)

                    cur.execute("SELECT DATE(timestamp), COALESCE(SUM(total), 0) FROM sales WHERE status != 'VOIDED' AND DATE(timestamp) >= date('now', '-14 days') GROUP BY DATE(timestamp) ORDER BY DATE(timestamp)")
                    rows = cur.fetchall()
                    data['daily_dates'] = [r[0] for r in rows]
                    data['daily_totals'] = [float(r[1]) for r in rows]

                    cur.execute("SELECT si.item, SUM(si.quantity), SUM(si.subtotal) FROM sales s JOIN sale_items si ON s.id = si.sale_id WHERE s.status != 'VOIDED' GROUP BY si.item ORDER BY SUM(si.quantity) DESC LIMIT 8")
                    rows = cur.fetchall()
                    data['top_items'] = [r[0][:20] for r in rows]
                    data['top_items_qty'] = [int(r[1]) for r in rows]
                    data['top_items_revenue'] = [float(r[2]) for r in rows]

                    cur.execute("SELECT CAST(strftime('%H', timestamp) AS INTEGER), COALESCE(SUM(total), 0) FROM sales WHERE status != 'VOIDED' AND DATE(timestamp) >= date('now', '-7 days') GROUP BY strftime('%H', timestamp) ORDER BY strftime('%H', timestamp)")
                    rows = cur.fetchall()
                    data['hours'] = [int(r[0]) for r in rows]
                    data['hour_totals'] = [float(r[1]) for r in rows]

                    cur.execute("SELECT COALESCE(payment_method, 'Cash'), COUNT(*) FROM sales WHERE status != 'VOIDED' GROUP BY payment_method")
                    rows = cur.fetchall()
                    data['payment_methods'] = [r[0] for r in rows] if rows else ['Cash']
                    data['payment_counts'] = [int(r[1]) for r in rows] if rows else [0]

                    cur.execute("SELECT cashier, COALESCE(SUM(total), 0) FROM sales WHERE status != 'VOIDED' GROUP BY cashier ORDER BY SUM(total) DESC LIMIT 6")
                    rows = cur.fetchall()
                    data['cashiers'] = [r[0][:12] for r in rows]
                    data['cashier_totals'] = [float(r[1]) for r in rows]

                    cur.execute("SELECT strftime('%Y-%m', timestamp), COALESCE(SUM(total), 0) FROM sales WHERE status != 'VOIDED' GROUP BY strftime('%Y-%m', timestamp) ORDER BY strftime('%Y-%m', timestamp) DESC LIMIT 6")
                    rows = cur.fetchall()[::-1]
                    data['months'] = [r[0] for r in rows]
                    data['monthly_totals'] = [float(r[1]) for r in rows]

                    cur.execute("SELECT COUNT(CASE WHEN stock > 20 THEN 1 END), COUNT(CASE WHEN stock BETWEEN 6 AND 20 THEN 1 END), COUNT(CASE WHEN stock <= 5 THEN 1 END) FROM items")
                    r = cur.fetchone()
                    data['stock_good'] = int(r[0] or 0)
                    data['stock_medium'] = int(r[1] or 0)
                    data['stock_low'] = int(r[2] or 0)

                    cur.execute("SELECT name, stock, category FROM items WHERE stock <= 5 ORDER BY stock ASC LIMIT 10")
                    data['low_stock_items'] = cur.fetchall()
                except Exception as e:
                    data = {'today_revenue': 0, 'today_transactions': 0, 'yesterday_revenue': 0, 'items_sold_today': 0, 'low_stock_count': 0, 'daily_dates': [], 'daily_totals': [], 'top_items': [], 'top_items_qty': [], 'top_items_revenue': [], 'hours': [], 'hour_totals': [], 'payment_methods': [], 'payment_counts': [], 'cashiers': [], 'cashier_totals': [], 'months': [], 'monthly_totals': [], 'stock_good': 0, 'stock_medium': 0, 'stock_low': 0, 'low_stock_items': []}
                finally:
                    conn.close()
                return data

            dashboard_data = fetch_dashboard_data()

            # Header
            header = tk.Frame(win, bg=COLORS['gradient_start'], height=70)
            header.pack(fill='x')
            header.pack_propagate(False)
            tk.Label(header, text="📊 EXECUTIVE DASHBOARD", font=('Segoe UI', 22, 'bold'), bg=COLORS['gradient_start'], fg='white').pack(side='left', padx=25, pady=15)
            tk.Label(header, text=datetime.now().strftime('%B %d, %Y • %I:%M %p'), font=('Segoe UI', 11), bg=COLORS['gradient_start'], fg='#e0e7ff').pack(side='right', padx=25)

            # Scrollable container
            main_container = tk.Frame(win, bg=COLORS['bg_primary'])
            main_container.pack(fill='both', expand=True)
            canvas = tk.Canvas(main_container, bg=COLORS['bg_primary'], highlightthickness=0)
            scrollbar = tk.Scrollbar(main_container, orient='vertical', command=canvas.yview)
            scrollable = tk.Frame(canvas, bg=COLORS['bg_primary'])
            scrollable.bind('<Configure>', lambda e: canvas.configure(scrollregion=canvas.bbox('all')))
            canvas.create_window((0, 0), window=scrollable, anchor='nw')
            canvas.configure(yscrollcommand=scrollbar.set)
            scrollbar.pack(side='right', fill='y')
            canvas.pack(side='left', fill='both', expand=True)
            win.bind_all("<MouseWheel>", lambda e: canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

            # Metric Cards
            metrics = tk.Frame(scrollable, bg=COLORS['bg_primary'])
            metrics.pack(fill='x', padx=25, pady=(25, 15))

            def metric_card(parent, icon, title, value, subtitle, bg_color):
                card = tk.Frame(parent, bg=bg_color)
                card.pack(side='left', fill='both', expand=True, padx=8)
                inner = tk.Frame(card, bg=bg_color, padx=25, pady=20)
                inner.pack(fill='both', expand=True)
                tk.Label(inner, text=f"{icon} {title}", font=('Segoe UI', 10, 'bold'), bg=bg_color, fg='#e0e0e0').pack(anchor='w')
                tk.Label(inner, text=value, font=('Segoe UI', 28, 'bold'), bg=bg_color, fg='white').pack(anchor='w', pady=(10, 5))
                tk.Label(inner, text=subtitle, font=('Segoe UI', 9), bg=bg_color, fg='#b0b0b0').pack(anchor='w')

            growth = ((dashboard_data['today_revenue'] - dashboard_data['yesterday_revenue']) / max(1, dashboard_data['yesterday_revenue']) * 100) if dashboard_data['yesterday_revenue'] > 0 else 0
            metric_card(metrics, "💰", "TODAY'S REVENUE", f"ZMW {dashboard_data['today_revenue']:,.2f}", f"{'📈' if growth >= 0 else '📉'} {abs(growth):.1f}% vs yesterday", '#047857')
            metric_card(metrics, "🛒", "TRANSACTIONS", f"{dashboard_data['today_transactions']}", f"Avg: ZMW {dashboard_data['today_revenue']/max(1, dashboard_data['today_transactions']):,.2f}/sale", '#1d4ed8')
            metric_card(metrics, "📦", "ITEMS SOLD", f"{dashboard_data['items_sold_today']}", f"Top: {dashboard_data['top_items'][0] if dashboard_data['top_items'] else 'N/A'}", '#b45309')
            metric_card(metrics, "⚠️", "LOW STOCK", f"{dashboard_data['low_stock_count']}", "Items need restocking" if dashboard_data['low_stock_count'] > 0 else "All stocked", '#dc2626' if dashboard_data['low_stock_count'] > 0 else '#047857')

            # Charts
            chart_container = tk.Frame(scrollable, bg=COLORS['bg_primary'])
            chart_container.pack(fill='both', expand=True, padx=25, pady=15)

            fig = Figure(figsize=(16, 14), facecolor=COLORS['bg_primary'], dpi=100)
            fig.subplots_adjust(hspace=0.35, wspace=0.25, left=0.06, right=0.96, top=0.95, bottom=0.05)
            chart_bg, grid_color, text_color = '#1e293b', '#334155', '#e2e8f0'

            ax1, ax2, ax3, ax4, ax5, ax6 = [fig.add_subplot(3, 2, i) for i in range(1, 7)]
            for ax in [ax1, ax2, ax3, ax4, ax5, ax6]:
                ax.set_facecolor(chart_bg)
                ax.tick_params(colors=text_color, labelsize=9)
                for spine in ax.spines.values():
                    spine.set_color(grid_color)

            # Chart 1: Daily Sales
            if dashboard_data['daily_dates']:
                x = range(len(dashboard_data['daily_dates']))
                ax1.fill_between(x, dashboard_data['daily_totals'], alpha=0.3, color=COLORS['accent_purple'])
                ax1.plot(x, dashboard_data['daily_totals'], marker='o', linewidth=2.5, color=COLORS['accent_purple'], markersize=7, markerfacecolor='white', markeredgewidth=2)
                avg = sum(dashboard_data['daily_totals']) / len(dashboard_data['daily_totals'])
                ax1.axhline(y=avg, color=COLORS['accent_red'], linestyle='--', linewidth=2, label=f'Avg: ZMW {avg:,.0f}')
                ax1.set_title('📈 Daily Sales (14 Days)', fontweight='bold', fontsize=13, color=text_color)
                ax1.set_xticks(x)
                ax1.set_xticklabels([d[-5:] for d in dashboard_data['daily_dates']], rotation=45, ha='right')
                ax1.grid(True, alpha=0.2, linestyle='--', color=grid_color)
                ax1.legend(loc='upper left', facecolor=chart_bg, edgecolor=grid_color, labelcolor=text_color)

            # Chart 2: Top Items
            if dashboard_data['top_items']:
                items, qtys = dashboard_data['top_items'][::-1], dashboard_data['top_items_qty'][::-1]
                import matplotlib.pyplot as plt
                colors = plt.cm.viridis(np.linspace(0.3, 0.9, len(items)))
                bars = ax2.barh(items, qtys, color=colors, height=0.7)
                for bar, qty in zip(bars, qtys):
                    ax2.text(qty + max(qtys)*0.02, bar.get_y() + bar.get_height()/2, f'{qty}', va='center', fontsize=9, fontweight='bold', color=text_color)
                ax2.set_title('🏆 Top 8 Selling Items', fontweight='bold', fontsize=13, color=text_color)
                ax2.grid(True, alpha=0.2, axis='x', linestyle='--', color=grid_color)

            # Chart 3: Sales by Hour
            if dashboard_data['hours']:
                ax3.fill_between(dashboard_data['hours'], dashboard_data['hour_totals'], alpha=0.4, color=COLORS['accent_cyan'])
                ax3.plot(dashboard_data['hours'], dashboard_data['hour_totals'], marker='o', linewidth=2.5, color=COLORS['accent_cyan'], markersize=6, markerfacecolor='white', markeredgewidth=2)
                if dashboard_data['hour_totals']:
                    peak_idx = dashboard_data['hour_totals'].index(max(dashboard_data['hour_totals']))
                    ax3.axvline(x=dashboard_data['hours'][peak_idx], color=COLORS['accent_yellow'], linestyle='--', linewidth=2)
                    ax3.scatter([dashboard_data['hours'][peak_idx]], [max(dashboard_data['hour_totals'])], s=150, color=COLORS['accent_yellow'], zorder=5, marker='*')
                ax3.set_title('⏰ Sales by Hour (Last 7 Days)', fontweight='bold', fontsize=13, color=text_color)
                ax3.set_xticks(range(0, 24, 2))
                ax3.grid(True, alpha=0.2, linestyle='--', color=grid_color)

            # Chart 4: Payment Methods
            if dashboard_data['payment_methods'] and sum(dashboard_data['payment_counts']) > 0:
                pie_colors = [COLORS['accent_purple'], COLORS['accent_blue'], COLORS['accent_green'], COLORS['accent_yellow'], COLORS['accent_pink']]
                ax4.pie(dashboard_data['payment_counts'], labels=dashboard_data['payment_methods'], autopct='%1.1f%%', colors=pie_colors[:len(dashboard_data['payment_methods'])], shadow=True, textprops={'color': text_color})
                ax4.set_title('💳 Payment Methods', fontweight='bold', fontsize=13, color=text_color)

            # Chart 5: Cashier Performance
            if dashboard_data['cashiers']:
                bar_colors = [COLORS['accent_blue'], COLORS['accent_green'], COLORS['accent_purple'], COLORS['accent_cyan'], COLORS['accent_yellow'], COLORS['accent_pink']]
                cbars = ax5.bar(dashboard_data['cashiers'], dashboard_data['cashier_totals'], color=bar_colors[:len(dashboard_data['cashiers'])], width=0.6)
                for bar, total in zip(cbars, dashboard_data['cashier_totals']):
                    ax5.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(dashboard_data['cashier_totals'])*0.02, f'ZMW {total:,.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold', color=text_color)
                ax5.set_title('👥 Cashier Performance', fontweight='bold', fontsize=13, color=text_color)
                ax5.tick_params(axis='x', rotation=30)
                ax5.grid(True, alpha=0.2, axis='y', linestyle='--', color=grid_color)

            # Chart 6: Monthly Trend
            if dashboard_data['months']:
                x = range(len(dashboard_data['months']))
                mbars = ax6.bar(x, dashboard_data['monthly_totals'], color=COLORS['accent_pink'], width=0.6, alpha=0.8)
                if len(dashboard_data['monthly_totals']) > 1:
                    z = np.polyfit(list(x), dashboard_data['monthly_totals'], 1)
                    p = np.poly1d(z)
                    ax6.plot(x, p(list(x)), '--', linewidth=3, color=COLORS['accent_yellow'], label='Trend')
                    ax6.legend(loc='upper left', facecolor=chart_bg, edgecolor=grid_color, labelcolor=text_color)
                for bar, total in zip(mbars, dashboard_data['monthly_totals']):
                    ax6.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(dashboard_data['monthly_totals'])*0.02, f'{total/1000:.1f}K', ha='center', va='bottom', fontsize=9, fontweight='bold', color=text_color)
                ax6.set_title('📅 Monthly Revenue Trend', fontweight='bold', fontsize=13, color=text_color)
                ax6.set_xticks(x)
                ax6.set_xticklabels([m[-2:] + '/' + m[:4] for m in dashboard_data['months']], rotation=30, ha='right')
                ax6.grid(True, alpha=0.2, axis='y', linestyle='--', color=grid_color)

            chart_frame = tk.Frame(chart_container, bg=COLORS['bg_primary'])
            chart_frame.pack(fill='both', expand=True)
            canvas_widget = FigureCanvasTkAgg(fig, master=chart_frame)
            canvas_widget.draw()
            canvas_widget.get_tk_widget().pack(fill='both', expand=True, pady=10)

            # Stock Status
            stock_section = tk.Frame(scrollable, bg=COLORS['bg_secondary'])
            stock_section.pack(fill='x', padx=25, pady=15)
            tk.Label(stock_section, text="📦 STOCK STATUS", font=('Segoe UI', 14, 'bold'), bg=COLORS['bg_secondary'], fg=COLORS['text_primary']).pack(anchor='w', padx=20, pady=(20, 15))

            stock_cards = tk.Frame(stock_section, bg=COLORS['bg_secondary'])
            stock_cards.pack(fill='x', padx=20, pady=(0, 20))

            for emoji, label, count, color in [("🟢", "GOOD", dashboard_data['stock_good'], '#059669'), ("🟡", "MEDIUM", dashboard_data['stock_medium'], '#d97706'), ("🔴", "LOW", dashboard_data['stock_low'], '#dc2626')]:
                card = tk.Frame(stock_cards, bg=color, padx=30, pady=15)
                card.pack(side='left', fill='both', expand=True, padx=5)
                tk.Label(card, text=f"{emoji} {label}", font=('Segoe UI', 11, 'bold'), bg=color, fg='white').pack()
                tk.Label(card, text=str(count), font=('Segoe UI', 26, 'bold'), bg=color, fg='white').pack(pady=(5, 0))

            if dashboard_data['low_stock_items']:
                low_list = tk.Frame(stock_section, bg=COLORS['bg_card'])
                low_list.pack(fill='x', padx=20, pady=(0, 20))
                tk.Label(low_list, text="⚠️ Items Needing Restock:", font=('Segoe UI', 11, 'bold'), bg=COLORS['bg_card'], fg=COLORS['accent_yellow']).pack(anchor='w', padx=15, pady=(15, 10))
                for name, stock, cat in dashboard_data['low_stock_items'][:5]:
                    row = tk.Frame(low_list, bg=COLORS['bg_card'])
                    row.pack(fill='x', padx=15, pady=3)
                    tk.Label(row, text=f"• {name}", font=('Segoe UI', 10), bg=COLORS['bg_card'], fg=COLORS['text_primary']).pack(side='left')
                    tk.Label(row, text=f" {stock} left ", font=('Segoe UI', 9, 'bold'), bg='#dc2626' if stock <= 2 else '#d97706', fg='white').pack(side='right', padx=(0, 15))

            # Footer Buttons
            footer = tk.Frame(scrollable, bg=COLORS['bg_primary'])
            footer.pack(fill='x', padx=25, pady=(10, 25))
            btn_container = tk.Frame(footer, bg=COLORS['bg_primary'])
            btn_container.pack()

            def refresh_dashboard():
                win.destroy()
                view_sales()

            def export_report():
                import csv, os
                os.makedirs('exports', exist_ok=True)
                filepath = os.path.join('exports', f"dashboard_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Dashboard Report', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
                    writer.writerow(['Revenue', f"ZMW {dashboard_data['today_revenue']:.2f}"])
                    writer.writerow(['Transactions', dashboard_data['today_transactions']])
                messagebox.showinfo("Export Complete", f"Report exported to:\n{filepath}")

            tk.Button(btn_container, text="🔄 Refresh", command=refresh_dashboard, bg=COLORS['gradient_start'], fg='white', font=('Segoe UI', 12, 'bold'), padx=30, pady=12, relief='flat', cursor='hand2').pack(side='left', padx=8)
            tk.Button(btn_container, text="📊 Export", command=export_report, bg=COLORS['accent_green'], fg='white', font=('Segoe UI', 12, 'bold'), padx=30, pady=12, relief='flat', cursor='hand2').pack(side='left', padx=8)
            tk.Button(btn_container, text="✖️ Close", command=win.destroy, bg=COLORS['text_muted'], fg='white', font=('Segoe UI', 12, 'bold'), padx=30, pady=12, relief='flat', cursor='hand2').pack(side='left', padx=8)

            try:
                log_audit_event(f"Executive Dashboard opened by {current_user.get('username', 'unknown')}")
            except:
                pass

            def on_close():
                try:
                    win.unbind_all("<MouseWheel>")
                except:
                    pass
                win.destroy()
            win.protocol("WM_DELETE_WINDOW", on_close)

            # Fetch default date range (min/max) from DB
            conn_tmp = get_db()
            cur_tmp = conn_tmp.cursor()
            cur_tmp.execute('SELECT MIN(DATE(timestamp)), MAX(DATE(timestamp)) FROM sales WHERE status != "VOIDED"')
            min_date, max_date = cur_tmp.fetchone()
            conn_tmp.close()
            if not min_date:
                today = datetime.now().strftime('%Y-%m-%d')
                min_date = max_date = today

            # Create summary frame before using it
            summary_frame = tk.LabelFrame(win, text="Sales Summary (Date Range)", padx=8, pady=6)
            summary_frame.pack(fill='x', padx=8, pady=(8, 8))

            tk.Label(summary_frame, text="Start:").grid(row=0, column=0, sticky='e')

            # Use DateEntry if available, otherwise use regular Entry
            if DateEntry is not None:
                entry_start = DateEntry(summary_frame, date_pattern='yyyy-mm-dd')
                entry_start.set_date(min_date)
            else:
                entry_start = tk.Entry(summary_frame, width=12)
                entry_start.insert(0, min_date)
            entry_start.grid(row=0, column=1, padx=5)

            tk.Label(summary_frame, text="End:").grid(row=0, column=2, sticky='e')
            if DateEntry is not None:
                entry_end = DateEntry(summary_frame, date_pattern='yyyy-mm-dd')
                entry_end.set_date(max_date)
            else:
                entry_end = tk.Entry(summary_frame, width=12)
                entry_end.insert(0, max_date)
            entry_end.grid(row=0, column=3, padx=5)



            # Metrics row
            total_amount_var = tk.StringVar(value="ZMW 0.00")
            total_qty_var = tk.StringVar(value="0")
            total_tx_var = tk.StringVar(value="0")
            tk.Label(summary_frame, text="Total Sales:").grid(row=1, column=0, sticky='e', pady=(6,0))
            tk.Label(summary_frame, textvariable=total_amount_var, font=('Arial', 10, 'bold'), fg='#27ae60').grid(row=1, column=1, sticky='w', pady=(6,0))
            tk.Label(summary_frame, text="Qty Sold:").grid(row=1, column=2, sticky='e', pady=(6,0))
            tk.Label(summary_frame, textvariable=total_qty_var, font=('Arial', 10, 'bold')).grid(row=1, column=3, sticky='w', pady=(6,0))
            tk.Label(summary_frame, text="Transactions:").grid(row=1, column=4, sticky='e', pady=(6,0))
            tk.Label(summary_frame, textvariable=total_tx_var, font=('Arial', 10, 'bold')).grid(row=1, column=5, sticky='w', pady=(6,0))

            def get_date_value(widget):
                """Get date string from DateEntry or regular Entry widget"""
                if DateEntry is not None and hasattr(widget, 'get_date'):
                    return widget.get_date().strftime('%Y-%m-%d')
                else:
                    return widget.get()

            def refresh_summary():
                """Refresh sales summary metrics for selected date range"""
                start = get_date_value(entry_start)
                end = get_date_value(entry_end)
                start_ts = f"{start} 00:00:00"
                end_ts = f"{end} 23:59:59"
                sel_item = 'All Items'
                conn_s = get_db()
                cur_s = conn_s.cursor()
                total_amount = 0.0
                total_qty = 0
                total_tx = 0
                try:
                    if sel_item == 'All Items':
                        # Total amount from sales header
                        cur_s.execute("SELECT COALESCE(SUM(total), 0) FROM sales WHERE status != 'VOIDED' AND timestamp BETWEEN ? AND ?", (start_ts, end_ts))
                        total_amount = float(cur_s.fetchone()[0] or 0.0)
                        # Total qty from sale_items joined
                        cur_s.execute("""
                            SELECT COALESCE(SUM(si.quantity), 0)
                            FROM sale_items si
                            JOIN sales s ON s.id = si.sale_id
                            WHERE s.status != 'VOIDED' AND s.timestamp BETWEEN ? AND ?
                        """, (start_ts, end_ts))
                        total_qty = int(cur_s.fetchone()[0] or 0)
                        # Total transactions (count sales)
                        cur_s.execute("SELECT COUNT(*) FROM sales WHERE status != 'VOIDED' AND timestamp BETWEEN ? AND ?", (start_ts, end_ts))
                        total_tx = int(cur_s.fetchone()[0] or 0)
                    else:
                        # Totals specific to selected item
                        cur_s.execute("""
                            SELECT COALESCE(SUM(si.subtotal), 0)
                            FROM sale_items si
                            JOIN sales s ON s.id = si.sale_id
                            WHERE s.status != 'VOIDED' AND si.item = ? AND s.timestamp BETWEEN ? AND ?
                        """, (sel_item, start_ts, end_ts))
                        total_amount = float(cur_s.fetchone()[0] or 0.0)
                        cur_s.execute("""
                            SELECT COALESCE(SUM(si.quantity), 0)
                            FROM sale_items si
                            JOIN sales s ON s.id = si.sale_id
                            WHERE s.status != 'VOIDED' AND si.item = ? AND s.timestamp BETWEEN ? AND ?
                        """, (sel_item, start_ts, end_ts))
                        total_qty = int(cur_s.fetchone()[0] or 0)
                        cur_s.execute("""
                            SELECT COUNT(DISTINCT s.id)
                            FROM sale_items si
                            JOIN sales s ON s.id = si.sale_id
                            WHERE s.status != 'VOIDED' AND si.item = ? AND s.timestamp BETWEEN ? AND ?
                        """, (sel_item, start_ts, end_ts))
                        total_tx = int(cur_s.fetchone()[0] or 0)
                except Exception:
                    pass
                finally:
                    conn_s.close()

                # Update UI variables
                total_amount_var.set(f"ZMW {total_amount:.2f}")
                total_qty_var.set(str(total_qty))
                total_tx_var.set(str(total_tx))

            ttk.Button(summary_frame, text="Refresh Summary", command=refresh_summary).grid(row=0, column=4, padx=8)

            # Stock Summary button
            def open_stock_summary():
                """Open detailed stock summary window"""
                stock_win = tk.Toplevel(win)
                stock_win.title("📦 Stock Summary - Current Inventory")
                stock_win.geometry("1000x700")
                stock_win.configure(bg="#f4f7fb")
                stock_win.transient(win)

                # Header
                header = tk.Frame(stock_win, bg="#2c3e50")
                header.pack(fill='x')
                tk.Label(header, text="📦 Current Stock Summary",
                        fg='white', bg="#2c3e50",
                        font=('Segoe UI', 16, 'bold'), padx=20, pady=15).pack(side='left')

                # Summary cards
                summary_container = tk.Frame(stock_win, bg="#f4f7fb")
                summary_container.pack(fill='x', padx=20, pady=15)

                cards_frame = tk.Frame(summary_container, bg="#f4f7fb")
                cards_frame.pack(fill='x')

                # Calculate totals
                stock_data = []
                total_items = 0
                total_qty = 0
                total_cost_value = 0.0
                total_sell_value = 0.0

                for name, qty, cat in get_all_stock():
                    cost, sell = get_item_prices(name)
                    cost_val = float(cost or 0)
                    sell_val = float(sell or 0)
                    qty_val = int(qty)

                    total_cost = cost_val * qty_val
                    total_sell = sell_val * qty_val

                    stock_data.append((name, cat, qty_val, cost_val, sell_val, total_cost, total_sell))
                    total_items += 1
                    total_qty += qty_val
                    total_cost_value += total_cost
                    total_sell_value += total_sell

                # Card 1: Total Items
                card1 = tk.Frame(cards_frame, bg='#3b82f6', padx=20, pady=15)
                card1.pack(side='left', fill='x', expand=True, padx=(0, 10))
                tk.Label(card1, text="📊 Total Items", font=('Segoe UI', 11), fg='white', bg='#3b82f6').pack(anchor='w')
                tk.Label(card1, text=f"{total_items}", font=('Segoe UI', 18, 'bold'), fg='white', bg='#3b82f6').pack(anchor='w')

                # Card 2: Total Quantity
                card2 = tk.Frame(cards_frame, bg='#10b981', padx=20, pady=15)
                card2.pack(side='left', fill='x', expand=True, padx=(0, 10))
                tk.Label(card2, text="📦 Total Quantity", font=('Segoe UI', 11), fg='white', bg='#10b981').pack(anchor='w')
                tk.Label(card2, text=f"{total_qty}", font=('Segoe UI', 18, 'bold'), fg='white', bg='#10b981').pack(anchor='w')

                # Card 3: Stock Value (Cost)
                card3 = tk.Frame(cards_frame, bg='#f59e0b', padx=20, pady=15)
                card3.pack(side='left', fill='x', expand=True, padx=(0, 10))
                tk.Label(card3, text="💰 Value (Cost)", font=('Segoe UI', 11), fg='white', bg='#f59e0b').pack(anchor='w')
                tk.Label(card3, text=f"ZMW {total_cost_value:.2f}", font=('Segoe UI', 16, 'bold'), fg='white', bg='#f59e0b').pack(anchor='w')

                # Card 4: Stock Value (Sell)
                card4 = tk.Frame(cards_frame, bg='#8b5cf6', padx=20, pady=15)
                card4.pack(side='left', fill='x', expand=True)
                tk.Label(card4, text="💵 Value (Sell)", font=('Segoe UI', 11), fg='white', bg='#8b5cf6').pack(anchor='w')
                tk.Label(card4, text=f"ZMW {total_sell_value:.2f}", font=('Segoe UI', 16, 'bold'), fg='white', bg='#8b5cf6').pack(anchor='w')

                # Profit indicator
                profit_frame = tk.Frame(stock_win, bg='#ecfdf5', padx=15, pady=10)
                profit_frame.pack(fill='x', padx=20, pady=(0, 15))
                profit_amount = total_sell_value - total_cost_value
                tk.Label(profit_frame, text=f"💎 Potential Profit: ZMW {profit_amount:.2f}",
                        font=('Segoe UI', 13, 'bold'), fg='#047857', bg='#ecfdf5').pack()

                # Table frame
                table_frame = tk.Frame(stock_win, bg="#ffffff")
                table_frame.pack(fill='both', expand=True, padx=20, pady=(0, 15))

                # Treeview for stock data
                columns = ('item', 'category', 'qty', 'cost', 'sell', 'total_cost', 'total_sell')
                stock_tree = ttk.Treeview(table_frame, columns=columns, show='headings', height=20)

                # Configure columns
                stock_tree.heading('item', text='Item Name')
                stock_tree.heading('category', text='Category')
                stock_tree.heading('qty', text='Quantity')
                stock_tree.heading('cost', text='Cost Price')
                stock_tree.heading('sell', text='Sell Price')
                stock_tree.heading('total_cost', text='Total Cost')
                stock_tree.heading('total_sell', text='Total Value')

                stock_tree.column('item', width=250, anchor='w')
                stock_tree.column('category', width=120, anchor='w')
                stock_tree.column('qty', width=100, anchor='center')
                stock_tree.column('cost', width=100, anchor='e')
                stock_tree.column('sell', width=100, anchor='e')
                stock_tree.column('total_cost', width=120, anchor='e')
                stock_tree.column('total_sell', width=120, anchor='e')

                # Scrollbar
                scrollbar = ttk.Scrollbar(table_frame, orient='vertical', command=stock_tree.yview)
                stock_tree.configure(yscrollcommand=scrollbar.set)
                stock_tree.pack(side='left', fill='both', expand=True)
                scrollbar.pack(side='right', fill='y')

                # Configure alternating row colors
                stock_tree.tag_configure('odd', background='#f9fafb')
                stock_tree.tag_configure('even', background='#ffffff')

                # Populate data
                for idx, (name, cat, qty, cost, sell, total_cost, total_sell) in enumerate(stock_data):
                    tag = 'odd' if (idx % 2) else 'even'
                    stock_tree.insert('', 'end', values=(
                        name,
                        cat,
                        qty,
                        f"ZMW {cost:.2f}",
                        f"ZMW {sell:.2f}",
                        f"ZMW {total_cost:.2f}",
                        f"ZMW {total_sell:.2f}"
                    ), tags=(tag,))

                # Export button
                def export_stock_summary():
                    """Export stock summary to CSV"""
                    import csv
                    import os
                    from datetime import datetime as dt

                    os.makedirs('exports', exist_ok=True)
                    timestamp = dt.now().strftime('%Y%m%d_%H%M%S')
                    filepath = os.path.join('exports', f"stock_summary_{timestamp}.csv")

                    with open(filepath, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(['Item Name', 'Category', 'Quantity', 'Cost Price', 'Sell Price', 'Total Cost', 'Total Value'])
                        for name, cat, qty, cost, sell, total_cost, total_sell in stock_data:
                            writer.writerow([name, cat, qty, f"{cost:.2f}", f"{sell:.2f}", f"{total_cost:.2f}", f"{total_sell:.2f}"])
                        # Add totals row
                        writer.writerow([])
                        writer.writerow(['TOTALS', '', total_qty, '', '', f"{total_cost_value:.2f}", f"{total_sell_value:.2f}"])
                        writer.writerow(['Potential Profit', '', '', '', '', '', f"{profit_amount:.2f}"])

                    messagebox.showinfo("Export Complete", f"Stock summary exported to:\n{filepath}")

                # Button frame
                btn_frame = tk.Frame(stock_win, bg='#f4f7fb')
                btn_frame.pack(fill='x', padx=20, pady=(0, 20))

                tk.Button(btn_frame, text="📊 Export Report", command=export_stock_summary,
                         bg='#3b82f6', fg='white', font=('Segoe UI', 11, 'bold'),
                         padx=25, pady=10, relief='flat', cursor='hand2').pack(side='left', padx=(0, 10))

                tk.Button(btn_frame, text="✅ Close", command=stock_win.destroy,
                         bg='#6b7280', fg='white', font=('Segoe UI', 11, 'bold'),
                         padx=25, pady=10, relief='flat', cursor='hand2').pack(side='left')

            ttk.Button(summary_frame, text="📦 Stock Summary", command=open_stock_summary).grid(row=0, column=5, padx=8)

            # Stock snapshot frame (current inventory values)
            stock_frame = tk.LabelFrame(win, text="Stock Snapshot (Current)", padx=8, pady=6)
            stock_frame.pack(fill='x', padx=8, pady=(0, 8))
            stock_cost_var = tk.StringVar(value="ZMW 0.00")
            stock_sell_var = tk.StringVar(value="ZMW 0.00")
            stock_profit_var = tk.StringVar(value="ZMW 0.00")
            tk.Label(stock_frame, text="Total Stock (Cost):").grid(row=0, column=0, sticky='e')
            tk.Label(stock_frame, textvariable=stock_cost_var, font=('Arial', 10, 'bold')).grid(row=0, column=1, sticky='w', padx=5)
            tk.Label(stock_frame, text="Total Stock (Sell):").grid(row=0, column=2, sticky='e')
            tk.Label(stock_frame, textvariable=stock_sell_var, font=('Arial', 10, 'bold')).grid(row=0, column=3, sticky='w', padx=5)
            tk.Label(stock_frame, text="Expected Profit:").grid(row=0, column=4, sticky='e')
            tk.Label(stock_frame, textvariable=stock_profit_var, font=('Arial', 10, 'bold'), fg='#27ae60').grid(row=0, column=5, sticky='w', padx=5)

            def update_stock_snapshot():
                total_cost = 0.0
                total_sell = 0.0
                for name, qty, _cat in get_all_stock():
                    cost, sell = get_item_prices(name)
                    total_cost += (float(cost or 0) * int(qty))
                    total_sell += (float(sell or 0) * int(qty))
                stock_cost_var.set(f"ZMW {total_cost:.2f}")
                stock_sell_var.set(f"ZMW {total_sell:.2f}")
                stock_profit_var.set(f"ZMW {(total_sell - total_cost):.2f}")

            # Initialize summary and snapshot
            refresh_summary()
            update_stock_snapshot()

            # Detailed log view below summaries (left) + Stock Sales Summary (right)
            content_frame = tk.Frame(win)
            content_frame.pack(fill='both', expand=True, padx=8, pady=(0,8))

            # Left: detailed sales log (styled table)
            left_frame = tk.Frame(content_frame)
            left_frame.pack(side='left', fill='both', expand=True)

            admin_columns = ('user','pay','ref','item','qty','unit','total','date')
            admin_left_tree = ttk.Treeview(left_frame, columns=admin_columns, show='headings', height=22, style='Summary.Treeview')
            # Headings
            admin_left_tree.heading('user', text='User')
            admin_left_tree.heading('pay', text='Pay')
            admin_left_tree.heading('ref', text='Ref')
            admin_left_tree.heading('item', text='Item')
            admin_left_tree.heading('qty', text='Qty')
            admin_left_tree.heading('unit', text='Unit')
            admin_left_tree.heading('total', text='Total')
            admin_left_tree.heading('date', text='Date')
            # Columns
            admin_left_tree.column('user', width=110, anchor='w')
            admin_left_tree.column('pay', width=110, anchor='center')
            admin_left_tree.column('ref', width=130, anchor='w')
            admin_left_tree.column('item', width=220, anchor='w')
            admin_left_tree.column('qty', width=70, anchor='e')
            admin_left_tree.column('unit', width=100, anchor='e')
            admin_left_tree.column('total', width=110, anchor='e')
            admin_left_tree.column('date', width=170, anchor='w')
            admin_left_tree.pack(side='left', fill='both', expand=True)
            admin_left_scroll = ttk.Scrollbar(left_frame, orient='vertical', command=admin_left_tree.yview)
            admin_left_scroll.pack(side='right', fill='y')
            admin_left_tree.configure(yscrollcommand=admin_left_scroll.set)
            try:
                admin_left_tree.tag_configure('odd', background='#f9fbff')
                admin_left_tree.tag_configure('even', background='#ffffff')
            except Exception:
                pass

            # Right: Gen Z styled summary panel
            right_frame = tk.Frame(content_frame, width=450, bg='#0f172a')
            right_frame.pack(side='right', fill='y', padx=(10, 0))
            right_frame.pack_propagate(False)

            # Container with gradient-esque styling
            vibe_card = tk.Frame(right_frame, bg='#1e293b', padx=20, pady=20, relief='flat', bd=0)
            vibe_card.pack(fill='both', expand=True, padx=8, pady=8)

            # Header with emoji and modern font
            header_container = tk.Frame(vibe_card, bg='#1e293b')
            header_container.pack(fill='x', pady=(0, 15))

            tk.Label(header_container, text="✨ Top Sellers",
                    font=('Segoe UI', 18, 'bold'), fg='#f0abfc', bg='#1e293b').pack(anchor='w')
            tk.Label(header_container, text="Your bestselling items right now",
                    font=('Segoe UI', 10), fg='#cbd5e1', bg='#1e293b').pack(anchor='w', pady=(2, 0))

            # Total sales pill with gradient colors
            pill_frame = tk.Frame(vibe_card, bg='#1e293b')
            pill_frame.pack(fill='x', pady=(0, 15))

            sales_pill = tk.Label(pill_frame, textvariable=total_amount_var,
                                 font=('Segoe UI', 14, 'bold'), fg='#0f172a', bg='#a5f3fc',
                                 padx=16, pady=8)
            sales_pill.pack(anchor='w')

            # Top items scrollable container
            tk.Label(vibe_card, text="🔥 Trending Items",
                    font=('Segoe UI', 12, 'bold'), fg='#f8fafc', bg='#1e293b').pack(anchor='w', pady=(5, 10))

            item_preview_canvas = tk.Canvas(vibe_card, bg='#1e293b', highlightthickness=0, height=300)
            item_preview_scrollbar = ttk.Scrollbar(vibe_card, orient='vertical', command=item_preview_canvas.yview)
            item_preview_container = tk.Frame(item_preview_canvas, bg='#1e293b')

            item_preview_canvas.create_window((0, 0), window=item_preview_container, anchor='nw')
            item_preview_canvas.configure(yscrollcommand=item_preview_scrollbar.set)

            item_preview_canvas.pack(side='left', fill='both', expand=True)
            item_preview_scrollbar.pack(side='right', fill='y')

            def update_scroll_region(event=None):
                item_preview_canvas.configure(scrollregion=item_preview_canvas.bbox('all'))
            item_preview_container.bind('<Configure>', update_scroll_region)

            # Fetch and render item sales data
            def fetch_item_sales_summary(start_ts, end_ts, limit=None):
                conn_sum = get_db()
                cur_sum = conn_sum.cursor()
                try:
                    query = """
                        SELECT si.item,
                               COALESCE(SUM(CASE WHEN s.status != 'VOIDED' THEN si.quantity ELSE 0 END), 0) AS qty_sold,
                               COALESCE(SUM(CASE WHEN s.status != 'VOIDED' THEN si.subtotal ELSE 0 END), 0.0) AS revenue
                        FROM sale_items si
                        JOIN sales s ON s.id = si.sale_id
                        WHERE s.timestamp BETWEEN ? AND ?
                        GROUP BY si.item
                        HAVING qty_sold > 0
                        ORDER BY revenue DESC, si.item ASC
                    """
                    params = [start_ts, end_ts]
                    if limit:
                        query += " LIMIT ?"
                        params.append(limit)
                    cur_sum.execute(query, params)
                    rows = cur_sum.fetchall()
                finally:
                    conn_sum.close()
                return rows

            def render_item_preview(rows):
                for child in item_preview_container.winfo_children():
                    child.destroy()
                if not rows:
                    tk.Label(item_preview_container, text="No sales data yet 📊",
                             font=('Segoe UI', 11, 'italic'), fg='#cbd5e1', bg='#1e293b').pack(expand=True, pady=20)
                    return

                accent_colors = ['#f472b6', '#38bdf8', '#c084fc', '#facc15', '#34d399', '#fb7185', '#a78bfa', '#fb923c']
                for idx, (name, qty, revenue) in enumerate(rows):
                    card = tk.Frame(item_preview_container, bg='#334155', padx=14, pady=12)
                    card.pack(fill='x', pady=5, padx=2)

                    # Rank badge with vibrant color
                    badge = tk.Label(card, text=f"#{idx + 1}",
                                    fg='#0f172a', bg=accent_colors[idx % len(accent_colors)],
                                    font=('Segoe UI', 10, 'bold'), padx=12, pady=4)
                    badge.pack(side='left', padx=(0, 12))

                    # Item details
                    text_frame = tk.Frame(card, bg='#334155')
                    text_frame.pack(side='left', fill='x', expand=True)

                    tk.Label(text_frame, text=name[:28], font=('Segoe UI', 11, 'bold'),
                            fg='#f8fafc', bg='#334155').pack(anchor='w')
                    tk.Label(text_frame, text=f"{int(qty)} units · ZMW {float(revenue or 0.0):.2f}",
                            font=('Segoe UI', 9), fg='#94a3b8', bg='#334155').pack(anchor='w')

                    # Pulse indicator
                    tk.Frame(card, bg='#10b981', width=6, height=6).pack(side='right', padx=4)

            # Refresh items based on date range
            def refresh_item_preview(limit=10):
                start = get_date_value(entry_start)
                end = get_date_value(entry_end)
                start_ts = f"{start} 00:00:00"
                end_ts = f"{end} 23:59:59"
                rows = fetch_item_sales_summary(start_ts, end_ts, limit)
                render_item_preview(rows)
                return rows

            # Export functionality
            def export_item_summary():
                import csv, os
                from datetime import datetime as dt
                start = get_date_value(entry_start)
                end = get_date_value(entry_end)
                start_ts = f"{start} 00:00:00"
                end_ts = f"{end} 23:59:59"
                rows = fetch_item_sales_summary(start_ts, end_ts, limit=None)

                if not rows:
                    messagebox.showinfo("Export", "No sales data to export for selected range.")
                    return

                os.makedirs('exports', exist_ok=True)
                timestamp = dt.now().strftime('%Y%m%d_%H%M%S')
                filename = f"item_sales_summary_{start}_to_{end}_{timestamp}.csv"
                filepath = os.path.join('exports', filename)

                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Item Name', 'Quantity Sold', 'Total Revenue (ZMW)'])
                    for item_name, qty, revenue in rows:
                        writer.writerow([item_name, int(qty), f"{float(revenue or 0.0):.2f}"])

                messagebox.showinfo("Export Complete", f"Item summary exported to:\n{filepath}")

            # Full breakdown modal
            def show_full_breakdown():
                details_win = tk.Toplevel(win)
                details_win.title("📊 Full Item Breakdown")
                details_win.geometry("950x700")
                details_win.configure(bg="#f4f7fb")

                header = tk.Frame(details_win, bg="#2c3e50")
                header.pack(fill='x')
                tk.Label(header, text="📊 Complete Item Sales Breakdown",
                         fg='white', bg="#2c3e50",
                         font=('Segoe UI', 16, 'bold'), padx=20, pady=15).pack(side='left')

                content = tk.Frame(details_win, bg="#f4f7fb")
                content.pack(fill='both', expand=True, padx=20, pady=20)

                style = ttk.Style(details_win)
                try:
                    style.configure("ItemDetails.Treeview", font=('Segoe UI', 11), rowheight=30, background="#ffffff")
                    style.configure("ItemDetails.Treeview.Heading", font=('Segoe UI', 11, 'bold'), background="#6366f1", foreground="white")
                except Exception:
                    pass

                cols = ('item', 'qty', 'revenue')
                details_tree = ttk.Treeview(content, columns=cols, show='headings', style='ItemDetails.Treeview')
                details_tree.heading('item', text='Item Name')
                details_tree.heading('qty', text='Quantity Sold')
                details_tree.heading('revenue', text='Total Revenue (ZMW)')
                details_tree.column('item', width=450, anchor='w')
                details_tree.column('qty', width=200, anchor='center')
                details_tree.column('revenue', width=250, anchor='e')

                tree_scroll = ttk.Scrollbar(content, orient='vertical', command=details_tree.yview)
                details_tree.configure(yscrollcommand=tree_scroll.set)
                details_tree.pack(side='left', fill='both', expand=True)
                tree_scroll.pack(side='right', fill='y')

                try:
                    details_tree.tag_configure('odd', background='#f9fafb')
                    details_tree.tag_configure('even', background='#ffffff')
                except Exception:
                    pass

                start = get_date_value(entry_start)
                end = get_date_value(entry_end)
                start_ts = f"{start} 00:00:00"
                end_ts = f"{end} 23:59:59"
                rows = fetch_item_sales_summary(start_ts, end_ts, limit=None)

                for idx, (name, qty, rev) in enumerate(rows):
                    tag = 'odd' if (idx % 2) else 'even'
                    details_tree.insert('', 'end', values=(name, int(qty or 0), f"ZMW {float(rev or 0.0):.2f}"), tags=(tag,))

                footer = tk.Frame(details_win, bg="#e5e7eb", relief='solid', bd=1)
                footer.pack(fill='x', padx=20, pady=(0, 20))

                total_items = len(rows)
                total_qty = sum(int(r[1]) for r in rows)
                total_rev = sum(float(r[2] or 0) for r in rows)

                tk.Label(footer, text=f"📦 {total_items} Items | 📊 {total_qty} Units Sold | 💰 ZMW {total_rev:.2f}",
                        bg="#e5e7eb", font=('Segoe UI', 12, 'bold'), fg="#1f2937",
                        padx=15, pady=12).pack()

                tk.Button(details_win, text="✅ Close", command=details_win.destroy,
                         bg='#10b981', fg='white', font=('Segoe UI', 11, 'bold'),
                         padx=35, pady=10, relief='flat', cursor='hand2').pack(pady=(0, 20))

            # Action buttons with modern styling
            cta_frame = tk.Frame(vibe_card, bg='#1e293b')
            cta_frame.pack(fill='x', pady=(15, 0))

            breakdown_btn = tk.Button(cta_frame, text="📊 Full Breakdown", command=show_full_breakdown,
                                     bg='#6366f1', fg='white', font=('Segoe UI', 10, 'bold'),
                                     padx=16, pady=10, relief='flat', cursor='hand2',
                                     activebackground='#4f46e5')
            breakdown_btn.pack(side='left', fill='x', expand=True, padx=(0, 6))

            export_btn = tk.Button(cta_frame, text="⬇ Export CSV", command=export_item_summary,
                                  bg='#14b8a6', fg='#0f172a', font=('Segoe UI', 10, 'bold'),
                                  padx=16, pady=10, relief='flat', cursor='hand2',
                                  activebackground='#0d9488')
            export_btn.pack(side='right', fill='x', expand=True, padx=(6, 0))

            # Hover effects for buttons
            def on_breakdown_enter(e): breakdown_btn.config(bg='#4f46e5')
            def on_breakdown_leave(e): breakdown_btn.config(bg='#6366f1')
            def on_export_enter(e): export_btn.config(bg='#0d9488')
            def on_export_leave(e): export_btn.config(bg='#14b8a6')

            breakdown_btn.bind('<Enter>', on_breakdown_enter)
            breakdown_btn.bind('<Leave>', on_breakdown_leave)
            export_btn.bind('<Enter>', on_export_enter)
            export_btn.bind('<Leave>', on_export_leave)

            # Initial load of item preview
            refresh_item_preview()

            after_id = {'id': '', 'alive': True}

            def on_sales_window_close():
                """Clean up when sales window is closed"""
                after_id['alive'] = False
                try:
                    if after_id.get('id'):
                        win.after_cancel(after_id['id'])
                except (tk.TclError, AttributeError):
                    pass
                try:
                    win.destroy()
                except (tk.TclError, AttributeError):
                    pass

            win.protocol("WM_DELETE_WINDOW", on_sales_window_close)

            def refresh_detailed_log():
                """Refresh the detailed sales log table"""
                conn = None
                rows = []
                try:
                    conn = get_db()
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT s.cashier,
                               COALESCE(s.payment_method, 'Cash') AS pay,
                               COALESCE(s.mobile_ref, '') AS mob_ref,
                               si.item,
                               si.quantity,
                               si.unit_price AS unit_price,
                               si.subtotal AS total,
                               s.timestamp AS created_at
                        FROM sales s
                        JOIN sale_items si ON s.id = si.sale_id
                        WHERE s.status != 'VOIDED'
                        ORDER BY datetime(s.timestamp) DESC
                        """
                    )
                    rows = cur.fetchall()
                except Exception:
                    pass
                finally:
                    if conn:
                        try:
                            conn.close()
                        except Exception:
                            pass

                # Populate left table
                for iid in admin_left_tree.get_children():
                    admin_left_tree.delete(iid)
                for idx, row in enumerate(rows):
                    user, pay, ref, item, qty, unit, total, created = row
                    values = (
                        str(user)[:24],
                        str(pay)[:16],
                        str(ref)[:18],
                        str(item)[:30],
                        int(qty),
                        f"{float(unit):.2f}",
                        f"{float(total):.2f}",
                        str(created)[:19]
                    )
                    tag = 'odd' if (idx % 2) else 'even'
                    admin_left_tree.insert('', 'end', values=values, tags=(tag,))

            def _auto_refresh():
                """Auto-refresh sales data every 5 seconds"""
                if not after_id['alive']:
                    return
                try:
                    if not win.winfo_exists():
                        return
                except Exception:
                    return
                try:
                    refresh_summary()
                except Exception:
                    pass
                try:
                    refresh_detailed_log()
                except Exception:
                    pass
                try:
                    refresh_item_preview()
                except Exception:
                    pass
                try:
                    if after_id.get('alive') and win.winfo_exists():
                        timer_id = win.after(5000, _auto_refresh)
                        after_id['id'] = timer_id
                except (tk.TclError, AttributeError):
                    pass

            def _on_destroy(event):
                """Clean up when window is destroyed"""
                after_id['alive'] = False
                try:
                    if after_id.get('id'):
                        win.after_cancel(after_id['id'])
                except (tk.TclError, AttributeError):
                    pass

            win.bind("<Destroy>", _on_destroy)

            # Initial load of detailed log
            refresh_detailed_log()

            # Start auto-refresh timer
            try:
                if win.winfo_exists():
                    timer_id = win.after(5000, _auto_refresh)
                    after_id['id'] = timer_id
            except (tk.TclError, AttributeError):
                pass

        def user_management():
            from tkinter import Toplevel, Frame, Label, Entry, Button, Listbox, Scrollbar, StringVar, OptionMenu, messagebox, END, SINGLE
            import sales_utils

            def refresh_user_list():
                user_list.delete(0, END)
                conn = get_db()
                cur = conn.cursor()
                cur.execute("SELECT username, role FROM users ORDER BY username")
                for row in cur.fetchall():
                    user_list.insert(END, f"{row[0]} ({row[1]})")
                conn.close()

            def add_user():
                def save_new_user():
                    """Save or update user account"""
                    uname = entry_uname.get().strip()
                    pwd = entry_pwd.get()
                    role = role_var.get()
                    if not uname or not pwd:
                        messagebox.showerror("Error", "Username and password required.")
                        return
                    # Normalize username for existence check (case/space insensitive)
                    uname_norm = uname.lower()
                    conn = None
                    row = None
                    try:
                        conn = get_db()
                        cur = conn.cursor()
                        cur.execute("SELECT username FROM users WHERE LOWER(TRIM(username))=?", (uname_norm,))
                        row = cur.fetchone()
                    except Exception:
                        pass
                    finally:
                        if conn:
                            try:
                                conn.close()
                            except Exception:
                                pass
                    if row:
                        # User exists; offer to reset password and update role instead of blocking
                        if messagebox.askyesno("User Exists", f"User '{row[0]}' already exists.\nDo you want to reset the password and update the role?"):
                            try:
                                new_hash = sales_utils.hash_password(pwd)
                                conn2 = get_db()
                                cur2 = conn2.cursor()
                                cur2.execute("UPDATE users SET password_hash=?, role=? WHERE username=?", (new_hash, role, row[0]))
                                conn2.commit()
                                conn2.close()
                                messagebox.showinfo("Updated", f"User '{row[0]}' updated.")
                                win_add.destroy()
                                refresh_user_list()
                            except Exception as e:
                                messagebox.showerror("Error", f"Failed to update user: {e}")
                        else:
                            messagebox.showinfo("Cancelled", "No changes made.")
                        return
                    # Create new user
                    if sales_utils.create_user(uname, pwd, role):
                        messagebox.showinfo("Success", f"User '{uname}' added.")
                        win_add.destroy()
                        refresh_user_list()
                    else:
                        messagebox.showerror("Error", "Failed to add user.")
                win_add = Toplevel(win_um)
                win_add.title("Add User")
                Label(win_add, text="Username:").grid(row=0, column=0)
                entry_uname = Entry(win_add)
                entry_uname.grid(row=0, column=1)
                Label(win_add, text="Password:").grid(row=1, column=0)
                entry_pwd = Entry(win_add, show="*")
                entry_pwd.grid(row=1, column=1)
                Label(win_add, text="Role:").grid(row=2, column=0)
                role_var = StringVar(win_add)
                role_var.set("cashier")
                OptionMenu(win_add, role_var, "admin", "cashier").grid(row=2, column=1)
                Button(win_add, text="Save", command=save_new_user).grid(row=3, column=0, columnspan=2, pady=5)

            def edit_user():
                sel = user_list.curselection()
                if not sel:
                    messagebox.showerror("Error", "Select a user to edit.")
                    return
                uname = user_list.get(sel[0]).split()[0]
                user = sales_utils.get_user(uname)
                if not user:
                    messagebox.showerror("Error", "User not found.")
                    return
                def save_edit_user():
                    new_pwd = entry_pwd.get()
                    new_role = role_var.get()
                    conn = get_db()
                    cur = conn.cursor()
                    if new_pwd:
                        new_hash = sales_utils.hash_password(new_pwd)
                        cur.execute("UPDATE users SET password_hash=?, role=? WHERE username=?", (new_hash, new_role, uname))
                    else:
                        cur.execute("UPDATE users SET role=? WHERE username=?", (new_role, uname))
                    conn.commit()
                    conn.close()
                    messagebox.showinfo("Success", f"User '{uname}' updated.")
                    win_edit.destroy()
                    refresh_user_list()
                win_edit = Toplevel(win_um)
                win_edit.title(f"Edit User: {uname}")
                Label(win_edit, text="New Password (leave blank to keep current):").grid(row=0, column=0)
                entry_pwd = Entry(win_edit, show="*")
                entry_pwd.grid(row=0, column=1)
                Label(win_edit, text="Role:").grid(row=1, column=0)
                role_var = StringVar(win_edit)
                role_var.set(user['role'])
                OptionMenu(win_edit, role_var, "admin", "cashier").grid(row=1, column=1)
                Button(win_edit, text="Save", command=save_edit_user).grid(row=2, column=0, columnspan=2, pady=5)

            def delete_user():
                sel = user_list.curselection()
                if not sel:
                    messagebox.showerror("Error", "Select a user to delete.")
                    return
                uname = user_list.get(sel[0]).split()[0]
                if uname == current_user['username']:
                    messagebox.showerror("Error", "You cannot delete the currently logged-in user.")
                    return
                # Prevent deleting last admin
                conn = get_db()
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM users WHERE role='admin'")
                admin_count = cur.fetchone()[0]
                if admin_count <= 1:
                    cur.execute("SELECT role FROM users WHERE username=?", (uname,))
                    if cur.fetchone()[0] == 'admin':
                        conn.close()
                        messagebox.showerror("Error", "Cannot delete the last admin user.")
                        return
                cur.execute("DELETE FROM users WHERE username=?", (uname,))
                conn.commit()
                conn.close()
                messagebox.showinfo("Success", f"User '{uname}' deleted.")
                refresh_user_list()

            win_um = Toplevel(root)
            win_um.title("User Management")
            frame = Frame(win_um)
            frame.pack(padx=10, pady=10)
            user_list = Listbox(frame, width=30, height=10, selectmode=SINGLE)
            user_list.grid(row=0, column=0, rowspan=4)
            scrollbar = Scrollbar(frame, command=user_list.yview)
            scrollbar.grid(row=0, column=1, rowspan=4, sticky='ns')
            user_list.config(yscrollcommand=scrollbar.set)
            Button(frame, text="Add User", command=add_user).grid(row=0, column=2, padx=5)
            Button(frame, text="Edit User", command=edit_user).grid(row=1, column=2, padx=5)
            Button(frame, text="Delete User", command=delete_user).grid(row=2, column=2, padx=5)
            Button(frame, text="Refresh", command=refresh_user_list).grid(row=3, column=2, padx=5)
            refresh_user_list()

        def dashboard_prompt():
            """Enhanced Business Analytics Dashboard with 6 charts and metrics."""
            from tkinter.simpledialog import askstring
            pw = askstring("Dashboard Access", "Enter admin password:", show="*")
            if pw == USERS['admin']['password']:
                log_audit_event(f"Enhanced Dashboard opened by {current_user['username']}")
                
                import sqlite3
                from datetime import datetime, timedelta
                import matplotlib
                matplotlib.use('TkAgg')
                from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
                import matplotlib.pyplot as plt
                from matplotlib.figure import Figure
                import tkinter.scrolledtext as st
                import numpy as np
                
                conn = get_db()
                cur = conn.cursor()
                today = datetime.now().strftime('%Y-%m-%d')
                
                # Daily Sales (Last 14 days)
                cur.execute("""SELECT DATE(timestamp), SUM(total), COUNT(*) FROM sales 
                    WHERE status != 'VOIDED' GROUP BY DATE(timestamp) 
                    ORDER BY DATE(timestamp) DESC LIMIT 14""")
                daily_rows = cur.fetchall()[::-1]
                dates = [r[0] for r in daily_rows]
                daily_totals = [r[1] for r in daily_rows]
                
                # Top 8 Selling Items
                cur.execute("""SELECT si.item, SUM(si.quantity) as qty, SUM(si.subtotal) as revenue
                    FROM sales s JOIN sale_items si ON s.id = si.sale_id 
                    WHERE s.status != 'VOIDED' GROUP BY si.item ORDER BY qty DESC LIMIT 8""")
                item_rows = cur.fetchall()
                items = [r[0] for r in item_rows]
                item_qtys = [r[1] for r in item_rows]
                
                # Sales by Cashier
                cur.execute("""SELECT cashier, SUM(total), COUNT(*) FROM sales 
                    WHERE status != 'VOIDED' GROUP BY cashier ORDER BY SUM(total) DESC""")
                user_rows = cur.fetchall()
                users = [r[0] for r in user_rows]
                user_totals = [r[1] for r in user_rows]
                
                # Sales by Hour
                cur.execute("""SELECT strftime('%H', timestamp) as hour, SUM(total), COUNT(*) 
                    FROM sales WHERE status != 'VOIDED' AND DATE(timestamp) >= date('now', '-7 days')
                    GROUP BY hour ORDER BY hour""")
                hour_rows = cur.fetchall()
                hours = [int(r[0]) for r in hour_rows]
                hour_totals = [r[1] for r in hour_rows]
                
                # Payment Methods
                cur.execute("""SELECT payment_method, COUNT(*), SUM(total) FROM sales 
                    WHERE status != 'VOIDED' AND payment_method IS NOT NULL GROUP BY payment_method""")
                payment_rows = cur.fetchall()
                payment_methods = [r[0] or 'Cash' for r in payment_rows]
                payment_counts = [r[1] for r in payment_rows]
                
                # Stock Status
                cur.execute("""SELECT COUNT(CASE WHEN stock > 20 THEN 1 END) as good,
                    COUNT(CASE WHEN stock BETWEEN 6 AND 20 THEN 1 END) as medium,
                    COUNT(CASE WHEN stock <= 5 THEN 1 END) as low FROM items""")
                stock_status = cur.fetchone()
                
                # Today's Stats
                cur.execute("""SELECT COUNT(*), SUM(total), AVG(total) FROM sales 
                    WHERE status != 'VOIDED' AND DATE(timestamp) = ?""", (today,))
                today_stats = cur.fetchone()
                
                # Monthly Comparison
                cur.execute("""SELECT strftime('%Y-%m', timestamp) as month, SUM(total) 
                    FROM sales WHERE status != 'VOIDED' GROUP BY month ORDER BY month DESC LIMIT 6""")
                monthly_rows = cur.fetchall()[::-1]
                months = [r[0] for r in monthly_rows]
                monthly_totals = [r[1] for r in monthly_rows]
                
                conn.close()
                
                # Create Dashboard Window
                dash = tk.Toplevel(root)
                dash.title("📊 Business Analytics Dashboard")
                dash.geometry("1400x900")
                dash.configure(bg='#f8f9fa')
                
                # Header
                header = tk.Frame(dash, bg='#6366f1', height=80)
                header.pack(fill='x')
                header.pack_propagate(False)
                tk.Label(header, text="📊 BUSINESS ANALYTICS DASHBOARD", 
                        font=('Arial', 24, 'bold'), bg='#6366f1', fg='white').pack(side='left', padx=30, pady=20)
                tk.Label(header, text=f"📅 {datetime.now().strftime('%B %d, %Y')}", 
                        font=('Arial', 14), bg='#6366f1', fg='#e0e7ff').pack(side='right', padx=30)
                
                # Scrollable container
                main_container = tk.Frame(dash, bg='#f8f9fa')
                main_container.pack(fill='both', expand=True)
                canvas_scroll = tk.Canvas(main_container, bg='#f8f9fa', highlightthickness=0)
                scrollbar = tk.Scrollbar(main_container, orient="vertical", command=canvas_scroll.yview)
                scrollable_frame = tk.Frame(canvas_scroll, bg='#f8f9fa')
                scrollable_frame.bind("<Configure>", lambda e: canvas_scroll.configure(scrollregion=canvas_scroll.bbox("all")))
                canvas_scroll.create_window((0, 0), window=scrollable_frame, anchor="nw")
                canvas_scroll.configure(yscrollcommand=scrollbar.set)
                canvas_scroll.pack(side="left", fill="both", expand=True)
                scrollbar.pack(side="right", fill="y")
                
                # Metrics Cards
                metrics_frame = tk.Frame(scrollable_frame, bg='#f8f9fa')
                metrics_frame.pack(fill='x', padx=20, pady=(20, 10))
                
                def create_metric_card(parent, title, value, subtitle, color):
                    card = tk.Frame(parent, bg=color)
                    card.pack(side='left', fill='both', expand=True, padx=5)
                    tk.Label(card, text=title, font=('Arial', 11), bg=color, fg='white').pack(fill='x', padx=15, pady=(15, 5))
                    tk.Label(card, text=value, font=('Arial', 24, 'bold'), bg=color, fg='white').pack(fill='x', padx=15)
                    tk.Label(card, text=subtitle, font=('Arial', 9), bg=color, fg='#e0e0e0').pack(fill='x', padx=15, pady=(0, 15))
                
                transactions_today = today_stats[0] if today_stats[0] else 0
                revenue_today = today_stats[1] if today_stats[1] else 0
                avg_transaction = today_stats[2] if today_stats[2] else 0
                week_avg = sum(daily_totals[-7:]) / 7 if len(daily_totals) >= 7 else (revenue_today or 1)
                growth = ((revenue_today - week_avg) / week_avg * 100) if week_avg > 0 else 0
                
                create_metric_card(metrics_frame, "💰 TODAY'S REVENUE", f"ZMW {revenue_today:,.2f}", 
                                 f"{'📈' if growth >= 0 else '📉'} {abs(growth):.1f}% vs 7-day avg", '#10b981')
                create_metric_card(metrics_frame, "🛒 TRANSACTIONS", f"{transactions_today}", 
                                 f"Avg: ZMW {avg_transaction:.2f} per sale", '#3b82f6')
                total_items_sold = sum(item_qtys) if item_qtys else 0
                create_metric_card(metrics_frame, "📦 ITEMS SOLD", f"{total_items_sold}", 
                                 f"Top: {items[0] if items else 'N/A'}", '#f59e0b')
                low_stock_count = stock_status[2] if stock_status else 0
                create_metric_card(metrics_frame, "⚠️ LOW STOCK", f"{low_stock_count}", 
                                 "Items need restocking", '#ef4444' if low_stock_count > 0 else '#10b981')
                
                # 6 Charts
                fig = Figure(figsize=(14, 12), facecolor='#f8f9fa')
                ax1, ax2 = fig.add_subplot(3, 2, 1), fig.add_subplot(3, 2, 2)
                ax3, ax4 = fig.add_subplot(3, 2, 3), fig.add_subplot(3, 2, 4)
                ax5, ax6 = fig.add_subplot(3, 2, 5), fig.add_subplot(3, 2, 6)
                
                # Chart 1: Daily Sales Trend
                if dates and daily_totals:
                    ax1.plot(range(len(dates)), daily_totals, marker='o', linewidth=2.5, color='#6366f1', markersize=8)
                    ax1.fill_between(range(len(dates)), daily_totals, alpha=0.3, color='#6366f1')
                    ax1.set_title('📈 Daily Sales (Last 14 Days)', fontweight='bold', fontsize=12)
                    ax1.set_xticks(range(len(dates)))
                    ax1.set_xticklabels([d.split('-')[2] for d in dates], rotation=45)
                    ax1.grid(True, alpha=0.3, linestyle='--')
                    avg_sales = sum(daily_totals) / len(daily_totals)
                    ax1.axhline(y=avg_sales, color='#ef4444', linestyle='--', label=f'Avg: {avg_sales:.0f}')
                    ax1.legend()
                
                # Chart 2: Top Selling Items
                if items and item_qtys:
                    colors = plt.cm.viridis([i/len(items) for i in range(len(items))])
                    ax2.barh(items[::-1], item_qtys[::-1], color=colors[::-1])
                    ax2.set_title('🏆 Top Selling Items', fontweight='bold', fontsize=12)
                    for i, qty in enumerate(item_qtys[::-1]):
                        ax2.text(qty, i, f' {int(qty)}', va='center', fontsize=9, fontweight='bold')
                
                # Chart 3: Sales by Hour
                if hours and hour_totals:
                    ax3.fill_between(hours, hour_totals, alpha=0.6, color='#10b981')
                    ax3.plot(hours, hour_totals, marker='o', linewidth=2, color='#059669')
                    ax3.set_title('⏰ Sales by Hour', fontweight='bold', fontsize=12)
                    ax3.set_xticks(range(0, 24, 2))
                    if hour_totals:
                        peak_idx = hour_totals.index(max(hour_totals))
                        ax3.axvline(x=hours[peak_idx], color='#ef4444', linestyle='--', label=f'Peak: {hours[peak_idx]}:00')
                        ax3.legend()
                
                # Chart 4: Payment Methods
                if payment_methods and payment_counts:
                    colors_pie = ['#6366f1', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6']
                    ax4.pie(payment_counts, labels=payment_methods, autopct='%1.1f%%', colors=colors_pie[:len(payment_methods)], shadow=True)
                    ax4.set_title('💳 Payment Methods', fontweight='bold', fontsize=12)
                
                # Chart 5: Cashier Performance
                if users and user_totals:
                    colors_users = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6']
                    ax5.bar(users, user_totals, color=colors_users[:len(users)])
                    ax5.set_title('👥 Cashier Performance', fontweight='bold', fontsize=12)
                    ax5.tick_params(axis='x', rotation=30)
                    for i, total in enumerate(user_totals):
                        ax5.text(i, total, f'{total:,.0f}', ha='center', va='bottom', fontsize=8)
                
                # Chart 6: Monthly Trend
                if months and monthly_totals:
                    ax6.plot(range(len(months)), monthly_totals, marker='s', linewidth=3, color='#8b5cf6', markersize=10)
                    ax6.fill_between(range(len(months)), monthly_totals, alpha=0.2, color='#8b5cf6')
                    ax6.set_title('📅 Monthly Trend', fontweight='bold', fontsize=12)
                    ax6.set_xticks(range(len(months)))
                    ax6.set_xticklabels([m.split('-')[1] for m in months])
                    if len(monthly_totals) > 1:
                        z = np.polyfit(range(len(monthly_totals)), monthly_totals, 1)
                        p = np.poly1d(z)
                        ax6.plot(range(len(months)), p(range(len(months))), "r--", label='Trend')
                        ax6.legend()
                
                plt.tight_layout(pad=3.0)
                canvas_widget = FigureCanvasTkAgg(fig, master=scrollable_frame)
                canvas_widget.draw()
                canvas_widget.get_tk_widget().pack(fill='both', expand=True, padx=20, pady=10)
                
                # Stock Status Section
                stock_frame = tk.Frame(scrollable_frame, bg='#ffffff', relief='solid', bd=1)
                stock_frame.pack(fill='x', padx=20, pady=(10, 20))
                tk.Label(stock_frame, text="📦 STOCK STATUS", font=('Arial', 14, 'bold'), bg='#ffffff').pack(pady=(15, 10))
                
                if stock_status:
                    stock_indicators = tk.Frame(stock_frame, bg='#ffffff')
                    stock_indicators.pack(fill='x', padx=20, pady=(0, 15))
                    for label, count, color, emoji in [("Good", stock_status[0] or 0, '#10b981', '🟢'),
                                                        ("Medium", stock_status[1] or 0, '#f59e0b', '🟡'),
                                                        ("Low", stock_status[2] or 0, '#ef4444', '🔴')]:
                        frame = tk.Frame(stock_indicators, bg=color)
                        frame.pack(side='left', fill='both', expand=True, padx=5)
                        tk.Label(frame, text=f"{emoji} {label}", font=('Arial', 10, 'bold'), bg=color, fg='white').pack(pady=(10, 5))
                        tk.Label(frame, text=str(count), font=('Arial', 20, 'bold'), bg=color, fg='white').pack(pady=(0, 10))
                
                # Buttons
                btn_frame = tk.Frame(scrollable_frame, bg='#f8f9fa')
                btn_frame.pack(pady=(10, 20))
                def refresh_dashboard():
                    dash.destroy()
                    dashboard_prompt()
                tk.Button(btn_frame, text="🔄 Refresh", command=refresh_dashboard, bg='#6366f1', fg='white', 
                         font=('Arial', 12, 'bold'), padx=30, pady=12).pack(side='left', padx=5)
                tk.Button(btn_frame, text="✖️ Close", command=dash.destroy, bg='#6b7280', fg='white', 
                         font=('Arial', 12, 'bold'), padx=30, pady=12).pack(side='left', padx=5)
                
                # Mouse wheel scrolling
                def _on_mousewheel(event):
                    canvas_scroll.yview_scroll(int(-1*(event.delta/120)), "units")
                dash.bind_all("<MouseWheel>", _on_mousewheel)
                
                # Low sales alert
                if daily_totals and len(daily_totals) >= 7:
                    avg = sum(daily_totals[-7:]) / 7
                    if revenue_today < 0.5 * avg:
                        messagebox.showwarning('Low Sales Alert', f"Today's sales below 50% of average!")
            else:
                messagebox.showerror("Access Denied", "Incorrect password.")

        def export_all_sales():
            import sqlite3
            from tkinter import Toplevel, Label, Button, Checkbutton, IntVar, messagebox, StringVar, OptionMenu, Entry
            from datetime import datetime, timedelta
            win = Toplevel(root)
            win.title("Export All Sales - Options")
            # Get min/max dates from sales
            conn = get_db()
            cur = conn.cursor()
            cur.execute('SELECT MIN(DATE(timestamp)), MAX(DATE(timestamp)) FROM sales')
            min_date, max_date = cur.fetchone()
            if not min_date:
                min_date = max_date = datetime.now().strftime('%Y-%m-%d')
            # Date pickers
            Label(win, text="Start Date:").grid(row=0, column=0)
            if DateEntry is not None:
                entry_start = DateEntry(win, date_pattern='yyyy-mm-dd')
                entry_start.set_date(min_date)
            else:
                entry_start = Entry(win, width=12)
                entry_start.insert(0, min_date)
            entry_start.grid(row=0, column=1)
            Label(win, text="End Date:").grid(row=1, column=0)
            if DateEntry is not None:
                entry_end = DateEntry(win, date_pattern='yyyy-mm-dd')
                entry_end.set_date(max_date)
            else:
                entry_end = Entry(win, width=12)
                entry_end.insert(0, max_date)
            entry_end.grid(row=1, column=1)

            def get_date_val(widget):
                """Get date string from DateEntry or regular Entry"""
                if DateEntry is not None and hasattr(widget, 'get_date'):
                    return widget.get_date().strftime('%Y-%m-%d')
                return widget.get()

            # Preset range buttons
            def set_this_month():
                today = datetime.now()
                if DateEntry is not None and hasattr(entry_start, 'set_date'):
                    entry_start.set_date(today.replace(day=1))
                    entry_end.set_date(today)
                else:
                    entry_start.delete(0, 'end')
                    entry_start.insert(0, today.replace(day=1).strftime('%Y-%m-%d'))
                    entry_end.delete(0, 'end')
                    entry_end.insert(0, today.strftime('%Y-%m-%d'))
            def set_last_7_days():
                today = datetime.now()
                if DateEntry is not None and hasattr(entry_start, 'set_date'):
                    entry_start.set_date(today - timedelta(days=6))
                    entry_end.set_date(today)
                else:
                    entry_start.delete(0, 'end')
                    entry_start.insert(0, (today - timedelta(days=6)).strftime('%Y-%m-%d'))
                    entry_end.delete(0, 'end')
                    entry_end.insert(0, today.strftime('%Y-%m-%d'))
            Button(win, text="This Month", command=set_this_month).grid(row=0, column=2, padx=5)
            Button(win, text="Last 7 Days", command=set_last_7_days).grid(row=1, column=2, padx=5)
            # Get columns from sales table
            cur.execute('PRAGMA table_info(sales)')
            columns = [row[1] for row in cur.fetchall()]
            conn.close()
            col_vars = {col: IntVar(value=1) for col in columns}
            Label(win, text="Select columns to export:").grid(row=2, column=0, columnspan=3)
            for i, col in enumerate(columns):
                Checkbutton(win, text=col, variable=col_vars[col]).grid(row=3+i, column=0, columnspan=3, sticky='w')
            # Export format dropdown
            Label(win, text="Export Format:").grid(row=3+len(columns), column=0)
            format_var = StringVar(win)
            format_var.set("CSV")
            OptionMenu(win, format_var, "CSV", "Excel").grid(row=3+len(columns), column=1)
            def do_export():
                start = entry_start.get_date().strftime('%Y-%m-%d')
                end = entry_end.get_date().strftime('%Y-%m-%d')
                selected_cols = [col for col, var in col_vars.items() if var.get()]
                if not selected_cols:
                    messagebox.showerror("Error", "Select at least one column.")
                    return
                export_format = format_var.get()
                csv_path = export_all_sales_to_csv(start, end, selected_cols, export_format)
                log_audit_event(f"All sales exported by {current_user['username']} to {csv_path} (filtered, {export_format})")
                messagebox.showinfo("Export All Sales", f"All sales exported to:\n{csv_path}")
                win.destroy()
            Button(win, text="Export", command=do_export).grid(row=4+len(columns), column=0, columnspan=3, pady=10)

        def add_stock():
            """Add/Edit Stock feature has been disabled.
            This stub prevents NameError if referenced elsewhere.
            """
            try:
                log_audit_event(f"add_stock invoked (disabled) by {current_user.get('username')}")
            except Exception:
                pass
            messagebox.showinfo("Feature Disabled", "The Add/Edit Stock feature has been disabled.\n\nPlease use the bag management system or contact administrator.")

        # Advanced stock analytics on dashboard
        def show_stock_analytics():
            from sales_utils import get_all_stock, get_item_prices
            stock_list = get_all_stock()
            total_cost = 0
            total_sell = 0
            
            for item, qty, cat in stock_list:  # Fixed: unpack 3 values
                prices = get_item_prices(item)
                if prices:
                    cost, sell = prices
                    total_cost += (cost or 0) * qty
                    total_sell += (sell or 0) * qty
            
            profit = total_sell - total_cost
            messagebox.showinfo("Stock Analytics", 
                               f"Total Stock Value (Cost): ZMW {total_cost:.2f}\n"
                               f"Total Stock Value (Sell): ZMW {total_sell:.2f}\n"
                               f"Potential Profit: ZMW {profit:.2f}")

        def show_low_stock_alerts():
            from tkinter import messagebox
            from sales_utils import get_all_stock
            low_stock_items = []
            for item, qty, cat in get_all_stock():
                if qty <= 5:  # Threshold for low stock
                    low_stock_items.append(f"{item} ({qty}) [{cat}]")
            if low_stock_items:
                messagebox.showwarning("Low Stock Alert", "Low stock for:\n" + "\n".join(low_stock_items))
            else:
                messagebox.showinfo("Low Stock Alert", "All items are sufficiently stocked.")

        # ===== BAG MANAGEMENT FUNCTIONS =====

        def manage_bags_dialog():
            """🛍️ COMPREHENSIVE UNIFIED BAG & INVENTORY MANAGEMENT CENTER

            All-in-one interface for complete bag and item CRUD operations:
            - Left Panel: Create, rename, delete bags
            - Middle Panel: View, search, manage items in selected bag
            - Right Panel: Add new items or update existing ones

            Replaces old separate dialogs: manage_bags, add_item_to_bag, view_bag_contents
            """
            from sales_utils import (get_bags, create_bag, rename_bag, delete_bag,
                                    get_items_in_bag, add_item_to_bag, update_bag_item, delete_bag_item)

            dlg = tk.Toplevel(root)
            dlg.title("🛍️ Complete Bag & Inventory Management Center")
            dlg.geometry("1600x900")
            dlg.configure(bg='#f8f9fa')
            dlg.transient(root)

            # Header
            header = tk.Frame(dlg, bg='#6366f1', height=70)
            header.pack(fill='x')
            header.pack_propagate(False)
            tk.Label(header, text="🛍️ Bag & Inventory Management Center",
                    font=('Arial', 22, 'bold'), bg='#6366f1', fg='white').pack(pady=18)

            # Main container - 3 column layout
            main_container = tk.Frame(dlg, bg='#f8f9fa')
            main_container.pack(fill='both', expand=True, padx=15, pady=15)

            # ═══════════════════════════════════════════════════════════════
            # LEFT PANEL - BAG MANAGEMENT
            # ═══════════════════════════════════════════════════════════════
            left_panel = tk.Frame(main_container, bg='#ffffff', relief='solid', bd=1)
            left_panel.pack(side='left', fill='both', expand=True, padx=(0, 8))

            # Left header
            left_header = tk.Frame(left_panel, bg='#a855f7', height=50)
            left_header.pack(fill='x')
            left_header.pack_propagate(False)
            tk.Label(left_header, text="📦 MANAGE BAGS", font=('Arial', 14, 'bold'),
                    bg='#a855f7', fg='white').pack(pady=12)

            # Bag list section
            bag_list_frame = tk.Frame(left_panel, bg='#ffffff', padx=15, pady=15)
            bag_list_frame.pack(fill='both', expand=True)

            tk.Label(bag_list_frame, text="📋 All Bags:", bg='#ffffff',
                    font=('Arial', 11, 'bold'), fg='#2c3e50').pack(anchor='w', pady=(0, 8))

            # Listbox with scrollbar
            bags_list_container = tk.Frame(bag_list_frame, bg='#ffffff')
            bags_list_container.pack(fill='both', expand=True, pady=(0, 12))

            bags_scroll = tk.Scrollbar(bags_list_container)
            bags_scroll.pack(side='right', fill='y')

            bags_listbox = tk.Listbox(bags_list_container, font=('Arial', 11),
                                      yscrollcommand=bags_scroll.set, height=12,
                                      selectmode='single', activestyle='dotbox')
            bags_listbox.pack(side='left', fill='both', expand=True)
            bags_scroll.config(command=bags_listbox.yview)

            # Bag operations
            tk.Label(bag_list_frame, text="🔧 Bag Operations:", bg='#ffffff',
                    font=('Arial', 11, 'bold'), fg='#2c3e50').pack(anchor='w', pady=(8, 8))

            bag_name_frame = tk.Frame(bag_list_frame, bg='#ffffff')
            bag_name_frame.pack(fill='x', pady=(0, 10))
            tk.Label(bag_name_frame, text="Bag Name:", bg='#ffffff',
                    font=('Arial', 10)).pack(side='left', padx=(0, 8))
            bag_name_var = tk.StringVar()
            bag_name_entry = tk.Entry(bag_name_frame, textvariable=bag_name_var,
                                     font=('Arial', 11), width=25)
            bag_name_entry.pack(side='left', fill='x', expand=True)

            # Forward declarations for refresh functions
            def refresh_items():
                pass  # Defined later

            def refresh_bags():
                """Refresh the bags list."""
                bags_listbox.delete(0, tk.END)
                try:
                    bags = get_bags()
                    for bag_id, bag_name in bags:
                        bags_listbox.insert(tk.END, f"{bag_name} (ID: {bag_id})")
                    if bags:
                        bags_listbox.selection_set(0)
                        bags_listbox.event_generate('<<ListboxSelect>>')
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to load bags: {e}")

            def create_new_bag():
                """Create a new bag."""
                name = bag_name_var.get().strip()
                if not name:
                    messagebox.showwarning("Empty Name", "Please enter a bag name")
                    return
                try:
                    bag_id = create_bag(name)
                    messagebox.showinfo("Success", f"✅ Bag '{name}' created!")
                    bag_name_var.set("")
                    refresh_bags()
                    if REFRESH_ITEMS_CALLBACK:
                        try: REFRESH_ITEMS_CALLBACK()
                        except: pass
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to create bag:\n{e}")

            def rename_selected_bag():
                """Rename the selected bag."""
                sel = bags_listbox.curselection()
                if not sel:
                    messagebox.showwarning("No Selection", "Please select a bag to rename")
                    return
                text = bags_listbox.get(sel[0])
                bag_id = int(text.split("ID: ")[1].rstrip(')'))
                new_name = bag_name_var.get().strip()
                if not new_name:
                    messagebox.showwarning("Empty Name", "Please enter a new bag name")
                    return
                try:
                    if rename_bag(bag_id, new_name):
                        messagebox.showinfo("Success", f"✅ Bag renamed to '{new_name}'")
                        refresh_bags()
                        if REFRESH_ITEMS_CALLBACK:
                            try: REFRESH_ITEMS_CALLBACK()
                            except: pass
                    else:
                        messagebox.showerror("Error", "Rename operation failed")
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to rename bag:\n{e}")

            def delete_selected_bag():
                """Delete the selected bag and all its items."""
                sel = bags_listbox.curselection()
                if not sel:
                    messagebox.showwarning("No Selection", "Please select a bag to delete")
                    return
                text = bags_listbox.get(sel[0])
                bag_name = text.split(" (ID:")[0]
                bag_id = int(text.split("ID: ")[1].rstrip(')'))

                if not messagebox.askyesno("⚠️ Confirm Delete",
                                          f"Delete bag '{bag_name}' and ALL its items?\n\nThis cannot be undone!",
                                          icon='warning'):
                    return
                try:
                    if delete_bag(bag_id):
                        messagebox.showinfo("Deleted", f"✅ Bag '{bag_name}' deleted")
                        bag_name_var.set("")
                        refresh_bags()
                        if REFRESH_ITEMS_CALLBACK:
                            try: REFRESH_ITEMS_CALLBACK()
                            except: pass
                    else:
                        messagebox.showerror("Error", "Delete operation failed")
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to delete bag:\n{e}")

            # Bag action buttons
            bag_btn_frame = tk.Frame(bag_list_frame, bg='#ffffff')
            bag_btn_frame.pack(fill='x', pady=(0, 8))

            tk.Button(bag_btn_frame, text="➕ Create New", command=create_new_bag,
                     bg='#10b981', fg='white', font=('Arial', 10, 'bold'),
                     padx=15, pady=8).pack(side='left', padx=(0, 5), fill='x', expand=True)
            tk.Button(bag_btn_frame, text="✏️ Rename", command=rename_selected_bag,
                     bg='#f59e0b', fg='white', font=('Arial', 10, 'bold'),
                     padx=15, pady=8).pack(side='left', padx=5, fill='x', expand=True)
            tk.Button(bag_btn_frame, text="🗑️ Delete", command=delete_selected_bag,
                     bg='#ef4444', fg='white', font=('Arial', 10, 'bold'),
                     padx=15, pady=8).pack(side='left', padx=(5, 0), fill='x', expand=True)

            # ═══════════════════════════════════════════════════════════════
            # MIDDLE PANEL - ITEMS IN SELECTED BAG
            # ═══════════════════════════════════════════════════════════════
            middle_panel = tk.Frame(main_container, bg='#ffffff', relief='solid', bd=1)
            middle_panel.pack(side='left', fill='both', expand=True, padx=8)

            # Middle header
            middle_header = tk.Frame(middle_panel, bg='#06b6d4', height=50)
            middle_header.pack(fill='x')
            middle_header.pack_propagate(False)
            selected_bag_label = tk.Label(middle_header, text="📦 ITEMS IN BAG",
                                         font=('Arial', 14, 'bold'),
                                         bg='#06b6d4', fg='white')
            selected_bag_label.pack(pady=12)

            # Items list section
            items_list_frame = tk.Frame(middle_panel, bg='#ffffff', padx=15, pady=15)
            items_list_frame.pack(fill='both', expand=True)

            tk.Label(items_list_frame, text="📋 Items in Selected Bag:", bg='#ffffff',
                    font=('Arial', 11, 'bold'), fg='#2c3e50').pack(anchor='w', pady=(0, 8))

            # Search box
            search_frame = tk.Frame(items_list_frame, bg='#ffffff')
            search_frame.pack(fill='x', pady=(0, 8))
            tk.Label(search_frame, text="🔍", bg='#ffffff', font=('Arial', 12)).pack(side='left', padx=(0, 5))
            search_var = tk.StringVar()
            search_entry = tk.Entry(search_frame, textvariable=search_var, font=('Arial', 10),
                                   bg='#f3f4f6')
            search_entry.pack(side='left', fill='x', expand=True)
            tk.Label(search_frame, text="Search items...", bg='#ffffff',
                    font=('Arial', 9, 'italic'), fg='#9ca3af').pack(side='left', padx=(8, 0))

            # Items treeview
            items_tree_container = tk.Frame(items_list_frame, bg='#ffffff')
            items_tree_container.pack(fill='both', expand=True, pady=(0, 12))

            items_scroll_y = tk.Scrollbar(items_tree_container)
            items_scroll_y.pack(side='right', fill='y')
            items_scroll_x = tk.Scrollbar(items_tree_container, orient='horizontal')
            items_scroll_x.pack(side='bottom', fill='x')

            items_tree = ttk.Treeview(items_tree_container,
                                     columns=('id', 'name', 'price', 'stock'),
                                     show='headings',
                                     yscrollcommand=items_scroll_y.set,
                                     xscrollcommand=items_scroll_x.set,
                                     height=15)

            items_scroll_y.config(command=items_tree.yview)
            items_scroll_x.config(command=items_tree.xview)

            items_tree.heading('id', text='🔢 ID')
            items_tree.heading('name', text='📦 Item Name')
            items_tree.heading('price', text='💰 Price (ZMW)')
            items_tree.heading('stock', text='📊 Stock')

            items_tree.column('id', width=60, anchor='center')
            items_tree.column('name', width=200, anchor='w')
            items_tree.column('price', width=100, anchor='e')
            items_tree.column('stock', width=80, anchor='center')

            # Configure stock level tags with colors
            items_tree.tag_configure('low_stock', background='#fee2e2', foreground='#991b1b')  # Red for low stock (≤5)
            items_tree.tag_configure('medium_stock', background='#fef3c7', foreground='#92400e')  # Yellow for medium (6-20)
            items_tree.tag_configure('good_stock', background='#d1fae5', foreground='#065f46')  # Green for good (>20)

            items_tree.pack(fill='both', expand=True)

            # Enhanced item count label with legend
            count_frame = tk.Frame(items_list_frame, bg='#ffffff')
            count_frame.pack(fill='x', pady=(5, 0))

            item_count_label = tk.Label(count_frame, text="Total items: 0",
                                       bg='#ffffff', font=('Arial', 9, 'bold'),
                                       fg='#2c3e50')
            item_count_label.pack(side='left')

            # Stock level legend
            legend_frame = tk.Frame(count_frame, bg='#ffffff')
            legend_frame.pack(side='right')

            tk.Label(legend_frame, text="🟢 >20", bg='#ffffff', fg='#065f46',
                    font=('Arial', 8)).pack(side='left', padx=3)
            tk.Label(legend_frame, text="🟡 6-20", bg='#ffffff', fg='#92400e',
                    font=('Arial', 8)).pack(side='left', padx=3)
            tk.Label(legend_frame, text="🔴 ≤5", bg='#ffffff', fg='#991b1b',
                    font=('Arial', 8)).pack(side='left', padx=3)

            # ═══════════════════════════════════════════════════════════════
            # RIGHT PANEL - ADD/EDIT ITEM
            # ═══════════════════════════════════════════════════════════════
            right_panel = tk.Frame(main_container, bg='#ffffff', relief='solid', bd=1)
            right_panel.pack(side='right', fill='both', expand=True, padx=(8, 0))

            # Right header
            right_header = tk.Frame(right_panel, bg='#fb923c', height=50)
            right_header.pack(fill='x')
            right_header.pack_propagate(False)
            mode_label = tk.Label(right_header, text="➕ ADD NEW ITEM",
                                 font=('Arial', 14, 'bold'),
                                 bg='#fb923c', fg='white')
            mode_label.pack(pady=12)

            # Item form
            form_frame = tk.Frame(right_panel, bg='#ffffff', padx=20, pady=20)
            form_frame.pack(fill='both', expand=True)

            # Mode selector
            mode_var = tk.StringVar(value='add')
            item_id_var = tk.StringVar()

            mode_selector_frame = tk.Frame(form_frame, bg='#f3f4f6', relief='solid', bd=1)
            mode_selector_frame.pack(fill='x', pady=(0, 15))

            def update_mode_display():
                if mode_var.get() == 'add':
                    mode_label.config(text="➕ ADD NEW ITEM", bg='#10b981')
                    right_header.config(bg='#10b981')
                    item_id_var.set("")
                    save_button.config(text="➕ Add Item", bg='#10b981')
                else:
                    mode_label.config(text="✏️ UPDATE EXISTING ITEM", bg='#f59e0b')
                    right_header.config(bg='#f59e0b')
                    save_button.config(text="💾 Update Item", bg='#f59e0b')

            def switch_to_add():
                mode_var.set('add')
                item_name_var.set("")
                item_price_var.set("0.00")
                item_stock_var.set("1")
                item_id_var.set("")
                update_mode_display()

            tk.Label(mode_selector_frame, text="Mode:", bg='#f3f4f6',
                    font=('Arial', 10, 'bold')).pack(side='left', padx=10, pady=8)
            tk.Radiobutton(mode_selector_frame, text="➕ Add New Item", variable=mode_var,
                          value='add', bg='#f3f4f6', font=('Arial', 10),
                          command=update_mode_display).pack(side='left', padx=5)
            tk.Radiobutton(mode_selector_frame, text="✏️ Update Existing", variable=mode_var,
                          value='update', bg='#f3f4f6', font=('Arial', 10),
                          command=update_mode_display).pack(side='left', padx=5)

            # Info tip
            info_frame = tk.Frame(form_frame, bg='#dbeafe', relief='solid', bd=1)
            info_frame.pack(fill='x', pady=(0, 15))
            tk.Label(info_frame, text="💡 Tip: Select an item from the middle panel to edit it, or add a new item below",
                    bg='#dbeafe', fg='#1e40af', font=('Arial', 9, 'italic'),
                    wraplength=350, justify='left').pack(padx=10, pady=8)

            # Form fields
            tk.Label(form_frame, text="Item Name:", bg='#ffffff',
                    font=('Arial', 11, 'bold'), fg='#2c3e50').pack(anchor='w', pady=(0, 5))
            item_name_var = tk.StringVar()
            item_name_entry = tk.Entry(form_frame, textvariable=item_name_var,
                                      font=('Arial', 12), bg='#f9fafb')
            item_name_entry.pack(fill='x', pady=(0, 15), ipady=8)

            tk.Label(form_frame, text="Price (ZMW):", bg='#ffffff',
                    font=('Arial', 11, 'bold'), fg='#2c3e50').pack(anchor='w', pady=(0, 5))
            item_price_var = tk.StringVar(value="0.00")
            item_price_entry = tk.Entry(form_frame, textvariable=item_price_var,
                                       font=('Arial', 12), bg='#f9fafb')
            item_price_entry.pack(fill='x', pady=(0, 15), ipady=8)

            tk.Label(form_frame, text="Stock Quantity:", bg='#ffffff',
                    font=('Arial', 11, 'bold'), fg='#2c3e50').pack(anchor='w', pady=(0, 5))
            item_stock_var = tk.StringVar(value="1")
            item_stock_entry = tk.Entry(form_frame, textvariable=item_stock_var,
                                       font=('Arial', 12), bg='#f9fafb')
            item_stock_entry.pack(fill='x', pady=(0, 20), ipady=8)

            # Action buttons
            def save_item():
                """Save or update item based on mode."""
                # Validate bag selection
                sel = bags_listbox.curselection()
                if not sel:
                    messagebox.showwarning("No Bag Selected", "Please select a bag first")
                    return

                # Get bag ID
                text = bags_listbox.get(sel[0])
                bag_id = int(text.split("ID: ")[1].rstrip(')'))

                # Validate inputs
                item_name = item_name_var.get().strip()
                if not item_name:
                    messagebox.showwarning("Empty Name", "Please enter an item name")
                    item_name_entry.focus()
                    return

                try:
                    price = float(item_price_var.get())
                    if price < 0:
                        messagebox.showwarning("Invalid Price", "Price cannot be negative")
                        item_price_entry.focus()
                        return
                except ValueError:
                    messagebox.showwarning("Invalid Price", "Please enter a valid price")
                    item_price_entry.focus()
                    return

                try:
                    stock = int(item_stock_var.get())
                    if stock < 0:
                        messagebox.showwarning("Invalid Stock", "Stock cannot be negative")
                        item_stock_entry.focus()
                        return
                except ValueError:
                    messagebox.showwarning("Invalid Stock", "Please enter a valid stock quantity")
                    item_stock_entry.focus()
                    return

                try:
                    if mode_var.get() == 'add':
                        # Add new item
                        item_id = add_item_to_bag(bag_id, item_name, stock, price, current_user.get('username', 'admin'))
                        messagebox.showinfo("Success", f"✅ Item '{item_name}' added successfully!\nID: {item_id}")
                        # Clear form
                        item_name_var.set("")
                        item_price_var.set("0.00")
                        item_stock_var.set("1")
                        item_name_entry.focus()
                    else:
                        # Update existing item
                        item_id = int(item_id_var.get())
                        if update_bag_item(item_id, price=price, stock=stock,
                                         username=current_user.get('username', 'admin'),
                                         reason='Updated via Bag Management Center'):
                            messagebox.showinfo("Success", f"✅ Item '{item_name}' updated successfully!")
                            switch_to_add()  # Switch back to add mode
                        else:
                            messagebox.showerror("Error", "Update operation failed")

                    refresh_items()
                    if REFRESH_ITEMS_CALLBACK:
                        try: REFRESH_ITEMS_CALLBACK()
                        except: pass

                except Exception as e:
                    messagebox.showerror("Error", f"Operation failed:\n{e}")

            def clear_form():
                """Clear the form and switch to add mode."""
                switch_to_add()
                item_name_entry.focus()

            buttons_frame = tk.Frame(form_frame, bg='#ffffff')
            buttons_frame.pack(fill='x', pady=(0, 15))

            save_button = tk.Button(buttons_frame, text="➕ Add Item", command=save_item,
                                   bg='#10b981', fg='white', font=('Arial', 12, 'bold'),
                                   padx=20, pady=12)
            save_button.pack(fill='x', pady=(0, 8))

            tk.Button(buttons_frame, text="🔄 Clear Form", command=clear_form,
                     bg='#6b7280', fg='white', font=('Arial', 11, 'bold'),
                     padx=20, pady=10).pack(fill='x')

            # Quick stats
            stats_frame = tk.Frame(form_frame, bg='#f3f4f6', relief='solid', bd=1)
            stats_frame.pack(fill='x', pady=(20, 0))

            tk.Label(stats_frame, text="📊 Quick Stats", bg='#f3f4f6',
                    font=('Arial', 10, 'bold'), fg='#374151').pack(pady=(8, 5))

            stats_label = tk.Label(stats_frame, text="Bags: 0 | Items: 0 | Total Stock: 0",
                                  bg='#f3f4f6', font=('Arial', 9), fg='#6b7280')
            stats_label.pack(pady=(0, 8))

            # ═══════════════════════════════════════════════════════════════
            # MIDDLE PANEL - Item Actions (continued from middle panel section)
            # ═══════════════════════════════════════════════════════════════

            def delete_selected_item():
                """Delete the selected item from the bag."""
                sel = items_tree.selection()
                if not sel:
                    messagebox.showwarning("No Selection", "Please select an item to delete")
                    return
                item = items_tree.item(sel[0])
                item_id = item['values'][0]
                item_name = item['values'][1]

                if not messagebox.askyesno("⚠️ Confirm Delete",
                                          f"Delete item '{item_name}'?\n\nThis cannot be undone!",
                                          icon='warning'):
                    return
                try:
                    if delete_bag_item(item_id):
                        messagebox.showinfo("Deleted", f"✅ Item '{item_name}' deleted")
                        refresh_items()
                        if REFRESH_ITEMS_CALLBACK:
                            try: REFRESH_ITEMS_CALLBACK()
                            except: pass
                    else:
                        messagebox.showerror("Error", "Delete operation failed")
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to delete item:\n{e}")

            def edit_selected_item():
                """Enhanced edit dialog with clear old vs new comparison."""
                sel = items_tree.selection()
                if not sel:
                    messagebox.showwarning("No Selection", "Please select an item to edit")
                    return

                item = items_tree.item(sel[0])
                item_id = item['values'][0]
                old_name = item['values'][1]
                old_price = float(item['values'][2])
                old_stock = int(item['values'][3])

                # Create enhanced edit dialog
                edit_dlg = tk.Toplevel(dlg)
                edit_dlg.title(f"✏️ Edit Item: {old_name}")
                edit_dlg.geometry("600x550")
                edit_dlg.configure(bg='#f8f9fa')
                edit_dlg.transient(dlg)
                edit_dlg.grab_set()

                # Header
                header = tk.Frame(edit_dlg, bg='#f59e0b', height=70)
                header.pack(fill='x')
                header.pack_propagate(False)
                tk.Label(header, text=f"✏️ Editing: {old_name}",
                        font=('Arial', 16, 'bold'), bg='#f59e0b', fg='white').pack(pady=20)

                # Current values display
                current_frame = tk.LabelFrame(edit_dlg, text="📋 Current Values",
                                             font=('Arial', 11, 'bold'), bg='#f8f9fa',
                                             fg='#475569', padx=20, pady=15)
                current_frame.pack(fill='x', padx=20, pady=(15, 10))

                info_grid = tk.Frame(current_frame, bg='#f8f9fa')
                info_grid.pack(fill='x')

                tk.Label(info_grid, text="Name:", bg='#f8f9fa', fg='#64748b',
                        font=('Arial', 10, 'bold')).grid(row=0, column=0, sticky='w', pady=5)
                tk.Label(info_grid, text=old_name, bg='#f8f9fa', fg='#1e293b',
                        font=('Arial', 11)).grid(row=0, column=1, sticky='w', padx=(10, 0), pady=5)

                tk.Label(info_grid, text="Price:", bg='#f8f9fa', fg='#64748b',
                        font=('Arial', 10, 'bold')).grid(row=1, column=0, sticky='w', pady=5)
                tk.Label(info_grid, text=f"ZMW {old_price:.2f}", bg='#f8f9fa', fg='#1e293b',
                        font=('Arial', 11)).grid(row=1, column=1, sticky='w', padx=(10, 0), pady=5)

                tk.Label(info_grid, text="Stock:", bg='#f8f9fa', fg='#64748b',
                        font=('Arial', 10, 'bold')).grid(row=2, column=0, sticky='w', pady=5)
                tk.Label(info_grid, text=f"{old_stock} units", bg='#f8f9fa', fg='#1e293b',
                        font=('Arial', 11)).grid(row=2, column=1, sticky='w', padx=(10, 0), pady=5)

                # New values form
                new_frame = tk.LabelFrame(edit_dlg, text="🔄 New Values",
                                         font=('Arial', 11, 'bold'), bg='#f8f9fa',
                                         fg='#475569', padx=20, pady=15)
                new_frame.pack(fill='x', padx=20, pady=(0, 10))

                # Price input
                tk.Label(new_frame, text="New Price (ZMW):", bg='#f8f9fa',
                        font=('Arial', 10, 'bold')).pack(anchor='w', pady=(0, 5))
                new_price_var = tk.StringVar(value=f"{old_price:.2f}")
                price_entry = tk.Entry(new_frame, textvariable=new_price_var,
                                      font=('Arial', 12), bg='#ffffff', width=20)
                price_entry.pack(anchor='w', pady=(0, 15))

                # Stock input
                tk.Label(new_frame, text="New Stock Quantity:", bg='#f8f9fa',
                        font=('Arial', 10, 'bold')).pack(anchor='w', pady=(0, 5))
                new_stock_var = tk.StringVar(value=str(old_stock))
                stock_entry = tk.Entry(new_frame, textvariable=new_stock_var,
                                      font=('Arial', 12), bg='#ffffff', width=20)
                stock_entry.pack(anchor='w', pady=(0, 15))

                # Update reason
                tk.Label(new_frame, text="Reason for Update (optional):", bg='#f8f9fa',
                        font=('Arial', 10, 'bold')).pack(anchor='w', pady=(0, 5))
                reason_var = tk.StringVar()
                reason_entry = tk.Entry(new_frame, textvariable=reason_var,
                                       font=('Arial', 11), bg='#ffffff')
                reason_entry.pack(fill='x', pady=(0, 10))

                # Comparison preview (live update)
                preview_frame = tk.Frame(edit_dlg, bg='#dbeafe', relief='solid', bd=1)
                preview_frame.pack(fill='x', padx=20, pady=(0, 15))

                preview_label = tk.Label(preview_frame, text="", bg='#dbeafe',
                                        fg='#1e40af', font=('Arial', 9, 'italic'),
                                        justify='left', wraplength=550)
                preview_label.pack(padx=15, pady=10)

                def update_preview(*args):
                    """Show live comparison preview."""
                    try:
                        new_price = float(new_price_var.get())
                        new_stock = int(new_stock_var.get())

                        price_change = new_price - old_price
                        stock_change = new_stock - old_stock

                        preview_text = f"📊 Changes Preview:\n"

                        if price_change != 0:
                            arrow = "📈" if price_change > 0 else "📉"
                            preview_text += f"   Price: ZMW {old_price:.2f} → ZMW {new_price:.2f} ({arrow} {price_change:+.2f})\n"
                        else:
                            preview_text += f"   Price: No change (ZMW {old_price:.2f})\n"

                        if stock_change != 0:
                            arrow = "⬆️" if stock_change > 0 else "⬇️"
                            preview_text += f"   Stock: {old_stock} → {new_stock} units ({arrow} {stock_change:+d})"
                        else:
                            preview_text += f"   Stock: No change ({old_stock} units)"

                        preview_label.config(text=preview_text)
                    except ValueError:
                        preview_label.config(text="⚠️ Please enter valid numbers")

                # Bind live preview updates
                new_price_var.trace('w', update_preview)
                new_stock_var.trace('w', update_preview)

                # Initial preview
                update_preview()

                # Buttons
                btn_frame = tk.Frame(edit_dlg, bg='#f8f9fa')
                btn_frame.pack(pady=(0, 20))

                def save_changes():
                    """Save the updated values."""
                    try:
                        new_price = float(new_price_var.get())
                        new_stock = int(new_stock_var.get())
                        reason = reason_var.get().strip() or 'Updated via Edit Dialog'

                        if new_price < 0:
                            messagebox.showerror("Invalid Price", "Price cannot be negative")
                            return

                        if new_stock < 0:
                            messagebox.showerror("Invalid Stock", "Stock cannot be negative")
                            return

                        # Show confirmation with changes
                        price_change = new_price - old_price
                        stock_change = new_stock - old_stock

                        confirm_msg = f"Confirm update for '{old_name}'?\n\n"
                        confirm_msg += f"Price: ZMW {old_price:.2f} → ZMW {new_price:.2f} ({price_change:+.2f})\n"
                        confirm_msg += f"Stock: {old_stock} → {new_stock} units ({stock_change:+d})"

                        if messagebox.askyesno("Confirm Changes", confirm_msg):
                            if update_bag_item(item_id, price=new_price, stock=new_stock,
                                             username=current_user.get('username', 'admin'),
                                             reason=reason):
                                messagebox.showinfo("Success",
                                                  f"✅ Updated '{old_name}'!\n\n"
                                                  f"Old → New:\n"
                                                  f"Price: ZMW {old_price:.2f} → ZMW {new_price:.2f}\n"
                                                  f"Stock: {old_stock} → {new_stock} units")
                                edit_dlg.destroy()
                                refresh_items()
                                if REFRESH_ITEMS_CALLBACK:
                                    try: REFRESH_ITEMS_CALLBACK()
                                    except: pass
                            else:
                                messagebox.showerror("Error", "Update failed")
                    except ValueError:
                        messagebox.showerror("Invalid Input", "Please enter valid numbers for price and stock")
                    except Exception as e:
                        messagebox.showerror("Error", f"Failed to update:\n{e}")

                tk.Button(btn_frame, text="💾 Save Changes", command=save_changes,
                         bg='#10b981', fg='white', font=('Arial', 11, 'bold'),
                         padx=25, pady=10).pack(side='left', padx=5)

                tk.Button(btn_frame, text="Cancel", command=edit_dlg.destroy,
                         bg='#6b7280', fg='white', font=('Arial', 11, 'bold'),
                         padx=25, pady=10).pack(side='left', padx=5)

                # Focus on price entry
                price_entry.focus_set()
                price_entry.selection_range(0, tk.END)

            item_actions_frame = tk.Frame(items_list_frame, bg='#ffffff')
            item_actions_frame.pack(fill='x', pady=(8, 0))

            tk.Button(item_actions_frame, text="✏️ Edit Selected", command=edit_selected_item,
                     bg='#3b82f6', fg='white', font=('Arial', 10, 'bold'),
                     padx=15, pady=8).pack(side='left', padx=(0, 5), fill='x', expand=True)
            tk.Button(item_actions_frame, text="🗑️ Delete Selected", command=delete_selected_item,
                     bg='#ef4444', fg='white', font=('Arial', 10, 'bold'),
                     padx=15, pady=8).pack(side='left', padx=(5, 0), fill='x', expand=True)

            # ═══════════════════════════════════════════════════════════════
            # REFRESH AND EVENT HANDLERS
            # ═══════════════════════════════════════════════════════════════

            def refresh_items():
                """Refresh items list for selected bag."""
                sel = bags_listbox.curselection()
                if not sel:
                    items_tree.delete(*items_tree.get_children())
                    item_count_label.config(text="Total items: 0")
                    selected_bag_label.config(text="📦 ITEMS IN BAG")
                    return

                text = bags_listbox.get(sel[0])
                bag_name = text.split(" (ID:")[0]
                bag_id = int(text.split("ID: ")[1].rstrip(')'))

                selected_bag_label.config(text=f"📦 ITEMS IN: {bag_name}")

                # Clear and reload
                items_tree.delete(*items_tree.get_children())

                try:
                    search_term = search_var.get().strip().lower()
                    items = get_items_in_bag(bag_id, search=search_term if search_term else None)

                    for item_id, name, price, stock in items:
                        # Determine stock level tag
                        if stock <= 5:
                            tag = 'low_stock'
                        elif stock <= 20:
                            tag = 'medium_stock'
                        else:
                            tag = 'good_stock'

                        items_tree.insert('', 'end', values=(item_id, name, f"{price:.2f}", stock), tags=(tag,))

                    item_count_label.config(text=f"Total items: {len(items)}")

                    # Update stats
                    update_stats()

                except Exception as e:
                    messagebox.showerror("Error", f"Failed to load items:\n{e}")

            def update_stats():
                """Update the quick stats display."""
                try:
                    bags = get_bags()
                    total_bags = len(bags)
                    total_items = 0
                    total_stock = 0

                    for bag_id, _ in bags:
                        items = get_items_in_bag(bag_id)
                        total_items += len(items)
                        total_stock += sum(stock for _, _, _, stock in items)

                    stats_label.config(text=f"Bags: {total_bags} | Items: {total_items} | Total Stock: {total_stock}")
                except:
                    stats_label.config(text="Bags: 0 | Items: 0 | Total Stock: 0")

            def on_bag_select(evt=None):
                """Handle bag selection."""
                sel = bags_listbox.curselection()
                if not sel:
                    return
                text = bags_listbox.get(sel[0])
                bag_name = text.split(" (ID:")[0]
                bag_name_var.set(bag_name)
                refresh_items()

            def on_search_change(*args):
                """Handle search input change."""
                refresh_items()

            # Bind events
            bags_listbox.bind('<<ListboxSelect>>', on_bag_select)
            search_var.trace('w', on_search_change)

            # ═══════════════════════════════════════════════════════════════
            # FOOTER
            # ═══════════════════════════════════════════════════════════════

            footer = tk.Frame(dlg, bg='#f8f9fa')
            footer.pack(fill='x', padx=15, pady=(0, 15))

            tk.Button(footer, text="🔄 Refresh All", command=lambda: (refresh_bags(), update_stats()),
                     bg='#3b82f6', fg='white', font=('Arial', 11, 'bold'),
                     padx=25, pady=10).pack(side='left', padx=(0, 10))

            tk.Button(footer, text="✖️ Close", command=dlg.destroy,
                     bg='#6b7280', fg='white', font=('Arial', 11, 'bold'),
                     padx=25, pady=10).pack(side='right')

            # Initial load
            refresh_bags()
            update_mode_display()

        def add_item_to_bag_dialog():
            """Dialog to add or edit items in a selected bag with CRUD controls."""
            from sales_utils import get_bags, add_item_to_bag, get_items_in_bag, update_bag_item, delete_bag_item

            dlg = tk.Toplevel(root)
            dlg.title("Add / Edit Item in Bag")
            dlg.geometry("700x520")
            dlg.configure(bg='#ffffff')
            dlg.transient(root)

            tk.Label(dlg, text="Add / Edit Item in Bag", font=('Arial', 16, 'bold'),
                    bg='#ffffff', fg='#2c3e50').pack(pady=12)

            info_frame = tk.Frame(dlg, bg='#e3f2fd', relief='flat', bd=1)
            info_frame.pack(fill='x', padx=20, pady=(0, 10))
            tk.Label(info_frame, text="ℹ️ Tip: If item already exists in bag, quantity will be added unless you choose Update",
                    font=('Arial', 9, 'italic'), bg='#e3f2fd', fg='#1976d2',
                    wraplength=600, justify='center').pack(pady=8)

            form_frame = tk.Frame(dlg, bg='#ffffff', padx=20, pady=10)
            form_frame.pack(fill='both', expand=True)

            # Bag selection
            tk.Label(form_frame, text="Select Bag:", bg='#ffffff',
                    font=('Arial', 11)).grid(row=0, column=0, sticky='e', padx=10, pady=8)

            bags = get_bags()
            if not bags:
                messagebox.showwarning("No Bags", "Please create a bag first using 'Manage Bags'")
                dlg.destroy()
                return

            bag_options = [f"{name} (ID: {bid})" for bid, name in bags]
            bag_var = tk.StringVar(value=bag_options[0] if bag_options else "")
            bag_combo = ttk.Combobox(form_frame, textvariable=bag_var, values=bag_options,
                                    state='readonly', font=('Arial', 11), width=36)
            bag_combo.grid(row=0, column=1, sticky='w', pady=8)

            # Existing items combobox for editing
            tk.Label(form_frame, text="Existing Item:", bg='#ffffff', font=('Arial', 11)).grid(row=1, column=0, sticky='e', padx=10, pady=8)
            existing_var = tk.StringVar()
            existing_combo = ttk.Combobox(form_frame, textvariable=existing_var, values=[], state='readonly', width=34)
            existing_combo.grid(row=1, column=1, sticky='w', pady=8)

            # Item name
            tk.Label(form_frame, text="Item Name:", bg='#ffffff',
                    font=('Arial', 11, 'bold')).grid(row=2, column=0, sticky='e', padx=10, pady=8)
            item_name_var = tk.StringVar()
            item_entry = tk.Entry(form_frame, textvariable=item_name_var, font=('Arial', 11), width=36)
            item_entry.grid(row=2, column=1, sticky='w', pady=8)

            # Amount/Quantity
            tk.Label(form_frame, text="Quantity:", bg='#ffffff',
                    font=('Arial', 11, 'bold')).grid(row=3, column=0, sticky='e', padx=10, pady=8)
            amount_var = tk.StringVar(value="1")
            tk.Entry(form_frame, textvariable=amount_var, font=('Arial', 11), width=16).grid(row=3, column=1, sticky='w', pady=8)

            # Price
            tk.Label(form_frame, text="Price (ZMW):", bg='#ffffff',
                    font=('Arial', 11, 'bold')).grid(row=4, column=0, sticky='e', padx=10, pady=8)
            price_var = tk.StringVar(value="0.00")
            tk.Entry(form_frame, textvariable=price_var, font=('Arial', 11), width=16).grid(row=4, column=1, sticky='w', pady=8)

            # Status label
            status_label = tk.Label(form_frame, text="", bg='#ffffff', fg='red', font=('Arial', 10))
            status_label.grid(row=5, column=0, columnspan=2, pady=6)

            # Helper to load existing items for selected bag
            def load_existing_items(event=None):
                try:
                    bag_text = bag_var.get()
                    bag_id = int(bag_text.split("ID: ")[1].rstrip(")"))
                except Exception:
                    return
                try:
                    items = get_items_in_bag(bag_id)
                    opts = [f"{name} (ID: {iid})" for iid, name, price, stock in items]
                    existing_combo['values'] = opts
                    if opts:
                        existing_combo.current(0)
                except Exception:
                    existing_combo['values'] = []

            def on_existing_select(event=None):
                sel = existing_var.get()
                if not sel:
                    return
                try:
                    item_id = int(sel.split("ID: ")[1].rstrip(')'))
                except Exception:
                    return
                try:
                    bag_text = bag_var.get()
                    bag_id = int(bag_text.split("ID: ")[1].rstrip(')'))
                    items = get_items_in_bag(bag_id)
                    for iid, name, price, stock in items:
                        if iid == item_id:
                            item_name_var.set(name)
                            price_var.set(f"{price:.2f}")
                            amount_var.set(str(stock))
                            break
                except Exception:
                    pass

            bag_combo.bind('<<ComboboxSelected>>', load_existing_items)
            existing_combo.bind('<<ComboboxSelected>>', on_existing_select)
            load_existing_items()

            def submit_item():
                try:
                    bag_text = bag_var.get()
                    bag_id = int(bag_text.split("ID: ")[1].rstrip(')'))
                    item_name = item_name_var.get().strip()
                    if not item_name:
                        raise ValueError("Item name cannot be empty")
                    amount = int(amount_var.get())
                    if amount <= 0:
                        raise ValueError("Quantity must be greater than 0")
                    price = float(price_var.get())
                    if price < 0:
                        raise ValueError("Price cannot be negative")

                    # Use add_item_to_bag (adds or increments) - pass username for logging
                    item_id = add_item_to_bag(bag_id, item_name, amount, price, CURRENT_USER or 'admin')
                    messagebox.showinfo("Saved", f"Item saved (ID: {item_id})")
                    # refresh
                    load_existing_items()
                    if REFRESH_ITEMS_CALLBACK:
                        try: REFRESH_ITEMS_CALLBACK()
                        except: pass
                except Exception as e:
                    status_label.config(text=str(e))

            def update_existing_item():
                sel = existing_var.get()
                if not sel:
                    messagebox.showwarning("Select Item", "Please select an existing item to update")
                    return
                try:
                    item_id = int(sel.split("ID: ")[1].rstrip(')'))
                    price = float(price_var.get())
                    stock = int(amount_var.get())
                    if update_bag_item(item_id, price=price, stock=stock, username=CURRENT_USER or 'admin',
                                      reason='Manual update via Add Item to Bag'):
                        messagebox.showinfo("Updated", "Item updated successfully")
                        load_existing_items()
                        if REFRESH_ITEMS_CALLBACK:
                            try: REFRESH_ITEMS_CALLBACK()
                            except: pass
                    else:
                        messagebox.showerror("Error", "Update failed")
                except Exception as e:
                    status_label.config(text=str(e))

            def delete_existing_item():
                sel = existing_var.get()
                if not sel:
                    messagebox.showwarning("Select Item", "Please select an existing item to delete")
                    return
                if not messagebox.askyesno("Confirm Delete", "Delete selected item?", icon='warning'):
                    return
                try:
                    item_id = int(sel.split("ID: ")[1].rstrip(')'))
                    if delete_bag_item(item_id):
                        messagebox.showinfo("Deleted", "Item deleted")
                        load_existing_items()
                        if REFRESH_ITEMS_CALLBACK:
                            try: REFRESH_ITEMS_CALLBACK()
                            except: pass
                    else:
                        messagebox.showerror("Error", "Delete failed")
                except Exception as e:
                    status_label.config(text=str(e))

            # Status label
            status_label = tk.Label(form_frame, text="", bg='#ffffff', fg='red',
                                   font=('Arial', 10))
            status_label.grid(row=4, column=0, columnspan=2, pady=10)

            def submit_item():
                try:
                    # Parse bag ID from selection
                    bag_text = bag_var.get()
                    bag_id = int(bag_text.split("ID: ")[1].rstrip(")"))

                    item_name = item_name_var.get().strip()
                    if not item_name:
                        raise ValueError("Item name cannot be empty")

                    amount = int(amount_var.get())
                    if amount <= 0:
                        raise ValueError("Quantity must be greater than 0")

                    price = float(price_var.get())
                    if price < 0:
                        raise ValueError("Price cannot be negative")

                    # Check if item already exists
                    from sales_utils import get_items_in_bag
                    existing_items = get_items_in_bag(bag_id)
                    existing_item = None
                    for item_id, name, old_price, old_stock in existing_items:
                        if name.lower() == item_name.lower():
                            existing_item = (item_id, name, old_price, old_stock)
                            break

                    # Add item to bag
                    item_id = add_item_to_bag(bag_id, item_name, amount, price)

                    if existing_item:
                        _, _, old_price, old_stock = existing_item
                        new_stock = old_stock + amount
                        messagebox.showinfo("Quantity Added",
                                           f"✅ Added {amount} units to existing item '{item_name}'\n\n"
                                           f"Previous stock: {old_stock}\n"
                                           f"Added: {amount}\n"
                                           f"New stock: {new_stock}\n"
                                           f"Price: ZMW {price:.2f}")
                    else:
                        messagebox.showinfo("New Item Added",
                                           f"✅ New item '{item_name}' added to bag!\n\n"
                                           f"Quantity: {amount}\n"
                                           f"Price: ZMW {price:.2f}\n"
                                           f"Item ID: {item_id}")

                    # Clear form
                    item_name_var.set("")
                    amount_var.set("1")
                    price_var.set("0.00")
                    status_label.config(text="")
                    item_entry.focus_set()

                    # Refresh cashier UI if callback exists
                    if REFRESH_ITEMS_CALLBACK:
                        try:
                            REFRESH_ITEMS_CALLBACK()
                        except:
                            pass

                except Exception as e:
                    status_label.config(text=str(e))

            # Buttons
            btn_frame = tk.Frame(dlg, bg='#ffffff')
            btn_frame.pack(pady=12)

            tk.Button(btn_frame, text="Add (Append)", command=submit_item,
                     bg='#3498db', fg='white', font=('Arial', 11, 'bold'),
                     padx=14, pady=8).pack(side='left', padx=6)
            tk.Button(btn_frame, text="Update Selected", command=update_existing_item,
                     bg='#f39c12', fg='white', font=('Arial', 11, 'bold'),
                     padx=12, pady=8).pack(side='left', padx=6)
            tk.Button(btn_frame, text="Delete Selected", command=delete_existing_item,
                     bg='#c0392b', fg='white', font=('Arial', 11, 'bold'),
                     padx=12, pady=8).pack(side='left', padx=6)
            tk.Button(btn_frame, text="Close", command=dlg.destroy,
                     bg='#95a5a6', fg='white', font=('Arial', 11, 'bold'),
                     padx=20, pady=8).pack(side='left', padx=6)

        def view_bag_contents_dialog():
            """Dialog to view items in selected bags with search functionality."""
            from sales_utils import get_bags, get_items_in_bag

            dlg = tk.Toplevel(root)
            dlg.title("View Bag Contents")
            dlg.geometry("700x550")
            dlg.configure(bg='#ffffff')
            dlg.transient(root)

            tk.Label(dlg, text="Bag Contents", font=('Arial', 16, 'bold'),
                    bg='#ffffff', fg='#2c3e50').pack(pady=15)

            # Bag selection
            select_frame = tk.Frame(dlg, bg='#ffffff')
            select_frame.pack(fill='x', padx=20, pady=(0, 10))

            tk.Label(select_frame, text="Select Bag:", bg='#ffffff',
                    font=('Arial', 11, 'bold')).pack(side='left', padx=(0, 10))

            bags = get_bags()
            if not bags:
                messagebox.showinfo("No Bags", "No bags found. Create bags first.")
                dlg.destroy()
                return

            bag_options = [f"{name} (ID: {bid})" for bid, name in bags]
            bag_var = tk.StringVar(value=bag_options[0] if bag_options else "")
            bag_combo = ttk.Combobox(select_frame, textvariable=bag_var, values=bag_options,
                                    state='readonly', font=('Arial', 11), width=30)
            bag_combo.pack(side='left', padx=(0, 10))

            # Search
            tk.Label(select_frame, text="Search:", bg='#ffffff',
                    font=('Arial', 10)).pack(side='left', padx=(10, 5))
            search_var = tk.StringVar()
            search_entry = tk.Entry(select_frame, textvariable=search_var,
                                   font=('Arial', 10), width=20)
            search_entry.pack(side='left')

            # Items list
            list_frame = tk.Frame(dlg, bg='#ffffff')
            list_frame.pack(fill='both', expand=True, padx=20, pady=10)

            columns = ('ID', 'Item Name', 'Price', 'Stock')
            tree = ttk.Treeview(list_frame, columns=columns, show='headings', height=15)

            for col in columns:
                tree.heading(col, text=col)
                if col == 'ID':
                    tree.column(col, width=60, anchor='center')
                elif col == 'Item Name':
                    tree.column(col, width=300, anchor='w')
                elif col == 'Price':
                    tree.column(col, width=120, anchor='e')
                else:
                    tree.column(col, width=100, anchor='center')

            tree.pack(side='left', fill='both', expand=True)

            scroll = ttk.Scrollbar(list_frame, orient='vertical', command=tree.yview)
            scroll.pack(side='right', fill='y')
            tree.config(yscrollcommand=scroll.set)

            # Summary label
            summary_label = tk.Label(dlg, text="", bg='#ffffff', fg='#2c3e50',
                                    font=('Arial', 10, 'bold'))
            summary_label.pack(pady=(5, 0))

            def refresh_items(*args):
                try:
                    # Clear tree
                    for item in tree.get_children():
                        tree.delete(item)

                    # Parse bag ID
                    bag_text = bag_var.get()
                    bag_id = int(bag_text.split("ID: ")[1].rstrip(")"))

                    # Get items with optional search
                    search_text = search_var.get().strip()
                    items = get_items_in_bag(bag_id, search_text if search_text else None)

                    # Populate tree
                    total_items = 0
                    total_stock = 0
                    for item_id, item_name, price, stock in items:
                        tree.insert('', 'end', values=(item_id, item_name, f"ZMW {price:.2f}", stock))
                        total_items += 1
                        total_stock += stock

                    summary_label.config(text=f"Total: {total_items} item(s), {total_stock} units in stock")

                except Exception as e:
                    messagebox.showerror("Error", f"Failed to load items:\n{str(e)}")

            # Bind events
            bag_combo.bind('<<ComboboxSelected>>', refresh_items)
            search_var.trace('w', refresh_items)

            # Initial load
            refresh_items()

            # Close button
            tk.Button(dlg, text="Close", command=dlg.destroy,
                     bg='#95a5a6', fg='white', font=('Arial', 11, 'bold'),
                     padx=20, pady=8).pack(pady=15)

        def view_stock_history_dialog():
            """View complete stock movement history with filters."""
            from sales_utils import get_stock_history, get_stock_summary_by_item, get_items_in_bag, get_bags

            dlg = tk.Toplevel(root)
            dlg.title("📊 Stock History & Movement Tracking")
            dlg.geometry("1200x700")
            dlg.configure(bg='#f8f9fa')
            dlg.transient(root)

            # Header
            header = tk.Frame(dlg, bg='#6366f1', height=80)
            header.pack(fill='x')
            header.pack_propagate(False)

            tk.Label(header, text="📊 Stock Movement History", font=('Arial', 20, 'bold'),
                    bg='#6366f1', fg='white').pack(pady=20)

            # Create notebook for tabs
            notebook = ttk.Notebook(dlg)
            notebook.pack(fill='both', expand=True, padx=20, pady=20)

            # ═══════════════════════════════════════════════════════
            # TAB 1: DETAILED HISTORY
            # ═══════════════════════════════════════════════════════
            history_tab = tk.Frame(notebook, bg='#ffffff')
            notebook.add(history_tab, text="📜 Detailed History")

            # Filters frame
            filter_frame = tk.Frame(history_tab, bg='#ffffff')
            filter_frame.pack(fill='x', padx=20, pady=15)

            tk.Label(filter_frame, text="Filters:", font=('Arial', 12, 'bold'),
                    bg='#ffffff').grid(row=0, column=0, sticky='w', pady=(0, 10), columnspan=4)

            # Item filter
            tk.Label(filter_frame, text="Item:", bg='#ffffff').grid(row=1, column=0, sticky='w', padx=(0, 5))
            item_var = tk.StringVar(value="All Items")
            item_combo = ttk.Combobox(filter_frame, textvariable=item_var, state='readonly', width=25)
            item_combo.grid(row=1, column=1, padx=5)

            # Days filter
            tk.Label(filter_frame, text="Last:", bg='#ffffff').grid(row=1, column=2, sticky='w', padx=(15, 5))
            days_var = tk.StringVar(value="30 days")
            days_combo = ttk.Combobox(filter_frame, textvariable=days_var,
                                     values=["7 days", "30 days", "90 days", "180 days", "365 days"],
                                     state='readonly', width=12)
            days_combo.grid(row=1, column=3, padx=5)

            # Change type filter
            tk.Label(filter_frame, text="Type:", bg='#ffffff').grid(row=2, column=0, sticky='w', padx=(0, 5), pady=(5, 0))
            type_var = tk.StringVar(value="All Types")
            type_combo = ttk.Combobox(filter_frame, textvariable=type_var,
                                     values=["All Types", "SALE", "RESTOCK", "ADJUSTMENT", "INITIAL", "CORRECTION"],
                                     state='readonly', width=25)
            type_combo.grid(row=2, column=1, padx=5, pady=(5, 0))

            # Refresh button
            def refresh_history():
                """Refresh history with enhanced visual indicators."""
                # Get filters
                days = int(days_var.get().split()[0])
                change_type = None if type_var.get() == "All Types" else type_var.get()
                item_id = None

                # Get item_id if specific item selected
                if item_var.get() != "All Items":
                    try:
                        item_id = int(item_var.get().split("(ID: ")[1].rstrip(')'))
                    except:
                        pass

                # Fetch history
                history = get_stock_history(item_id=item_id, days=days, change_type=change_type)

                # Clear tree
                for item in history_tree.get_children():
                    history_tree.delete(item)

                # Populate tree with color coding
                for h in history:
                    change_amount = h['change_amount']

                    # Format change with clear indicators
                    if change_amount > 0:
                        change_text = f"⬆️ +{change_amount}"
                        tag = 'increase'
                    elif change_amount < 0:
                        change_text = f"⬇️ {change_amount}"
                        tag = 'decrease'
                    else:
                        change_text = "➖ 0"
                        tag = 'no_change'

                    # Format type with emoji
                    type_display = h['change_type']
                    if type_display == 'SALE':
                        type_display = "🛒 SALE"
                    elif type_display == 'RESTOCK':
                        type_display = "📦 RESTOCK"
                    elif type_display == 'ADJUSTMENT':
                        type_display = "⚙️ ADJUST"
                    elif type_display == 'INITIAL':
                        type_display = "🆕 INITIAL"

                    # Format old → new display
                    old_new = f"{h['old_stock']} → {h['new_stock']}"

                    history_tree.insert('', 'end', values=(
                        h['timestamp'],
                        h['item_name'],
                        h['bag_name'] or 'N/A',
                        h['old_stock'],
                        h['new_stock'],
                        change_text,
                        type_display,
                        h['changed_by'],
                        h['reason'] or 'N/A',
                        h['transaction_id'] or 'N/A'
                    ), tags=(tag,))

                # Update count with color indication
                increase_count = sum(1 for h in history if h['change_amount'] > 0)
                decrease_count = sum(1 for h in history if h['change_amount'] < 0)

                count_label.config(
                    text=f"Total records: {len(history)} | ⬆️ Increases: {increase_count} | ⬇️ Decreases: {decrease_count}"
                )

            tk.Button(filter_frame, text="🔄 Refresh", command=refresh_history,
                     bg='#6366f1', fg='white', font=('Arial', 10, 'bold'),
                     padx=15, pady=5).grid(row=2, column=3, padx=5, pady=(5, 0))

            # History tree
            tree_frame = tk.Frame(history_tab, bg='#ffffff')
            tree_frame.pack(fill='both', expand=True, padx=20, pady=(0, 10))

            # Scrollbars
            tree_scroll_y = tk.Scrollbar(tree_frame)
            tree_scroll_y.pack(side='right', fill='y')
            tree_scroll_x = tk.Scrollbar(tree_frame, orient='horizontal')
            tree_scroll_x.pack(side='bottom', fill='x')

            history_tree = ttk.Treeview(tree_frame,
                                       columns=('timestamp', 'item', 'bag', 'old_stock', 'new_stock',
                                               'change', 'type', 'user', 'reason', 'tx_id'),
                                       show='headings',
                                       yscrollcommand=tree_scroll_y.set,
                                       xscrollcommand=tree_scroll_x.set,
                                       height=15)

            tree_scroll_y.config(command=history_tree.yview)
            tree_scroll_x.config(command=history_tree.xview)

            # Configure columns with better headers
            history_tree.heading('timestamp', text='📅 Date/Time')
            history_tree.heading('item', text='📦 Item Name')
            history_tree.heading('bag', text='🛍️ Bag')
            history_tree.heading('old_stock', text='📊 Before')
            history_tree.heading('new_stock', text='📊 After')
            history_tree.heading('change', text='🔄 Change')
            history_tree.heading('type', text='🏷️ Type')
            history_tree.heading('user', text='👤 User')
            history_tree.heading('reason', text='📝 Reason')
            history_tree.heading('tx_id', text='🔢 Transaction ID')

            history_tree.column('timestamp', width=140, anchor='w')
            history_tree.column('item', width=150, anchor='w')
            history_tree.column('bag', width=100, anchor='center')
            history_tree.column('old_stock', width=80, anchor='center')
            history_tree.column('new_stock', width=80, anchor='center')
            history_tree.column('change', width=70, anchor='center')
            history_tree.column('type', width=100, anchor='center')
            history_tree.column('user', width=100, anchor='center')
            history_tree.column('reason', width=150, anchor='w')
            history_tree.column('tx_id', width=150, anchor='center')

            # Configure row colors for better readability
            history_tree.tag_configure('increase', background='#d1fae5', foreground='#065f46')  # Green for stock increase
            history_tree.tag_configure('decrease', background='#fee2e2', foreground='#991b1b')  # Red for stock decrease
            history_tree.tag_configure('no_change', background='#f3f4f6', foreground='#4b5563')  # Gray for no change

            history_tree.pack(fill='both', expand=True)

            # Count and legend frame
            count_legend_frame = tk.Frame(history_tab, bg='#ffffff')
            count_legend_frame.pack(fill='x', padx=20, pady=(5, 10))

            # Count label
            count_label = tk.Label(count_legend_frame, text="Total records: 0", bg='#ffffff',
                                  font=('Arial', 10, 'bold'), fg='#2c3e50')
            count_label.pack(side='left')

            # Legend
            legend_frame = tk.Frame(count_legend_frame, bg='#ffffff')
            legend_frame.pack(side='right')

            tk.Label(legend_frame, text="Legend:", bg='#ffffff', fg='#64748b',
                    font=('Arial', 9, 'bold')).pack(side='left', padx=(0, 10))
            tk.Label(legend_frame, text="🟢 Stock Increased", bg='#d1fae5', fg='#065f46',
                    font=('Arial', 8, 'bold'), padx=8, pady=2).pack(side='left', padx=2)
            tk.Label(legend_frame, text="🔴 Stock Decreased", bg='#fee2e2', fg='#991b1b',
                    font=('Arial', 8, 'bold'), padx=8, pady=2).pack(side='left', padx=2)
            tk.Label(legend_frame, text="⚪ No Change", bg='#f3f4f6', fg='#4b5563',
                    font=('Arial', 8, 'bold'), padx=8, pady=2).pack(side='left', padx=2)

            # ═══════════════════════════════════════════════════════
            # TAB 2: SUMMARY BY ITEM
            # ═══════════════════════════════════════════════════════
            summary_tab = tk.Frame(notebook, bg='#ffffff')
            notebook.add(summary_tab, text="📊 Summary by Item")

            # Summary controls
            summary_control = tk.Frame(summary_tab, bg='#ffffff')
            summary_control.pack(fill='x', padx=20, pady=15)

            tk.Label(summary_control, text="Period:", bg='#ffffff',
                    font=('Arial', 11)).pack(side='left', padx=(0, 10))

            summary_days_var = tk.StringVar(value="30 days")
            summary_days_combo = ttk.Combobox(summary_control, textvariable=summary_days_var,
                                             values=["7 days", "30 days", "90 days", "180 days", "365 days"],
                                             state='readonly', width=12)
            summary_days_combo.pack(side='left', padx=(0, 15))

            def refresh_summary():
                days = int(summary_days_var.get().split()[0])
                summary = get_stock_summary_by_item(days=days)

                # Clear tree
                for item in summary_tree.get_children():
                    summary_tree.delete(item)

                # Populate
                for s in summary:
                    summary_tree.insert('', 'end', values=(
                        s['item_name'],
                        s['bag_name'] or 'N/A',
                        s['total_added'],
                        s['total_removed'],
                        s['min_stock'],
                        s['max_stock'],
                        s['change_count']
                    ))

                summary_count.config(text=f"Items tracked: {len(summary)}")

            tk.Button(summary_control, text="🔄 Refresh", command=refresh_summary,
                     bg='#6366f1', fg='white', font=('Arial', 10, 'bold'),
                     padx=15, pady=5).pack(side='left')

            # Summary tree
            summary_tree_frame = tk.Frame(summary_tab, bg='#ffffff')
            summary_tree_frame.pack(fill='both', expand=True, padx=20, pady=(0, 10))

            summary_scroll = tk.Scrollbar(summary_tree_frame)
            summary_scroll.pack(side='right', fill='y')

            summary_tree = ttk.Treeview(summary_tree_frame,
                                       columns=('item', 'bag', 'added', 'removed', 'min', 'max', 'changes'),
                                       show='headings',
                                       yscrollcommand=summary_scroll.set,
                                       height=20)

            summary_scroll.config(command=summary_tree.yview)

            summary_tree.heading('item', text='Item Name')
            summary_tree.heading('bag', text='Bag')
            summary_tree.heading('added', text='Total Added')
            summary_tree.heading('removed', text='Total Sold')
            summary_tree.heading('min', text='Min Stock')
            summary_tree.heading('max', text='Max Stock')
            summary_tree.heading('changes', text='# Changes')

            summary_tree.column('item', width=200)
            summary_tree.column('bag', width=150)
            summary_tree.column('added', width=100)
            summary_tree.column('removed', width=100)
            summary_tree.column('min', width=100)
            summary_tree.column('max', width=100)
            summary_tree.column('changes', width=100)

            summary_tree.pack(fill='both', expand=True)

            summary_count = tk.Label(summary_tab, text="Items tracked: 0", bg='#ffffff',
                                    font=('Arial', 10), fg='#666666')
            summary_count.pack(pady=(5, 10))

            # Load item filter options
            try:
                bags = get_bags()
                all_items = []
                for bag_id, bag_name in bags:
                    items = get_items_in_bag(bag_id)
                    for item_id, item_name, price, stock in items:
                        all_items.append(f"{item_name} (ID: {item_id})")

                item_combo['values'] = ["All Items"] + all_items
                item_combo.set("All Items")
            except:
                pass

            # Initial load
            refresh_history()
            refresh_summary()

            # Close button
            tk.Button(dlg, text="Close", command=dlg.destroy,
                     bg='#95a5a6', fg='white', font=('Arial', 12, 'bold'),
                     padx=30, pady=10).pack(pady=15)

        def export_stock_report():
            import csv
            import os
            from datetime import datetime
            from sales_utils import get_all_stock
            
            # Create exports directory if it doesn't exist
            if not os.path.exists('exports'):
                os.makedirs('exports')
            
            stock_list = get_all_stock()
            today = datetime.now().strftime('%Y-%m-%d')
            path = f"exports/stock_report_{today}.csv"
            
            try:
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Item', 'Quantity', 'Category'])
                    for item, qty, cat in stock_list:  # Fixed: unpack 3 values
                        writer.writerow([item, qty, cat])
                messagebox.showinfo("Export Stock Report", f"Stock report exported to:\n{path}")
            except Exception as e:
                messagebox.showerror("Export Error", f"Failed to export stock report:\n{str(e)}")

        def view_stock():
            """Open a stock viewer with filters, sortable table, metrics, and CSV export."""
            from tkinter import Toplevel, StringVar
            from sales_utils import get_all_stock, get_item_prices, get_categories
            import csv, os
            from datetime import datetime

            win = Toplevel(root)
            win.title("View Stock")
            win.configure(bg='#f7f9fa')
            win.resizable(True, True)
            win.minsize(900, 600)

            # Start maximized or use good default size
            try:
                win.state('zoomed')  # Maximize on Windows
            except:
                win.geometry("1200x700")
                try:
                    win.update_idletasks()
                    width = 1200
                    height = 700
                    x = (win.winfo_screenwidth() // 2) - (width // 2)
                    y = (win.winfo_screenheight() // 2) - (height // 2)
                    win.geometry(f'{width}x{height}+{x}+{y}')
                except:
                    pass

            # Header
            header = tk.Frame(win, bg="#2c3e50")
            header.pack(fill='x')
            tk.Label(header, text="Stock Overview", fg='white', bg="#2c3e50",
                     font=('Segoe UI', 14, 'bold'), padx=12, pady=10).pack(side='left')

            # Filters
            filter_frame = tk.Frame(win, bg='#f7f9fa')
            filter_frame.pack(fill='x', padx=12, pady=(10,6))

            tk.Label(filter_frame, text="Search:", bg='#f7f9fa').grid(row=0, column=0, sticky='e', padx=(0,6))
            search_var = StringVar()
            search_entry = tk.Entry(filter_frame, textvariable=search_var, font=('Segoe UI', 10))
            search_entry.grid(row=0, column=1, sticky='w', padx=(0,12))

            tk.Label(filter_frame, text="Category:", bg='#f7f9fa').grid(row=0, column=2, sticky='e', padx=(0,6))
            categories = get_categories() or []
            cat_options = ['All'] + categories
            cat_var = StringVar(value='All')
            cat_combo = ttk.Combobox(filter_frame, textvariable=cat_var, values=cat_options, state='readonly', width=18)
            cat_combo.grid(row=0, column=3, sticky='w', padx=(0,12))

            tk.Label(filter_frame, text="Low stock ≤", bg='#f7f9fa').grid(row=0, column=4, sticky='e', padx=(0,6))
            low_var = StringVar(value='5')
            low_entry = tk.Entry(filter_frame, textvariable=low_var, width=6)
            low_entry.grid(row=0, column=5, sticky='w')

            def parse_int(v):
                try:
                    return max(0, int(str(v).strip()))
                except Exception:
                    return 0

            # Metrics
            metrics = tk.Frame(win, bg='#ecf0f1')
            metrics.pack(fill='x', padx=12, pady=(0,8))
            m_total_items = tk.StringVar(value='0')
            m_total_qty = tk.StringVar(value='0')
            m_total_cost = tk.StringVar(value='ZMW 0.00')
            m_total_sell = tk.StringVar(value='ZMW 0.00')
            m_profit = tk.StringVar(value='ZMW 0.00')

            def metric(label, var, col):
                box = tk.Frame(metrics, bg='#ecf0f1', padx=8, pady=6)
                box.grid(row=0, column=col, sticky='w')
                tk.Label(box, text=label, bg='#ecf0f1', fg='#7f8c8d', font=('Segoe UI', 9)).pack(anchor='w')
                tk.Label(box, textvariable=var, bg='#ecf0f1', fg='#2c3e50', font=('Segoe UI', 11, 'bold')).pack(anchor='w')
            metric('Items', m_total_items, 0)
            metric('Total Qty', m_total_qty, 1)
            metric('Stock Value (Cost)', m_total_cost, 2)
            metric('Stock Value (Sell)', m_total_sell, 3)
            metric('Potential Profit', m_profit, 4)

            # Table
            table_frame = tk.Frame(win, bg='#f7f9fa')
            table_frame.pack(fill='both', expand=True, padx=12, pady=(0,10))

            columns = ('item','category','qty','cost','sell','value_cost','value_sell','profit_u')
            tree = ttk.Treeview(table_frame, columns=columns, show='headings', height=20)
            headings = {
                'item': 'Item',
                'category': 'Category',
                'qty': 'Qty',
                'cost': 'Cost',
                'sell': 'Sell',
                'value_cost': 'Value (Cost)',
                'value_sell': 'Value (Sell)',
                'profit_u': 'Profit/Unit'
            }
            widths = {
                'item': 220, 'category': 140, 'qty': 70, 'cost': 90, 'sell': 90,
                'value_cost': 120, 'value_sell': 120, 'profit_u': 110
            }

            for col in columns:
                tree.heading(col, text=headings[col])
                # Use proper anchor values: 'w' for left, 'e' for right, 'center' for center
                if col in ('qty', 'cost', 'sell', 'value_cost', 'value_sell', 'profit_u'):
                    tree.column(col, width=widths[col], anchor='e')
                else:
                    tree.column(col, width=widths[col], anchor='w')

            vsb = ttk.Scrollbar(table_frame, orient='vertical', command=tree.yview)
            tree.configure(yscrollcommand=vsb.set)
            tree.pack(side='left', fill='both', expand=True)
            vsb.pack(side='right', fill='y')

            # Row style
            try:
                tree.tag_configure('low', background='#fdecea')
                tree.tag_configure('odd', background='#fafafa')
            except Exception:
                pass

            # Data store
            raw = []

            def load_data():
                nonlocal raw
                raw = []
                for name, qty, cat in get_all_stock():
                    try:
                        cost, sell = get_item_prices(name)
                    except Exception:
                        cost, sell = (None, None)
                    cost = float(cost or 0)
                    sell = float(sell or 0)
                    qty_i = int(qty or 0)
                    raw.append({
                        'item': name,
                        'category': cat or '',
                        'qty': qty_i,
                        'cost': cost,
                        'sell': sell,
                        'value_cost': cost * qty_i,
                        'value_sell': sell * qty_i,
                        'profit_u': sell - cost,
                    })

            load_data()

            def populate(rows):
                # clear
                for iid in tree.get_children():
                    tree.delete(iid)
                # metrics
                m_total_items.set(str(len(rows)))
                m_total_qty.set(str(sum(r['qty'] for r in rows)))
                total_cost = sum(r['value_cost'] for r in rows)
                total_sell = sum(r['value_sell'] for r in rows)
                m_total_cost.set(f"ZMW {total_cost:.2f}")
                m_total_sell.set(f"ZMW {total_sell:.2f}")
                m_profit.set(f"ZMW {(total_sell - total_cost):.2f}")
                # insert
                low_th = parse_int(low_var.get())
                for idx, r in enumerate(rows):
                    tags = []
                    if r['qty'] <= low_th:
                        tags.append('low')
                    if idx % 2 == 1:
                        tags.append('odd')
                    tree.insert('', 'end', values=(
                        r['item'], r['category'], r['qty'],
                        f"{r['cost']:.2f}", f"{r['sell']:.2f}",
                        f"{r['value_cost']:.2f}", f"{r['value_sell']:.2f}", f"{r['profit_u']:.2f}"
                    ), tags=tags)

            def apply_filters(event=None):
                term = (search_var.get() or '').strip().lower()
                cat_sel = cat_var.get()
                rows = raw
                if term:
                    rows = [r for r in rows if term in r['item'].lower()]
                if cat_sel and cat_sel != 'All':
                    rows = [r for r in rows if (r['category'] or '') == cat_sel]
                populate(rows)

            def tree_to_rows():
                idx_map = {(r['item'], r['category']): r for r in raw}
                for iid in tree.get_children():
                    vals = tree.item(iid, 'values')
                    r = idx_map.get((vals[0], vals[1]))
                    if r:
                        yield (iid, r)

            # Sort by column
            sort_state = {c: False for c in columns}
            def sort_by(col):
                reverse = sort_state[col]
                def keyer(r):
                    if col in ('qty','cost','sell','value_cost','value_sell','profit_u'):
                        return float(r[col])
                    return (r[col] or '').lower()
                rows = list(tree_to_rows())
                rows.sort(key=lambda rv: keyer(rv[1]), reverse=reverse)
                # update state
                sort_state[col] = not reverse
                # repopulate from sorted
                populate([rv[1] for rv in rows])

            # Fix lambda closure issue by creating proper function references
            def make_sort_command(column):
                return lambda: sort_by(column)

            for c in columns:
                tree.heading(c, text=headings[c], command=make_sort_command(c))

            # Action bar
            actions = tk.Frame(win, bg='#f7f9fa')
            actions.pack(fill='x', padx=12, pady=(0,12))
            def export_csv():
                try:
                    rows = [rv[1] for rv in tree_to_rows()]
                    if not rows:
                        messagebox.showinfo("Export", "No rows to export.")
                        return
                    if not os.path.exists('exports'):
                        os.makedirs('exports')
                    path = os.path.join('exports', f"stock_view_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv")
                    with open(path, 'w', newline='', encoding='utf-8') as f:
                        w = csv.writer(f)
                        w.writerow([headings[c] for c in columns])
                        for r in rows:
                            w.writerow([
                                r['item'], r['category'], r['qty'],
                                f"{r['cost']:.2f}", f"{r['sell']:.2f}",
                                f"{r['value_cost']:.2f}", f"{r['value_sell']:.2f}", f"{r['profit_u']:.2f}"
                            ])
                    messagebox.showinfo("Export", f"Exported to:\n{path}")
                except Exception as e:
                    messagebox.showerror("Export Error", str(e))

            tk.Button(actions, text="Export CSV (Filtered)", command=export_csv, bg='#16a085', fg='white', font=('Segoe UI', 10, 'bold')).pack(side='right')

            # Initial populate
            apply_filters()

        def show_stock_reconciliation():
            """Stock reconciliation tool using: Balance = Old + New - Sold - Loss/Drawn"""
            import sqlite3
            from datetime import datetime
            from tkinter import Toplevel, Label, Entry, Button, StringVar
            from sales_utils import get_all_stock, update_stock, log_audit_event

            win = Toplevel(root)
            win.title("Stock Reconciliation")
            win.geometry("460x480")

            # Load items and current quantities
            stock_list = get_all_stock()
            items = [row[0] for row in stock_list]
            qty_map = {row[0]: int(row[1]) for row in stock_list}

            Label(win, text="Select Item:").grid(row=0, column=0, sticky='e', padx=5, pady=8)
            item_var = tk.StringVar(win)
            item_combo = ttk.Combobox(win, textvariable=item_var, values=items, state='readonly', width=28)
            item_combo.grid(row=0, column=1, columnspan=2, sticky='w', padx=5)
            if items:
                item_var.set(items[0])

            # Variables
            old_var = StringVar(value="0")
            new_var = StringVar(value="0")
            sold_var = StringVar(value="0")
            loss_var = StringVar(value="0")
            balance_var = StringVar(value="0")

            # Display fields
            Label(win, text="Old Stock:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
            old_entry = Entry(win, textvariable=old_var, width=12)
            old_entry.grid(row=1, column=1, sticky='w', padx=5)

            Label(win, text="New Stock (received):").grid(row=2, column=0, sticky='e', padx=5, pady=5)
            new_entry = Entry(win, textvariable=new_var, width=12)
            new_entry.grid(row=2, column=1, sticky='w', padx=5)

            Label(win, text="Sold:").grid(row=3, column=0, sticky='e', padx=5, pady=5)
            sold_entry = Entry(win, textvariable=sold_var, width=12)
            sold_entry.grid(row=3, column=1, sticky='w', padx=5)

            Label(win, text="Loss/Drawn:").grid(row=4, column=0, sticky='e', padx=5, pady=5)
            loss_entry = Entry(win, textvariable=loss_var, width=12)
            loss_entry.grid(row=4, column=1, sticky='w', padx=5)

            # Formula hint
            Label(win, text="Balance = Old + New - Sold - Loss/Drawn", fg="#7f8c8d").grid(row=5, column=0, columnspan=3, pady=(5,10))

            # Balance display
            Label(win, text="Computed Balance:", font=('Arial', 11, 'bold')).grid(row=6, column=0, sticky='e', padx=5, pady=5)
            balance_label = Label(win, textvariable=balance_var, font=('Arial', 12, 'bold'), fg="#27ae60")
            balance_label.grid(row=6, column=1, sticky='w', padx=5, pady=5)

            status_label = Label(win, text="", fg='red')
            status_label.grid(row=7, column=0, columnspan=3, pady=(0,10))

            def parse_int(s: str) -> int:
                try:
                    v = int(str(s).strip() or "0")
                    return max(0, v)
                except Exception:
                    return 0

            def recompute(*args):
                o = parse_int(old_var.get())
                n = parse_int(new_var.get())
                s = parse_int(sold_var.get())
                l = parse_int(loss_var.get())
                bal = o + n - s - l
                balance_var.set(str(max(0, bal)))
                # Visual feedback
                if bal < 0:
                    balance_label.config(fg="#e74c3c")
                    status_label.config(text="Warning: Computed balance is negative. Check inputs.")
                    apply_btn.config(state='disabled')
                else:
                    balance_label.config(fg="#27ae60")
                    status_label.config(text="")
                    apply_btn.config(state='normal')

            def on_item_change(*args):
                """Update old stock value when item selection changes"""
                item = item_var.get()
                cur_qty = qty_map.get(item, 0)
                old_var.set(str(cur_qty))
                recompute()

            # Trace variable changes
            item_var.trace_add('write', on_item_change)

            # Auto-calc sold today for selected item
            def auto_calc_sold_today():
                """Calculate items sold today for selected item"""
                item = item_var.get()
                if not item:
                    return
                conn = None
                try:
                    conn = get_db()
                    cur = conn.cursor()
                    today = datetime.now().strftime('%Y-%m-%d')
                    cur.execute(
                        """
                        SELECT COALESCE(SUM(si.quantity),0)
                        FROM sale_items si
                        JOIN sales s ON s.id = si.sale_id
                        WHERE s.status != 'VOIDED' AND DATE(s.timestamp)=? AND si.item=?
                        """,
                        (today, item)
                    )
                    sold_today = cur.fetchone()[0] or 0
                    sold_var.set(str(int(sold_today)))
                except Exception as e:
                    status_label.config(text=f"Error calculating sold today: {e}")
                finally:
                    if conn:
                        try:
                            conn.close()
                        except Exception:
                            pass
                recompute()

            # Buttons row for helpers
            btn_calc = Button(win, text="Auto-calc Sold (Today)", command=auto_calc_sold_today, bg='#3498db', fg='white')
            btn_calc.grid(row=3, column=2, padx=5, pady=5)

            # Initial set for first item
            on_item_change()

            def apply_balance():
                item = item_var.get()
                if not item:
                    status_label.config(text="Select an item first")
                    return
                cur_qty = qty_map.get(item, 0)
                o = parse_int(old_var.get())
                n = parse_int(new_var.get())
                s = parse_int(sold_var.get())
                l = parse_int(loss_var.get())
                bal = o + n - s - l
                if bal < 0:
                    messagebox.showerror("Invalid Balance", "Balance cannot be negative. Adjust inputs.")
                    return
                # Adjust inventory to the computed balance
                delta = bal - cur_qty
                if delta != 0:
                    update_stock(item, delta)
                    # refresh local view
                    qty_map[item] = cur_qty + delta
                log_audit_event(
                    f"Stock reconciled by {current_user['username']}: {item} => old={o}, new={n}, sold={s}, loss={l}, balance={bal}, delta={delta}")
                messagebox.showinfo("Reconciliation Applied", f"Inventory updated for '{item}'. New quantity: {bal}")
                win.destroy()

            apply_btn = Button(win, text="Apply Balance", command=apply_balance, bg='#27ae60', fg='white')
            apply_btn.grid(row=8, column=0, columnspan=3, pady=15)

            # Focus order
            new_entry.focus_set()

        def reset_all_data():
            """🔥 COMPLETE DATA RESET - Password-protected destructive reset for admin use.

            ⚠️ WARNING: This will DELETE EVERYTHING except admin password:
            - All sales and transactions
            - All stock history records
            - All bags and items
            - All inventory data
            - All expenses
            - All non-admin users
            - All sequence counters

            ONLY PRESERVES: Admin user accounts

            Requires admin password confirmation and explicit user confirmation.
            """
            import sqlite3
            from sales_utils import get_user, check_password

            # Prompt for admin password in a small dialog
            dlg = tk.Toplevel(root)
            dlg.title("⚠️ COMPLETE DATA RESET")
            dlg.geometry("450x250")
            dlg.configure(bg='#fff3cd')
            dlg.transient(root)
            dlg.grab_set()

            # Warning header
            warning_frame = tk.Frame(dlg, bg='#dc3545', height=60)
            warning_frame.pack(fill='x')
            warning_frame.pack_propagate(False)
            tk.Label(warning_frame, text="⚠️ DANGER ZONE ⚠️", font=('Arial', 16, 'bold'),
                    bg='#dc3545', fg='white').pack(pady=15)

            content_frame = tk.Frame(dlg, bg='#fff3cd')
            content_frame.pack(fill='both', expand=True, padx=20, pady=20)

            tk.Label(content_frame, text="This will DELETE ALL DATA including:",
                    font=('Arial', 10, 'bold'), bg='#fff3cd', fg='#856404').pack(pady=(0, 5))

            warning_text = """• All sales & transactions
• All stock history records
• All bags & items
• All inventory data
• All expenses
• All non-admin users

ONLY admin password will remain!"""

            tk.Label(content_frame, text=warning_text, font=('Arial', 9),
                    bg='#fff3cd', fg='#856404', justify='left').pack(pady=(0, 15))

            tk.Label(content_frame, text="Enter Admin Password to Continue:",
                    font=('Arial', 10, 'bold'), bg='#fff3cd', fg='#721c24').pack()
            pw_var = tk.StringVar()
            pw_entry = tk.Entry(content_frame, textvariable=pw_var, show='*',
                               font=('Arial', 11), width=30)
            pw_entry.pack(pady=5)

            status_lbl = tk.Label(content_frame, text="", fg='#dc3545', bg='#fff3cd',
                                 font=('Arial', 9, 'bold'))
            status_lbl.pack(pady=(5, 0))

            def do_reset_confirm():
                pw = pw_var.get() or ''
                # Verify admin password against DB or fallback to built-in USERS
                verified = False
                try:
                    admin_user = get_user(current_user['username'])
                except Exception:
                    admin_user = None

                if admin_user and admin_user.get('role') == 'admin' and admin_user.get('password_hash'):
                    try:
                        if check_password(pw, admin_user['password_hash']):
                            verified = True
                    except Exception:
                        verified = False
                else:
                    # Fallback to static admin password if configured in USERS dict
                    try:
                        if pw == USERS.get('admin', {}).get('password'):
                            verified = True
                    except Exception:
                        verified = False

                if not verified:
                    status_lbl.config(text="❌ Password incorrect. Access denied.")
                    return

                # Final confirmation (destructive)
                confirm_msg = """🔥 FINAL WARNING 🔥

This will PERMANENTLY DELETE:
✗ All sales records
✗ All stock history (every change ever made)
✗ All bags and items
✗ All inventory
✗ All expenses
✗ All non-admin users
✗ Everything except admin password

This CANNOT be undone!

Are you ABSOLUTELY SURE you want to proceed?"""

                if not messagebox.askyesno("⚠️ FINAL CONFIRMATION", confirm_msg, icon='warning'):
                    dlg.destroy()
                    return

                # Perform complete DB reset
                try:
                    conn = sqlite3.connect(DB_NAME, timeout=30)
                    cur = conn.cursor()
                    cur.execute("BEGIN IMMEDIATE")

                    deleted_tables = []

                    # 1. Remove all non-admin users
                    try:
                        cur.execute("SELECT COUNT(*) FROM users WHERE role != 'admin'")
                        count = cur.fetchone()[0]
                        cur.execute("DELETE FROM users WHERE role != 'admin'")
                        deleted_tables.append(f"Non-admin users: {count}")
                    except Exception as e:
                        deleted_tables.append(f"Users: skipped ({e})")

                    # 2. DELETE STOCK HISTORY (NEW!)
                    try:
                        cur.execute("SELECT COUNT(*) FROM item_stock_history")
                        count = cur.fetchone()[0]
                        cur.execute("DELETE FROM item_stock_history")
                        deleted_tables.append(f"Stock history: {count} records")
                    except Exception as e:
                        deleted_tables.append(f"Stock history: skipped ({e})")

                    # 3. Delete bag items and bags
                    try:
                        cur.execute("SELECT COUNT(*) FROM items")
                        items_count = cur.fetchone()[0]
                        cur.execute("DELETE FROM items")
                        cur.execute("SELECT COUNT(*) FROM bags")
                        bags_count = cur.fetchone()[0]
                        cur.execute("DELETE FROM bags")
                        deleted_tables.append(f"Bags: {bags_count}, Items: {items_count}")
                    except Exception as e:
                        deleted_tables.append(f"Bags/Items: skipped ({e})")

                    # 4. Zero legacy inventory
                    try:
                        cur.execute("SELECT COUNT(*) FROM inventory")
                        count = cur.fetchone()[0]
                        cur.execute("DELETE FROM inventory")
                        deleted_tables.append(f"Legacy inventory: {count}")
                    except Exception as e:
                        deleted_tables.append(f"Legacy inventory: skipped ({e})")

                    # 5. Delete sales and sale items
                    try:
                        cur.execute("SELECT COUNT(*) FROM sale_items")
                        items_count = cur.fetchone()[0]
                        cur.execute("DELETE FROM sale_items")
                        cur.execute("SELECT COUNT(*) FROM sales")
                        sales_count = cur.fetchone()[0]
                        cur.execute("DELETE FROM sales")
                        deleted_tables.append(f"Sales: {sales_count}, Line items: {items_count}")
                    except Exception as e:
                        deleted_tables.append(f"Sales: skipped ({e})")

                    # 6. Delete legacy sales
                    try:
                        cur.execute("SELECT COUNT(*) FROM sales_legacy")
                        count = cur.fetchone()[0]
                        cur.execute("DELETE FROM sales_legacy")
                        deleted_tables.append(f"Legacy sales: {count}")
                    except Exception as e:
                        deleted_tables.append(f"Legacy sales: skipped ({e})")

                    # 7. Delete expenses
                    try:
                        cur.execute("SELECT COUNT(*) FROM expenses")
                        count = cur.fetchone()[0]
                        cur.execute("DELETE FROM expenses")
                        deleted_tables.append(f"Expenses: {count}")
                    except Exception as e:
                        deleted_tables.append(f"Expenses: skipped ({e})")

                    # 8. Reset all sequence counters
                    try:
                        cur.execute("""DELETE FROM sqlite_sequence WHERE name IN 
                                    ('bags','items','sales','sale_items','sales_legacy',
                                     'inventory','item_stock_history','expenses')""")
                        deleted_tables.append("All ID sequences reset")
                    except Exception as e:
                        deleted_tables.append(f"Sequences: skipped ({e})")

                    conn.commit()
                    conn.close()

                    try:
                        log_audit_event(f"🔥 COMPLETE DATA RESET performed by {current_user.get('username')}")
                    except Exception:
                        pass

                    # Show detailed success message
                    summary = "✅ COMPLETE RESET SUCCESSFUL\n\n" + "\n".join(f"• {item}" for item in deleted_tables)
                    messagebox.showinfo("Reset Complete", summary)

                except Exception as e:
                    try:
                        conn.rollback()
                        conn.close()
                    except Exception:
                        pass
                    try:
                        log_audit_event(f"🔥 COMPLETE DATA RESET FAILED by {current_user.get('username')}: {e}")
                    except Exception:
                        pass
                    messagebox.showerror("Reset Error", f"Reset failed:\n\n{str(e)}")
                finally:
                    dlg.destroy()

            btn_frame = tk.Frame(content_frame, bg='#fff3cd')
            btn_frame.pack(pady=(15, 0))

            tk.Button(btn_frame, text="🔥 RESET ALL DATA", command=do_reset_confirm,
                     bg='#dc3545', fg='white', font=('Arial', 11, 'bold'),
                     padx=20, pady=8).pack(side='left', padx=5)
            tk.Button(btn_frame, text="Cancel", command=dlg.destroy,
                     bg='#6c757d', fg='white', font=('Arial', 11, 'bold'),
                     padx=20, pady=8).pack(side='left', padx=5)

            pw_entry.focus_set()
            pw_entry.bind('<Return>', lambda e: do_reset_confirm())

            tk.Button(dlg, text="Execute Reset", command=do_reset_confirm, bg='#c0392b', fg='white').pack(side='left', padx=18, pady=12)
            tk.Button(dlg, text="Cancel", command=dlg.destroy).pack(side='right', padx=18, pady=12)
            pw_entry.focus_set()




        def reset_password():
            import sqlite3
            import sales_utils
            win_pwd = tk.Toplevel(root)
            win_pwd.title("Reset Password")
            tk.Label(win_pwd, text="Current Password:").grid(row=0, column=0, sticky='e', padx=6, pady=6)
            tk.Label(win_pwd, text="New Password:").grid(row=1, column=0, sticky='e', padx=6, pady=6)
            tk.Label(win_pwd, text="Confirm New Password:").grid(row=2, column=0, sticky='e', padx=6, pady=6)
            cur_var = tk.StringVar()
            new_var = tk.StringVar()
            conf_var = tk.StringVar()
            e1 = tk.Entry(win_pwd, textvariable=cur_var, show='*', width=24)
            e2 = tk.Entry(win_pwd, textvariable=new_var, show='*', width=24)
            e3 = tk.Entry(win_pwd, textvariable=conf_var, show='*', width=24)
            e1.grid(row=0, column=1, padx=6, pady=6)
            e2.grid(row=1, column=1, padx=6, pady=6)
            e3.grid(row=2, column=1, padx=6, pady=6)
            status = tk.Label(win_pwd, text="", fg='red')
            status.grid(row=3, column=0, columnspan=2)
            def do_reset():
                """Reset user password with verification"""
                # Master account cannot be changed
                if current_user['username'].strip().lower() == 'comfort':
                    status.config(text="Master account password cannot be changed.")
                    return
                cur_pw = cur_var.get()
                new_pw = new_var.get()
                conf_pw = conf_var.get()
                if not new_pw or new_pw != conf_pw:
                    status.config(text="New passwords do not match or are empty.")
                    return
                # Verify current
                verified = False
                dbu = None
                get_user = None
                check_password = None
                create_user = None
                try:
                    from sales_utils import get_user, check_password, create_user
                    dbu = get_user(current_user['username'])
                except Exception:
                    pass

                if dbu and check_password and check_password(cur_pw, dbu['password_hash']):
                    verified = True
                    try:
                        conn = get_db()
                        cur = conn.cursor()
                        new_hash = sales_utils.hash_password(new_pw)
                        cur.execute("UPDATE users SET password_hash=? WHERE username=?", (new_hash, current_user['username']))
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        status.config(text=f"DB update failed: {e}")
                        return
                elif current_user['username'] == 'admin' and USERS['admin']['password'] == cur_pw and create_user:
                    try:
                        create_user('admin', new_pw, 'admin')
                        verified = True
                    except Exception as e:
                        status.config(text=f"DB create failed: {e}")
                        return
                if not verified:
                    status.config(text="Current password incorrect.")
                    return
                log_audit_event(f"Password reset for {current_user['username']}")
                messagebox.showinfo("Password Reset", "Password updated successfully.")
                win_pwd.destroy()
            tk.Button(win_pwd, text="Update Password", command=do_reset, bg='#27ae60', fg='white').grid(row=4, column=0, columnspan=2, pady=8)


        # ══════════════════════════════════════════════════════════════════════
        # MODERN GEN-Z ADMIN DASHBOARD WITH SIDEBAR NAVIGATION
        # ══════════════════════════════════════════════════════════════════════

        # Color palette - Ultra Modern Gen-Z theme
        SIDEBAR_BG = '#0f0f1e'        # Ultra dark navy (almost black)
        SIDEBAR_HOVER = '#1a1a2e'     # Darker hover with subtle lift
        ACCENT_PRIMARY = '#6366f1'    # Electric indigo (vibrant)
        ACCENT_SECONDARY = '#a855f7'  # Vivid purple
        ACCENT_TEAL = '#06b6d4'       # Bright cyan
        ACCENT_PINK = '#ec4899'       # Hot pink
        ACCENT_ORANGE = '#fb923c'     # Warm orange
        ACCENT_RED = '#f43f5e'        # Rose red
        TEXT_PRIMARY = '#f8fafc'      # Crisp white text
        TEXT_SECONDARY = '#94a3b8'    # Cool gray text
        MAIN_BG = '#f1f5f9'           # Light gray background
        CARD_BG = '#ffffff'           # White cards
        SCROLLBAR_BG = '#1e1e2f'      # Scrollbar track
        SCROLLBAR_THUMB = '#6366f1'   # Scrollbar thumb (matches accent)

        # Main container - horizontal split
        admin_container = tk.Frame(main_frame, bg=MAIN_BG)
        admin_container.pack(fill='both', expand=True)

        # ═══════════════════════════════════════════════════════════════
        # LEFT SIDEBAR - Modern Gen-Z Navigation with Custom Scrolling
        # ═══════════════════════════════════════════════════════════════
        sidebar_outer = tk.Frame(admin_container, bg=SIDEBAR_BG, width=280)
        sidebar_outer.pack(side='left', fill='y')
        sidebar_outer.pack_propagate(False)

        # Create canvas for sidebar with custom scrolling
        sidebar_canvas = tk.Canvas(sidebar_outer, bg=SIDEBAR_BG, highlightthickness=0, width=260, bd=0)

        # Custom styled scrollbar using ttk
        style = ttk.Style()
        style.theme_use('default')

        # Configure modern scrollbar style
        style.configure('Sidebar.Vertical.TScrollbar',
                       background=SCROLLBAR_BG,
                       troughcolor=SCROLLBAR_BG,
                       bordercolor=SIDEBAR_BG,
                       arrowcolor=TEXT_SECONDARY,
                       relief='flat',
                       borderwidth=0)

        style.map('Sidebar.Vertical.TScrollbar',
                 background=[('active', SCROLLBAR_THUMB), ('!active', '#4a5568')],
                 arrowcolor=[('active', TEXT_PRIMARY), ('!active', TEXT_SECONDARY)])

        sidebar_scrollbar = ttk.Scrollbar(sidebar_outer, orient='vertical',
                                         command=sidebar_canvas.yview,
                                         style='Sidebar.Vertical.TScrollbar')

        sidebar = tk.Frame(sidebar_canvas, bg=SIDEBAR_BG, width=260)

        # Configure scrolling
        sidebar.bind('<Configure>', lambda e: sidebar_canvas.configure(scrollregion=sidebar_canvas.bbox('all')))
        sidebar_canvas.create_window((0, 0), window=sidebar, anchor='nw', width=260)
        sidebar_canvas.configure(yscrollcommand=sidebar_scrollbar.set)

        sidebar_canvas.pack(side='left', fill='both', expand=True)
        sidebar_scrollbar.pack(side='right', fill='y', padx=(0, 2), pady=2)

        # ═══════════════════════════════════════════════════════════════
        # ADVANCED SCROLLING FUNCTIONALITY (Mouse + Keyboard)
        # ═══════════════════════════════════════════════════════════════

        # Mouse wheel scrolling
        def _on_mousewheel(event):
            try:
                sidebar_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
            except:
                pass

        # Keyboard scrolling support (Arrow keys, Page Up/Down, Home/End)
        def _on_keyboard_scroll(event):
            try:
                if event.keysym == 'Up':
                    sidebar_canvas.yview_scroll(-1, "units")
                elif event.keysym == 'Down':
                    sidebar_canvas.yview_scroll(1, "units")
                elif event.keysym == 'Prior':  # Page Up
                    sidebar_canvas.yview_scroll(-5, "units")
                elif event.keysym == 'Next':  # Page Down
                    sidebar_canvas.yview_scroll(5, "units")
                elif event.keysym == 'Home':
                    sidebar_canvas.yview_moveto(0)
                elif event.keysym == 'End':
                    sidebar_canvas.yview_moveto(1)
            except:
                pass

        # Bind mousewheel to sidebar components
        sidebar_canvas.bind('<MouseWheel>', _on_mousewheel)
        sidebar_outer.bind('<MouseWheel>', _on_mousewheel)

        # Bind keyboard navigation
        sidebar_canvas.bind('<Up>', _on_keyboard_scroll)
        sidebar_canvas.bind('<Down>', _on_keyboard_scroll)
        sidebar_canvas.bind('<Prior>', _on_keyboard_scroll)
        sidebar_canvas.bind('<Next>', _on_keyboard_scroll)
        sidebar_canvas.bind('<Home>', _on_keyboard_scroll)
        sidebar_canvas.bind('<End>', _on_keyboard_scroll)

        # Make canvas focusable for keyboard events
        sidebar_canvas.focus_set()

        # When mouse enters sidebar, set focus for keyboard scrolling
        def _set_focus(event):
            sidebar_canvas.focus_set()
        sidebar_outer.bind('<Enter>', _set_focus)

        # Recursively bind mousewheel to all children
        def bind_to_children(widget):
            """Recursively bind mousewheel to all children"""
            try:
                widget.bind('<MouseWheel>', _on_mousewheel)
                for child in widget.winfo_children():
                    bind_to_children(child)
            except:
                pass

        # Bind to all sidebar children after they're created
        root.after(100, lambda: bind_to_children(sidebar))

        # ═══════════════════════════════════════════════════════════════
        # SIDEBAR HEADER - Modern Logo Area with Gradient Effect
        # ═══════════════════════════════════════════════════════════════
        sidebar_header = tk.Frame(sidebar, bg=SIDEBAR_BG)
        sidebar_header.pack(fill='x', pady=(25, 20))

        # Load business name from settings
        try:
            import business_settings
            sidebar_business_name = business_settings.get_business_name()
        except Exception:
            sidebar_business_name = "Gorgeous Brides Boutique"

        # Logo emoji with gradient-like background
        logo_container = tk.Frame(sidebar_header, bg='#6366f1', width=60, height=60)
        logo_container.pack(pady=(0, 15))
        logo_container.pack_propagate(False)

        tk.Label(logo_container, text="✨", font=('Segoe UI Emoji', 30),
                bg='#6366f1', fg='white').place(relx=0.5, rely=0.5, anchor='center')

        # Title with modern typography
        tk.Label(sidebar_header, text="Admin Panel",
                font=('Segoe UI', 20, 'bold'), bg=SIDEBAR_BG, fg=TEXT_PRIMARY,
                justify='center').pack(pady=(0, 5))

        tk.Label(sidebar_header, text=sidebar_business_name,
                font=('Segoe UI', 10), bg=SIDEBAR_BG, fg=TEXT_SECONDARY,
                justify='center').pack(pady=(0, 5))

        # User badge
        user_badge = tk.Frame(sidebar_header, bg='#1e293b', bd=0)
        user_badge.pack(pady=(10, 0))
        tk.Label(user_badge, text=f"👤 {current_user.get('username', 'Admin')}",
                font=('Segoe UI', 9), bg='#1e293b', fg='#cbd5e1',
                padx=12, pady=6).pack()

        # Stylish separator with gradient effect
        sep_frame = tk.Frame(sidebar, bg=SIDEBAR_BG, height=2)
        sep_frame.pack(fill='x', padx=20, pady=(15, 20))
        tk.Frame(sep_frame, bg='#334155', height=1).pack(fill='x')
        tk.Frame(sep_frame, bg='#1e293b', height=1).pack(fill='x')

        # ═══════════════════════════════════════════════════════════════
        # MODERN BUTTON STYLING - Gen-Z Aesthetic
        # ═══════════════════════════════════════════════════════════════
        def create_sidebar_button(parent, text, icon, command, accent_color=ACCENT_PRIMARY):
            """Create an ultra-modern sidebar button with Gen-Z styling"""
            btn_container = tk.Frame(parent, bg=SIDEBAR_BG)
            btn_container.pack(fill='x', padx=15, pady=5)

            # Create button with modern styling
            btn = tk.Button(btn_container,
                          text=f"  {icon}  {text}",
                          command=command,
                          font=('Segoe UI', 11, 'normal'),
                          bg=SIDEBAR_BG,
                          fg=TEXT_SECONDARY,
                          activebackground=accent_color,
                          activeforeground='white',
                          bd=0,
                          relief='flat',
                          anchor='w',
                          padx=18,
                          pady=14,
                          cursor='hand2')
            btn.pack(fill='x')

            # Store the accent color for hover effects
            btn.accent_color = accent_color

            # Advanced hover effects with color transitions
            def on_enter(e):
                btn.config(bg=accent_color, fg='white', font=('Segoe UI', 11, 'bold'))

            def on_leave(e):
                btn.config(bg=SIDEBAR_BG, fg=TEXT_SECONDARY, font=('Segoe UI', 11, 'normal'))

            # Add subtle press effect
            def on_press(e):
                btn.config(bg=accent_color, relief='sunken')

            def on_release(e):
                btn.config(relief='flat')

            btn.bind('<Enter>', on_enter)
            btn.bind('<Leave>', on_leave)
            btn.bind('<ButtonPress-1>', on_press)
            btn.bind('<ButtonRelease-1>', on_release)

            return btn

        # Section Label with modern typography
        def create_section_label(parent, text):
            section_frame = tk.Frame(parent, bg=SIDEBAR_BG)
            section_frame.pack(fill='x', padx=20, pady=(25, 10))

            # Section title
            lbl = tk.Label(section_frame,
                          text=text.upper(),
                          font=('Segoe UI', 9, 'bold'),
                          bg=SIDEBAR_BG,
                          fg='#64748b',
                          anchor='w',
                          justify='left')
            lbl.pack(side='left')

            # Decorative line next to label
            line = tk.Frame(section_frame, bg='#334155', height=1)
            line.pack(side='left', fill='x', expand=True, padx=(10, 0))

        # ─────────────────────────────────────────────────────────────
        # SIDEBAR NAVIGATION BUTTONS (All original commands preserved)
        # ─────────────────────────────────────────────────────────────

        # Dashboard Section
        create_section_label(sidebar, "📊 Dashboard")
        create_sidebar_button(sidebar, "Monthly Report", "📅", lambda: show_monthly_sales_report(root), ACCENT_TEAL)

        # User Management Section
        create_section_label(sidebar, "👥 Users")
        create_sidebar_button(sidebar, "Manage Cashiers", "👤", user_management, ACCENT_SECONDARY)

        # Inventory Section
        create_section_label(sidebar, "📦 Inventory")
        create_sidebar_button(sidebar, "View Stock", "📋", view_stock, ACCENT_TEAL)
        create_sidebar_button(sidebar, "Low Stock Alerts", "⚠️", show_low_stock_alerts, ACCENT_ORANGE)
        create_sidebar_button(sidebar, "Stock Analytics", "📊", show_stock_analytics, ACCENT_PRIMARY)

        # Bag Management Section
        create_section_label(sidebar, "🛍️ Bags")
        create_sidebar_button(sidebar, "Manage Bags & Items", "📦", manage_bags_dialog, ACCENT_SECONDARY)
        create_sidebar_button(sidebar, "Stock History", "📊", view_stock_history_dialog, ACCENT_PRIMARY)

        # Finance Section
        create_section_label(sidebar, "💰 Finance")
        create_sidebar_button(sidebar, "Expenses", "💸", open_expenses, ACCENT_ORANGE)
        create_sidebar_button(sidebar, "Daily Sales", "📊", open_daily_sales, ACCENT_TEAL)

        # Export Section
        create_section_label(sidebar, "📤 Export")
        create_sidebar_button(sidebar, "Export All Sales", "💾", export_all_sales, ACCENT_TEAL)
        create_sidebar_button(sidebar, "Export Stock Report", "📄", export_stock_report, ACCENT_PRIMARY)

        # Settings Section
        create_section_label(sidebar, "⚙️ Settings")

        create_sidebar_button(sidebar, "Business Settings", "🏢", lambda: show_business_settings(root), ACCENT_PRIMARY)
        create_sidebar_button(sidebar, "Reset Password", "🔐", reset_password, ACCENT_SECONDARY)
        create_sidebar_button(sidebar, "Reset All Data", "🗑️", reset_all_data, ACCENT_RED)
        create_sidebar_button(sidebar, "Logout", "🚪", logout, ACCENT_RED)

        # ═══════════════════════════════════════════════════════════════
        # SIDEBAR FOOTER - Version & Info
        # ═══════════════════════════════════════════════════════════════
        tk.Frame(sidebar, bg=SIDEBAR_BG, height=20).pack(fill='x')

        footer_frame = tk.Frame(sidebar, bg='#1a1a2e', bd=0)
        footer_frame.pack(fill='x', padx=15, pady=(10, 20))

        tk.Label(footer_frame, text="💎 POS System v2.0",
                font=('Segoe UI', 8), bg='#1a1a2e', fg='#64748b',
                pady=8).pack()

        tk.Label(footer_frame, text="⌨️ Use ↑↓ arrows to scroll",
                font=('Segoe UI', 7), bg='#1a1a2e', fg='#475569',
                pady=3).pack()

        tk.Frame(sidebar, bg=SIDEBAR_BG, height=30).pack(fill='x')

        # ═══════════════════════════════════════════════════════════════
        # MAIN CONTENT AREA - Dashboard Placeholder
        # ═══════════════════════════════════════════════════════════════
        main_content = tk.Frame(admin_container, bg=MAIN_BG)
        main_content.pack(side='right', fill='both', expand=True)

        # Top bar in main content
        top_bar = tk.Frame(main_content, bg=CARD_BG, height=70)
        top_bar.pack(fill='x', padx=0, pady=0)
        top_bar.pack_propagate(False)

        # Top bar content
        top_bar_inner = tk.Frame(top_bar, bg=CARD_BG)
        top_bar_inner.pack(fill='both', expand=True, padx=30, pady=15)

        tk.Label(top_bar_inner, text="🏠 Dashboard",
                font=('Segoe UI', 16, 'bold'), bg=CARD_BG, fg='#1e293b').pack(side='left')

        # Date/Time display
        from datetime import datetime
        current_datetime = datetime.now().strftime("%A, %B %d, %Y")
        tk.Label(top_bar_inner, text=f"📅 {current_datetime}",
                font=('Segoe UI', 10), bg=CARD_BG, fg='#64748b').pack(side='right')

        # Shadow line under top bar
        shadow_line = tk.Frame(main_content, bg='#e2e8f0', height=2)
        shadow_line.pack(fill='x')

        # ═══════════════════════════════════════════════════════════════
        # DASHBOARD CONTENT AREA - Business Overview
        # ═══════════════════════════════════════════════════════════════
        dashboard_content = tk.Frame(main_content, bg=MAIN_BG)
        dashboard_content.pack(fill='both', expand=True, padx=30, pady=30)

        def load_dashboard_data():
            """Load summary data from database (read-only)"""
            try:
                conn = get_db()
                cursor = conn.cursor()

                # Get total sales amount and count
                cursor.execute("SELECT COUNT(*), COALESCE(SUM(total), 0) FROM sales")
                sales_count, sales_total = cursor.fetchone()

                # Get hire data (from sales where hire_details is not empty)
                cursor.execute("""
                    SELECT COUNT(*), COALESCE(SUM(total), 0) 
                    FROM sales 
                    WHERE hire_details IS NOT NULL AND hire_details != ''
                """)
                hire_count, hire_total = cursor.fetchone()

                # Get outstanding hire balances (half-paid)
                cursor.execute("""
                    SELECT COUNT(*), COALESCE(SUM(balance_remaining), 0)
                    FROM sales
                    WHERE hire_details IS NOT NULL 
                    AND balance_remaining > 0
                """)
                outstanding_count, outstanding_amount = cursor.fetchone()

                conn.close()

                return {
                    'sales_count': sales_count or 0,
                    'sales_total': sales_total or 0,
                    'hire_count': hire_count or 0,
                    'hire_total': hire_total or 0,
                    'outstanding_count': outstanding_count or 0,
                    'outstanding_amount': outstanding_amount or 0
                }
            except Exception as e:
                # Fail gracefully - return zeros if database error
                log_audit_event(f"Dashboard data load error: {str(e)}")
                return {
                    'sales_count': 0,
                    'sales_total': 0,
                    'hire_count': 0,
                    'hire_total': 0,
                    'outstanding_count': 0,
                    'outstanding_amount': 0
                }

        # Load data
        dashboard_data = load_dashboard_data()

        # Check if there's any data
        has_data = (dashboard_data['sales_count'] > 0 or
                   dashboard_data['hire_count'] > 0)

        if not has_data:
            # Show "No data yet" message
            no_data_frame = tk.Frame(dashboard_content, bg=MAIN_BG)
            no_data_frame.place(relx=0.5, rely=0.5, anchor='center')

            tk.Label(no_data_frame, text="📊",
                    font=('Segoe UI', 48), bg=MAIN_BG, fg='#cbd5e1').pack()
            tk.Label(no_data_frame, text="No Data Yet",
                    font=('Segoe UI', 20, 'bold'), bg=MAIN_BG, fg='#64748b').pack(pady=(10, 5))
            tk.Label(no_data_frame, text="Start making sales to see your business overview",
                    font=('Segoe UI', 11), bg=MAIN_BG, fg='#94a3b8').pack()
        else:
            # Create scrollable dashboard (in case of small screens)
            canvas = tk.Canvas(dashboard_content, bg=MAIN_BG, highlightthickness=0)
            scrollbar = tk.Scrollbar(dashboard_content, orient="vertical", command=canvas.yview)
            scrollable_frame = tk.Frame(canvas, bg=MAIN_BG)

            scrollable_frame.bind(
                "<Configure>",
                lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
            )

            canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)

            canvas.pack(side="left", fill="both", expand=True)
            scrollbar.pack(side="right", fill="y")

            # ─────────────────────────────────────────────────────────
            # Dashboard Title
            # ─────────────────────────────────────────────────────────
            title_frame = tk.Frame(scrollable_frame, bg=MAIN_BG)
            title_frame.pack(fill='x', pady=(0, 25))

            tk.Label(title_frame, text="Business Overview",
                    font=('Segoe UI', 22, 'bold'), bg=MAIN_BG, fg='#1e293b').pack(anchor='w')
            tk.Label(title_frame, text="Summary of all business activity",
                    font=('Segoe UI', 11), bg=MAIN_BG, fg='#64748b').pack(anchor='w')

            # ─────────────────────────────────────────────────────────
            # Summary Cards Row
            # ─────────────────────────────────────────────────────────
            cards_container = tk.Frame(scrollable_frame, bg=MAIN_BG)
            cards_container.pack(fill='x', pady=(0, 30))

            def create_metric_card(parent, title, value, subtitle, icon, color):
                """Create a modern metric card"""
                card = tk.Frame(parent, bg=CARD_BG, relief='flat', bd=0)
                card.pack(side='left', padx=8, fill='both', expand=True)

                # Add subtle shadow effect with border
                border_frame = tk.Frame(card, bg='#e2e8f0', bd=0)
                border_frame.pack(fill='both', expand=True, padx=1, pady=1)

                inner_card = tk.Frame(border_frame, bg=CARD_BG, bd=0)
                inner_card.pack(fill='both', expand=True, padx=8, pady=15)

                # Icon and title row
                header = tk.Frame(inner_card, bg=CARD_BG)
                header.pack(fill='x', padx=10, pady=(0, 8))

                tk.Label(header, text=icon, font=('Segoe UI Emoji', 20),
                        bg=CARD_BG, fg=color).pack(side='left', padx=(0, 10))
                tk.Label(header, text=title, font=('Segoe UI', 10, 'bold'),
                        bg=CARD_BG, fg='#64748b', anchor='w').pack(side='left', fill='x')

                # Value
                tk.Label(inner_card, text=value, font=('Segoe UI', 24, 'bold'),
                        bg=CARD_BG, fg=color, anchor='w').pack(fill='x', padx=10)

                # Subtitle
                tk.Label(inner_card, text=subtitle, font=('Segoe UI', 9),
                        bg=CARD_BG, fg='#94a3b8', anchor='w').pack(fill='x', padx=10, pady=(5, 0))

                return card

            # Create metric cards
            create_metric_card(cards_container, "Total Sales",
                             f"UGX {dashboard_data['sales_total']:,.0f}",
                             f"{dashboard_data['sales_count']} transactions",
                             "💰", "#6366f1")

            create_metric_card(cards_container, "Hire Revenue",
                             f"UGX {dashboard_data['hire_total']:,.0f}",
                             f"{dashboard_data['hire_count']} rentals",
                             "🛍️", "#a855f7")

            create_metric_card(cards_container, "Pending Returns",
                             f"{dashboard_data['outstanding_count']}",
                             f"UGX {dashboard_data['outstanding_amount']:,.0f} due",
                             "⏳", "#f59e0b")

            # ─────────────────────────────────────────────────────────
            # Visual Chart Section
            # ─────────────────────────────────────────────────────────
            chart_frame = tk.Frame(scrollable_frame, bg=CARD_BG, relief='flat', bd=0)
            chart_frame.pack(fill='both', expand=True, pady=(0, 20))

            # Border effect
            border = tk.Frame(chart_frame, bg='#e2e8f0', bd=0)
            border.pack(fill='both', expand=True, padx=1, pady=1)

            inner_chart = tk.Frame(border, bg=CARD_BG, bd=0)
            inner_chart.pack(fill='both', expand=True, padx=20, pady=20)

            # Chart title
            tk.Label(inner_chart, text="Revenue Breakdown",
                    font=('Segoe UI', 14, 'bold'), bg=CARD_BG, fg='#1e293b').pack(anchor='w', pady=(0, 15))

            # Canvas for bar chart
            chart_canvas = tk.Canvas(inner_chart, bg=CARD_BG, height=250, highlightthickness=0)
            chart_canvas.pack(fill='x', pady=(10, 0))

            def draw_bar_chart():
                """Draw a simple horizontal bar chart"""
                chart_canvas.delete('all')

                # Data for chart
                categories = [
                    ("Sales Revenue", dashboard_data['sales_total'], "#6366f1"),
                    ("Hire Revenue", dashboard_data['hire_total'], "#a855f7"),
                    ("Outstanding", dashboard_data['outstanding_amount'], "#f59e0b")
                ]

                # Calculate max value for scaling
                max_value = max(cat[1] for cat in categories)
                if max_value == 0:
                    max_value = 1  # Avoid division by zero

                # Chart dimensions
                chart_width = chart_canvas.winfo_width() - 200  # Leave space for labels
                chart_height = 200
                bar_height = 40
                bar_spacing = 20
                start_x = 180
                start_y = 20

                # Draw bars
                for i, (label, value, color) in enumerate(categories):
                    y_pos = start_y + (i * (bar_height + bar_spacing))

                    # Calculate bar width (proportional to value)
                    if max_value > 0:
                        bar_width = (value / max_value) * chart_width
                    else:
                        bar_width = 0

                    # Draw label
                    chart_canvas.create_text(10, y_pos + bar_height/2,
                                           text=label,
                                           font=('Segoe UI', 11, 'bold'),
                                           anchor='w',
                                           fill='#475569')

                    # Draw bar background (light)
                    chart_canvas.create_rectangle(start_x, y_pos,
                                                 start_x + chart_width, y_pos + bar_height,
                                                 fill='#f1f5f9',
                                                 outline='#e2e8f0',
                                                 width=1)

                    # Draw actual bar (colored)
                    if bar_width > 0:
                        chart_canvas.create_rectangle(start_x, y_pos,
                                                     start_x + bar_width, y_pos + bar_height,
                                                     fill=color,
                                                     outline=color,
                                                     width=0)

                    # Draw value text
                    chart_canvas.create_text(start_x + chart_width + 10, y_pos + bar_height/2,
                                           text=f"UGX {value:,.0f}",
                                           font=('Segoe UI', 10),
                                           anchor='w',
                                           fill='#64748b')

            # Draw chart after widget is visible
            chart_canvas.bind('<Configure>', lambda e: draw_bar_chart())
            root.after(100, draw_bar_chart)


    root.mainloop()


def show_popup_receipt(root, current_user, sale_id, tx_id, total, payment_method, amount_received, change, mobile_ref, cart_items=None):
    """Show a popup receipt after successful payment"""
    import business_settings

    # Load business settings for receipt
    receipt_settings = business_settings.get_receipt_settings()
    business_name = receipt_settings.get('business_name', 'Gorgeous Brides Boutique')
    business_tagline = receipt_settings.get('business_tagline', '')
    phone_primary = receipt_settings.get('phone_primary', '')
    email = receipt_settings.get('email', '')
    address = receipt_settings.get('address', '')
    tpin = receipt_settings.get('tpin', '')
    receipt_header = receipt_settings.get('receipt_header', 'Thank you for shopping with us!')
    receipt_footer = receipt_settings.get('receipt_footer', 'Please keep this receipt for your records.')
    currency_symbol = receipt_settings.get('currency_symbol', 'ZMW')
    show_tpin = receipt_settings.get('show_tpin_on_receipt', '1') == '1'
    show_address = receipt_settings.get('show_address_on_receipt', '1') == '1'

    receipt_window = tk.Toplevel(root)
    receipt_window.title("Receipt")
    receipt_window.geometry("420x700")
    receipt_window.configure(bg='#ffffff')
    receipt_window.transient(root)
    receipt_window.grab_set()
    receipt_window.geometry("+%d+%d" % (root.winfo_rootx() + 100, root.winfo_rooty() + 50))
    
    # Header with business name
    header_frame = tk.Frame(receipt_window, bg='#34495e', height=80 if business_tagline else 60)
    header_frame.pack(fill='x')
    header_frame.pack_propagate(False)
    
    tk.Label(header_frame, text=f"🏪 {business_name}",
            font=('Arial', 16, 'bold'), bg='#34495e', fg='white').pack(pady=(12, 0))

    if business_tagline:
        tk.Label(header_frame, text=business_tagline,
                font=('Arial', 9, 'italic'), bg='#34495e', fg='#bdc3c7').pack(pady=(2, 0))

    # Receipt content
    content_frame = tk.Frame(receipt_window, bg='#ffffff', padx=20, pady=15)
    content_frame.pack(fill='both', expand=True)
    
    # Business contact info section
    contact_frame = tk.Frame(content_frame, bg='#f8f9fa')
    contact_frame.pack(fill='x', pady=(0, 10))

    if show_address and address:
        tk.Label(contact_frame, text=f"📍 {address}",
                font=('Arial', 9), bg='#f8f9fa', fg='#6c757d', wraplength=350).pack(pady=2)
    if phone_primary:
        tk.Label(contact_frame, text=f"📞 {phone_primary}",
                font=('Arial', 9), bg='#f8f9fa', fg='#6c757d').pack(pady=2)
    if email:
        tk.Label(contact_frame, text=f"✉️ {email}",
                font=('Arial', 9), bg='#f8f9fa', fg='#6c757d').pack(pady=2)
    if show_tpin and tpin:
        tk.Label(contact_frame, text=f"TPIN: {tpin}",
                font=('Arial', 9, 'bold'), bg='#f8f9fa', fg='#495057').pack(pady=2)

    # Header message
    if receipt_header:
        tk.Label(content_frame, text=receipt_header,
                font=('Arial', 10, 'italic'), bg='#ffffff', fg='#27ae60', wraplength=350).pack(pady=(5, 10))

    # Transaction details
    tk.Label(content_frame, text="RECEIPT", font=('Arial', 18, 'bold'), 
            bg='#ffffff', fg='#2c3e50').pack(pady=(0, 10))
    
    details_frame = tk.Frame(content_frame, bg='#ffffff')
    details_frame.pack(fill='x', pady=10)
    
    # Transaction info
    tk.Label(details_frame, text=f"Transaction ID: {tx_id}", 
            font=('Arial', 12, 'bold'), bg='#ffffff', anchor='w').pack(fill='x')
    tk.Label(details_frame, text=f"Cashier: {current_user['username']}", 
            font=('Arial', 10), bg='#ffffff', anchor='w').pack(fill='x')
    
    from datetime import datetime
    tk.Label(details_frame, text=f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
            font=('Arial', 10), bg='#ffffff', anchor='w').pack(fill='x')
    
    # Separator
    tk.Frame(content_frame, bg='#bdc3c7', height=1).pack(fill='x', pady=10)
    
    # Items
    tk.Label(content_frame, text="ITEMS PURCHASED", font=('Arial', 12, 'bold'), 
            bg='#ffffff', fg='#2c3e50').pack(anchor='w')
    
    items_frame = tk.Frame(content_frame, bg='#ffffff')
    items_frame.pack(fill='x', pady=5)
    
    # Get sale items
    try:
        from sales_utils import get_sale_items
        items = get_sale_items(sale_id)
        
        if items:
            for item_name, qty, unit_price, subtotal in items:
                item_frame = tk.Frame(items_frame, bg='#ffffff')
                item_frame.pack(fill='x', pady=2)
                
                tk.Label(item_frame, text=f"{item_name} x{qty}", 
                        font=('Arial', 10), bg='#ffffff', anchor='w').pack(side='left')
                tk.Label(item_frame, text=f"{currency_symbol} {subtotal:.2f}",
                        font=('Arial', 10), bg='#ffffff', anchor='e').pack(side='right')
        elif cart_items:
            # Fallback: display items from cart if database retrieval failed
            for item in cart_items:
                item_frame = tk.Frame(items_frame, bg='#ffffff')
                item_frame.pack(fill='x', pady=2)
                
                subtotal = float(item['quantity']) * float(item['unit_price'])
                tk.Label(item_frame, text=f"{item['item']} x{item['quantity']}", 
                        font=('Arial', 10), bg='#ffffff', anchor='w').pack(side='left')
                tk.Label(item_frame, text=f"{currency_symbol} {subtotal:.2f}",
                        font=('Arial', 10), bg='#ffffff', anchor='e').pack(side='right')
        else:
            # Final fallback
            tk.Label(items_frame, text="Transaction completed successfully", 
                    font=('Arial', 10), bg='#ffffff', fg='#27ae60').pack()
    except Exception as e:
        tk.Label(items_frame, text=f"Error loading items: {str(e)}", 
                font=('Arial', 10), bg='#ffffff', fg='red').pack()
    
    # Separator
    tk.Frame(content_frame, bg='#bdc3c7', height=1).pack(fill='x', pady=10)
    
    # Totals
    totals_frame = tk.Frame(content_frame, bg='#ffffff')
    totals_frame.pack(fill='x')
    
    tk.Label(totals_frame, text=f"TOTAL: {currency_symbol} {total:.2f}",
            font=('Arial', 14, 'bold'), bg='#ffffff', fg='#27ae60').pack(anchor='e')
    
    # Payment details
    payment_frame = tk.Frame(content_frame, bg='#ffffff')
    payment_frame.pack(fill='x', pady=10)
    
    tk.Label(payment_frame, text=f"Payment Method: {payment_method}", 
            font=('Arial', 11, 'bold'), bg='#ffffff', anchor='w').pack(fill='x')
    
    if payment_method == "Cash":
        tk.Label(payment_frame, text=f"Cash Received: {currency_symbol} {amount_received:.2f}",
                font=('Arial', 10), bg='#ffffff', anchor='w').pack(fill='x')
        tk.Label(payment_frame, text=f"Change: {currency_symbol} {change:.2f}",
                font=('Arial', 10, 'bold'), bg='#ffffff', fg='#e74c3c', anchor='w').pack(fill='x')
    elif payment_method == "Mobile Money" and mobile_ref:
        tk.Label(payment_frame, text=f"Reference: {mobile_ref}", 
                font=('Arial', 10), bg='#ffffff', anchor='w').pack(fill='x')
    
    # Footer with configurable message
    footer_frame = tk.Frame(content_frame, bg='#ffffff')
    footer_frame.pack(fill='x', pady=20)
    tk.Label(footer_frame, text=receipt_footer if receipt_footer else "Thank you for your business!",
            font=('Arial', 12, 'italic'), bg='#ffffff', fg='#7f8c8d', wraplength=350).pack()

    # Buttons
    button_frame = tk.Frame(content_frame, bg='#ffffff')
    button_frame.pack(fill='x', pady=10)

    # Auto-close timer management
    timer = {'id': None}
    def on_close():
        try:
            if timer['id']:
                root.after_cancel(timer['id'])
        except Exception:
            pass
        try:
            if receipt_window and receipt_window.winfo_exists():
                receipt_window.destroy()
        except Exception:
            pass
    
    def print_receipt():
        try:
            from sales_utils import generate_pdf_receipt_for_sale, print_sales_receipt_thermal

            # Generate PDF receipt
            pdf_path = generate_pdf_receipt_for_sale(sale_id)

            # Auto-print to thermal printer
            thermal_success = print_sales_receipt_thermal(sale_id)

            if thermal_success:
                messagebox.showinfo("Receipt Printed",
                    f"Receipt sent to thermal printer!\n\nPDF also saved to:\n{pdf_path}")
            else:
                messagebox.showinfo("Receipt Saved",
                    f"PDF receipt saved to:\n{pdf_path}\n\n(Thermal printer not available)")
        except Exception as e:
            messagebox.showerror("Print Error", f"Error generating receipt: {str(e)}")
    
    tk.Button(button_frame, text="📄 Print Receipt", command=print_receipt,
             bg='#3498db', fg='white', font=('Arial', 11, 'bold'), padx=20).pack(side='left', padx=5)
    tk.Button(button_frame, text="✅ Close", command=on_close,
             bg='#27ae60', fg='white', font=('Arial', 11, 'bold'), padx=20).pack(side='right', padx=5)
    
    # Auto-close after 30 seconds (safe)
    def _safe_destroy(win):
        try:
            if win and win.winfo_exists():
                win.destroy()
        except Exception:
            pass
    # Use a proper function reference instead of lambda to avoid invalid command name errors
    def auto_close_receipt():
        _safe_destroy(receipt_window)
    timer['id'] = root.after(30000, auto_close_receipt)
    try:
        receipt_window.protocol("WM_DELETE_WINDOW", on_close)
    except Exception:
        pass


# ========================================
# ADMIN: BUSINESS SETTINGS FUNCTION
# ========================================

def show_business_settings(root_window):
    """
    Business Settings Panel - Manage business information system-wide.
    Includes business identity, contact info, tax details, and receipt customization.
    """
    from tkinter import Toplevel, filedialog
    import business_settings

    win = Toplevel(root_window)
    win.title("⚙️ Business Settings")
    win.geometry("900x750")
    win.configure(bg="#f8fafc")
    win.transient(root_window)
    win.grab_set()

    # Center the window
    win.update_idletasks()
    x = (win.winfo_screenwidth() // 2) - (900 // 2)
    y = (win.winfo_screenheight() // 2) - (750 // 2)
    win.geometry(f"+{x}+{y}")

    # Colors
    HEADER_BG = "#1e293b"
    SECTION_BG = "#ffffff"
    LABEL_FG = "#374151"
    INPUT_BG = "#f9fafb"
    ACCENT = "#3b82f6"
    SUCCESS = "#10b981"
    ERROR = "#ef4444"

    # Header
    header = tk.Frame(win, bg=HEADER_BG, height=70)
    header.pack(fill='x')
    header.pack_propagate(False)

    header_inner = tk.Frame(header, bg=HEADER_BG)
    header_inner.pack(fill='both', expand=True, padx=20, pady=15)

    tk.Label(header_inner, text="⚙️ Business Settings",
             font=('Segoe UI', 18, 'bold'), bg=HEADER_BG, fg='white').pack(side='left')

    tk.Label(header_inner, text="Configure your business information",
             font=('Segoe UI', 10), bg=HEADER_BG, fg='#94a3b8').pack(side='left', padx=(15, 0))

    # Status label in header
    status_label = tk.Label(header_inner, text="", font=('Segoe UI', 10, 'bold'),
                           bg=HEADER_BG, fg=SUCCESS)
    status_label.pack(side='right')

    # ═══════════════════════════════════════════════════════════════
    # BUTTON BAR (Pack first with side='bottom' to reserve space)
    # ═══════════════════════════════════════════════════════════════
    button_frame = tk.Frame(win, bg="#f1f5f9", height=80)
    button_frame.pack(fill='x', side='bottom')
    button_frame.pack_propagate(False)

    button_inner = tk.Frame(button_frame, bg="#f1f5f9")
    button_inner.pack(expand=True, pady=20)

    # Main scrollable area
    main_canvas = tk.Canvas(win, bg="#f8fafc", highlightthickness=0)
    scrollbar = ttk.Scrollbar(win, orient="vertical", command=main_canvas.yview)
    scrollable_frame = tk.Frame(main_canvas, bg="#f8fafc")

    scrollable_frame.bind(
        "<Configure>",
        lambda e: main_canvas.configure(scrollregion=main_canvas.bbox("all"))
    )

    main_canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
    main_canvas.configure(yscrollcommand=scrollbar.set)

    # Mouse wheel scrolling
    def _on_mousewheel(event):
        main_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
    main_canvas.bind_all("<MouseWheel>", _on_mousewheel)

    main_canvas.pack(side="left", fill="both", expand=True, padx=20, pady=20)
    scrollbar.pack(side="right", fill="y")

    # Load current settings
    current_settings = business_settings.get_all_settings()

    # Dictionary to hold all entry variables
    entry_vars = {}

    def create_section(parent, title, icon):
        """Create a styled section frame"""
        section = tk.LabelFrame(parent, text=f"  {icon} {title}  ",
                               font=('Segoe UI', 12, 'bold'),
                               bg=SECTION_BG, fg=LABEL_FG,
                               padx=20, pady=15)
        section.pack(fill='x', pady=(0, 15))
        return section

    def create_field(parent, label, key, default='', width=50, required=False):
        """Create a labeled input field"""
        frame = tk.Frame(parent, bg=SECTION_BG)
        frame.pack(fill='x', pady=5)

        label_text = f"{label} {'*' if required else ''}"
        tk.Label(frame, text=label_text, font=('Segoe UI', 10),
                bg=SECTION_BG, fg=LABEL_FG, width=22, anchor='w').pack(side='left')

        var = tk.StringVar(value=current_settings.get(key, default))
        entry = tk.Entry(frame, textvariable=var, font=('Segoe UI', 10),
                        bg=INPUT_BG, fg='#1f2937', relief='flat',
                        width=width, highlightthickness=1,
                        highlightbackground='#e5e7eb', highlightcolor=ACCENT)
        entry.pack(side='left', fill='x', expand=True, ipady=6)

        entry_vars[key] = var
        return entry

    def create_text_field(parent, label, key, default='', height=3):
        """Create a multiline text field"""
        frame = tk.Frame(parent, bg=SECTION_BG)
        frame.pack(fill='x', pady=5)

        tk.Label(frame, text=label, font=('Segoe UI', 10),
                bg=SECTION_BG, fg=LABEL_FG, width=22, anchor='nw').pack(side='left', anchor='n')

        text_frame = tk.Frame(frame, bg=INPUT_BG, highlightthickness=1,
                             highlightbackground='#e5e7eb', highlightcolor=ACCENT)
        text_frame.pack(side='left', fill='x', expand=True)

        text = tk.Text(text_frame, font=('Segoe UI', 10), bg=INPUT_BG, fg='#1f2937',
                      relief='flat', height=height, wrap='word')
        text.pack(fill='x', padx=2, pady=2)
        text.insert('1.0', current_settings.get(key, default))

        # Store reference to text widget
        entry_vars[key] = text
        return text

    def create_checkbox(parent, label, key, default='0'):
        """Create a checkbox field"""
        frame = tk.Frame(parent, bg=SECTION_BG)
        frame.pack(fill='x', pady=5)

        var = tk.BooleanVar(value=current_settings.get(key, default) == '1')
        cb = tk.Checkbutton(frame, text=label, variable=var,
                           font=('Segoe UI', 10), bg=SECTION_BG, fg=LABEL_FG,
                           activebackground=SECTION_BG, selectcolor=INPUT_BG)
        cb.pack(side='left', padx=(0, 10))

        entry_vars[key] = var
        return cb

    # ═══════════════════════════════════════════════════════════════
    # SECTION 1: Business Identity
    # ═══════════════════════════════════════════════════════════════
    section1 = create_section(scrollable_frame, "Business Identity", "🏢")

    create_field(section1, "Business Name", 'business_name', 'Gorgeous Brides Boutique', required=True)
    create_field(section1, "Tagline/Slogan", 'business_tagline', 'Making Your Special Day Perfect')

    # Logo upload field
    logo_frame = tk.Frame(section1, bg=SECTION_BG)
    logo_frame.pack(fill='x', pady=5)

    tk.Label(logo_frame, text="Business Logo", font=('Segoe UI', 10),
            bg=SECTION_BG, fg=LABEL_FG, width=22, anchor='w').pack(side='left')

    logo_var = tk.StringVar(value=current_settings.get('business_logo_path', ''))
    entry_vars['business_logo_path'] = logo_var

    logo_entry = tk.Entry(logo_frame, textvariable=logo_var, font=('Segoe UI', 10),
                         bg=INPUT_BG, fg='#1f2937', relief='flat', width=35,
                         highlightthickness=1, highlightbackground='#e5e7eb')
    logo_entry.pack(side='left', fill='x', expand=True, ipady=6)

    def browse_logo():
        filepath = filedialog.askopenfilename(
            title="Select Logo Image",
            filetypes=[("Image files", "*.png *.jpg *.jpeg *.gif *.bmp"), ("All files", "*.*")]
        )
        if filepath:
            logo_var.set(filepath)

    tk.Button(logo_frame, text="📁 Browse", command=browse_logo,
             font=('Segoe UI', 9), bg=ACCENT, fg='white',
             relief='flat', padx=10, cursor='hand2').pack(side='left', padx=(5, 0))

    # ═══════════════════════════════════════════════════════════════
    # SECTION 2: Contact Information
    # ═══════════════════════════════════════════════════════════════
    section2 = create_section(scrollable_frame, "Contact Information", "📞")

    create_field(section2, "Primary Phone", 'phone_primary')
    create_field(section2, "Secondary Phone", 'phone_secondary')
    create_field(section2, "Email Address", 'email')
    create_field(section2, "Website URL", 'website')
    create_text_field(section2, "Physical Address", 'address', height=2)

    # ═══════════════════════════════════════════════════════════════
    # SECTION 3: Tax/Legal Information
    # ═══════════════════════════════════════════════════════════════
    section3 = create_section(scrollable_frame, "Tax & Legal Information", "📋")

    create_field(section3, "TPIN", 'tpin')
    create_field(section3, "Business Registration #", 'business_registration')
    create_field(section3, "VAT Number", 'vat_number')
    create_field(section3, "Currency Symbol", 'currency_symbol', 'ZMW', width=10)

    # ═══════════════════════════════════════════════════════════════
    # SECTION 4: Receipt Customization
    # ═══════════════════════════════════════════════════════════════
    section4 = create_section(scrollable_frame, "Receipt Customization", "🧾")

    create_text_field(section4, "Receipt Header", 'receipt_header',
                     'Thank you for shopping with us!', height=2)
    create_text_field(section4, "Receipt Footer", 'receipt_footer',
                     'Please keep this receipt for your records.', height=2)
    create_text_field(section4, "Return Policy", 'return_policy',
                     'Returns accepted within 7 days with original receipt.', height=2)

    # Display options
    options_frame = tk.Frame(section4, bg=SECTION_BG)
    options_frame.pack(fill='x', pady=(10, 5))

    tk.Label(options_frame, text="Display Options:", font=('Segoe UI', 10, 'bold'),
            bg=SECTION_BG, fg=LABEL_FG).pack(anchor='w', pady=(0, 5))

    create_checkbox(section4, "Show TPIN on receipts", 'show_tpin_on_receipt', '1')
    create_checkbox(section4, "Show address on receipts", 'show_address_on_receipt', '1')
    create_checkbox(section4, "Show social media on receipts", 'show_social_on_receipt', '0')

    # ═══════════════════════════════════════════════════════════════
    # SECTION 5: Social Media
    # ═══════════════════════════════════════════════════════════════
    section5 = create_section(scrollable_frame, "Social Media", "📱")

    create_field(section5, "Facebook", 'social_facebook')
    create_field(section5, "Instagram", 'social_instagram')
    create_field(section5, "Twitter/X", 'social_twitter')
    create_field(section5, "WhatsApp", 'social_whatsapp')

    # ═══════════════════════════════════════════════════════════════
    # BUTTON FUNCTIONS AND CREATION
    # ═══════════════════════════════════════════════════════════════

    def save_settings():
        """Collect and save all settings"""
        settings_to_save = {}

        for key, var in entry_vars.items():
            if isinstance(var, tk.Text):
                # Get text from Text widget
                settings_to_save[key] = var.get('1.0', 'end-1c').strip()
            elif isinstance(var, tk.BooleanVar):
                # Convert boolean to string
                settings_to_save[key] = '1' if var.get() else '0'
            else:
                # StringVar
                settings_to_save[key] = var.get().strip()

        # Save settings
        success, message = business_settings.save_all_settings(settings_to_save)

        if success:
            status_label.config(text="✅ " + message, fg=SUCCESS)
            messagebox.showinfo("Settings Saved", message)
        else:
            status_label.config(text="❌ " + message, fg=ERROR)
            messagebox.showerror("Error", message)

    def preview_receipt():
        """Show a preview of how the receipt will look"""
        preview_win = Toplevel(win)
        preview_win.title("Receipt Preview")
        preview_win.geometry("400x600")
        preview_win.configure(bg='#ffffff')
        preview_win.transient(win)

        # Get current values from form
        biz_name = entry_vars.get('business_name', tk.StringVar()).get() if isinstance(entry_vars.get('business_name'), tk.StringVar) else 'Business Name'
        tagline = entry_vars.get('business_tagline', tk.StringVar()).get() if isinstance(entry_vars.get('business_tagline'), tk.StringVar) else ''
        phone = entry_vars.get('phone_primary', tk.StringVar()).get() if isinstance(entry_vars.get('phone_primary'), tk.StringVar) else ''
        email = entry_vars.get('email', tk.StringVar()).get() if isinstance(entry_vars.get('email'), tk.StringVar) else ''
        address = entry_vars.get('address').get('1.0', 'end-1c') if isinstance(entry_vars.get('address'), tk.Text) else ''
        tpin = entry_vars.get('tpin', tk.StringVar()).get() if isinstance(entry_vars.get('tpin'), tk.StringVar) else ''
        header_text = entry_vars.get('receipt_header').get('1.0', 'end-1c') if isinstance(entry_vars.get('receipt_header'), tk.Text) else ''
        footer_text = entry_vars.get('receipt_footer').get('1.0', 'end-1c') if isinstance(entry_vars.get('receipt_footer'), tk.Text) else ''
        show_tpin = entry_vars.get('show_tpin_on_receipt', tk.BooleanVar()).get() if isinstance(entry_vars.get('show_tpin_on_receipt'), tk.BooleanVar) else True
        show_addr = entry_vars.get('show_address_on_receipt', tk.BooleanVar()).get() if isinstance(entry_vars.get('show_address_on_receipt'), tk.BooleanVar) else True
        currency = entry_vars.get('currency_symbol', tk.StringVar()).get() if isinstance(entry_vars.get('currency_symbol'), tk.StringVar) else 'ZMW'

        # Header
        header_frame = tk.Frame(preview_win, bg='#34495e', height=80)
        header_frame.pack(fill='x')
        header_frame.pack_propagate(False)

        tk.Label(header_frame, text=biz_name,
                font=('Arial', 16, 'bold'), bg='#34495e', fg='white').pack(pady=(15, 0))
        if tagline:
            tk.Label(header_frame, text=tagline,
                    font=('Arial', 9), bg='#34495e', fg='#bdc3c7').pack()

        # Content
        content = tk.Frame(preview_win, bg='#ffffff', padx=20, pady=15)
        content.pack(fill='both', expand=True)

        # Contact info
        if show_addr and address:
            tk.Label(content, text=address, font=('Arial', 9),
                    bg='#ffffff', fg='#7f8c8d', wraplength=350).pack()
        if phone:
            tk.Label(content, text=f"📞 {phone}", font=('Arial', 9),
                    bg='#ffffff', fg='#7f8c8d').pack()
        if email:
            tk.Label(content, text=f"✉️ {email}", font=('Arial', 9),
                    bg='#ffffff', fg='#7f8c8d').pack()
        if show_tpin and tpin:
            tk.Label(content, text=f"TPIN: {tpin}", font=('Arial', 9, 'bold'),
                    bg='#ffffff', fg='#2c3e50').pack(pady=(5, 0))

        tk.Frame(content, bg='#bdc3c7', height=1).pack(fill='x', pady=10)

        # Header message
        if header_text:
            tk.Label(content, text=header_text, font=('Arial', 10, 'italic'),
                    bg='#ffffff', fg='#27ae60', wraplength=350).pack(pady=5)

        tk.Label(content, text="RECEIPT", font=('Arial', 14, 'bold'),
                bg='#ffffff', fg='#2c3e50').pack(pady=10)

        # Sample items
        tk.Label(content, text="─" * 40, font=('Arial', 8), bg='#ffffff', fg='#bdc3c7').pack()

        sample_items = [
            ("Sample Item 1", 2, 150.00),
            ("Sample Item 2", 1, 250.00),
        ]

        for item, qty, price in sample_items:
            item_frame = tk.Frame(content, bg='#ffffff')
            item_frame.pack(fill='x', pady=2)
            tk.Label(item_frame, text=f"{item} x{qty}", font=('Arial', 10),
                    bg='#ffffff', anchor='w').pack(side='left')
            tk.Label(item_frame, text=f"{currency} {price:.2f}", font=('Arial', 10),
                    bg='#ffffff', anchor='e').pack(side='right')

        tk.Label(content, text="─" * 40, font=('Arial', 8), bg='#ffffff', fg='#bdc3c7').pack()

        # Total
        total_frame = tk.Frame(content, bg='#ffffff')
        total_frame.pack(fill='x', pady=10)
        tk.Label(total_frame, text="TOTAL:", font=('Arial', 12, 'bold'),
                bg='#ffffff', anchor='w').pack(side='left')
        tk.Label(total_frame, text=f"{currency} 550.00", font=('Arial', 12, 'bold'),
                bg='#ffffff', fg='#27ae60', anchor='e').pack(side='right')

        tk.Frame(content, bg='#bdc3c7', height=1).pack(fill='x', pady=10)

        # Footer
        if footer_text:
            tk.Label(content, text=footer_text, font=('Arial', 9),
                    bg='#ffffff', fg='#7f8c8d', wraplength=350).pack(pady=5)

        # Close button
        tk.Button(preview_win, text="Close Preview", command=preview_win.destroy,
                 font=('Segoe UI', 10), bg='#95a5a6', fg='white',
                 relief='flat', padx=20, pady=8, cursor='hand2').pack(pady=15)

    def reset_defaults():
        """Reset all settings to defaults"""
        if messagebox.askyesno("Reset Settings",
                              "Are you sure you want to reset all settings to defaults?\n\n"
                              "This cannot be undone."):
            # Clear the table and reinitialize
            conn = business_settings.get_db_connection()
            cur = conn.cursor()
            try:
                cur.execute("DELETE FROM business_settings")
                conn.commit()
                business_settings._set_default_settings(cur, conn)
            finally:
                conn.close()

            # Reload the window
            win.destroy()
            show_business_settings(root_window)

    # Buttons
    tk.Button(button_inner, text="👁️ Preview Receipt", command=preview_receipt,
             font=('Segoe UI', 11), bg='#6366f1', fg='white',
             relief='flat', padx=20, pady=10, cursor='hand2').pack(side='left', padx=5)

    tk.Button(button_inner, text="🔄 Reset to Defaults", command=reset_defaults,
             font=('Segoe UI', 11), bg='#f59e0b', fg='white',
             relief='flat', padx=20, pady=10, cursor='hand2').pack(side='left', padx=5)

    tk.Button(button_inner, text="💾 Save Settings", command=save_settings,
             font=('Segoe UI', 11, 'bold'), bg=SUCCESS, fg='white',
             relief='flat', padx=30, pady=10, cursor='hand2').pack(side='left', padx=5)

    tk.Button(button_inner, text="❌ Close", command=win.destroy,
             font=('Segoe UI', 11), bg='#64748b', fg='white',
             relief='flat', padx=20, pady=10, cursor='hand2').pack(side='left', padx=5)

    # Unbind mousewheel when window closes
    def on_close():
        main_canvas.unbind_all("<MouseWheel>")
        win.destroy()

    win.protocol("WM_DELETE_WINDOW", on_close)


# ========================================
# ADMIN: MONTHLY SALES REPORT FUNCTION
# ========================================

def show_monthly_sales_report(root_window):
    """
    Admin-only monthly sales report with comprehensive analytics.
    Prevents redundant data aggregation by showing complete monthly summaries.
    """
    from tkinter import Toplevel
    from datetime import datetime as dt
    import reporting_system

    win = Toplevel(root_window)
    win.title("📊 Monthly Sales Report (Admin Only)")
    win.geometry("1400x850")
    win.configure(bg="#f4f7fb")

    # Header
    header = tk.Frame(win, bg="#2c3e50")
    header.pack(fill='x')
    tk.Label(header, text="📊 Monthly Sales Summary & Analytics", fg='white', bg="#2c3e50",
             font=('Segoe UI', 18, 'bold'), padx=20, pady=18).pack(side='left')

    # Month/Year Selector
    selector_frame = tk.LabelFrame(win, text="📅 Select Month & Year",
                                   bg='#ffffff', font=('Segoe UI', 12, 'bold'),
                                   padx=20, pady=20)
    selector_frame.pack(fill='x', padx=20, pady=(15, 10))

    # Instructions
    tk.Label(selector_frame,
            text="Select a month to view complete sales analytics. All days in the month are automatically included.",
            bg='#ffffff', fg='#2c3e50', font=('Segoe UI', 10)).pack(pady=(0, 15))

    # Month and year selection
    select_row = tk.Frame(selector_frame, bg='#ffffff')
    select_row.pack()

    tk.Label(select_row, text="Month:", bg='#ffffff',
            font=('Segoe UI', 11, 'bold')).pack(side='left', padx=(0, 10))

    months = ['January', 'February', 'March', 'April', 'May', 'June',
              'July', 'August', 'September', 'October', 'November', 'December']

    current_month = dt.now().month
    current_year = dt.now().year

    month_var = tk.StringVar(value=months[current_month - 1])
    month_combo = ttk.Combobox(select_row, textvariable=month_var, values=months,
                              state='readonly', font=('Segoe UI', 11), width=12)
    month_combo.pack(side='left', padx=5)

    tk.Label(select_row, text="Year:", bg='#ffffff',
            font=('Segoe UI', 11, 'bold')).pack(side='left', padx=(20, 10))

    # Year range: last 5 years to current year
    years = list(range(current_year - 4, current_year + 1))
    year_var = tk.StringVar(value=str(current_year))
    year_combo = ttk.Combobox(select_row, textvariable=year_var, values=years,
                             state='readonly', font=('Segoe UI', 11), width=8)
    year_combo.pack(side='left', padx=5)

    # Display containers
    overview_frame = tk.Frame(win, bg='#f4f7fb')
    overview_frame.pack(fill='x', padx=20, pady=(0, 10))

    details_frame = tk.Frame(win, bg='#f4f7fb')
    details_frame.pack(fill='both', expand=True, padx=20, pady=(0, 10))

    status_label = tk.Label(win, text="", bg='#ecf0f1', fg='#7f8c8d',
                           font=('Segoe UI', 10), padx=10, pady=8, anchor='w')
    status_label.pack(fill='x', side='bottom')

    def load_monthly_report():
        """Load and display monthly sales report"""
        selected_month_name = month_var.get()
        selected_month_num = months.index(selected_month_name) + 1
        selected_year = int(year_var.get())

        # Clear previous data
        for widget in overview_frame.winfo_children():
            widget.destroy()
        for widget in details_frame.winfo_children():
            widget.destroy()

        try:
            # Get monthly summary
            summary = reporting_system.get_monthly_sales_summary(selected_year, selected_month_num)

            # ===== OVERVIEW CARDS =====
            cards_container = tk.Frame(overview_frame, bg='#1e293b', padx=15, pady=15)
            cards_container.pack(fill='x')

            tk.Label(cards_container, text=f"MONTHLY OVERVIEW - {summary['month_name']}",
                    bg='#1e293b', fg='#ffffff', font=('Segoe UI', 13, 'bold')).pack(anchor='w', pady=(0, 10))

            cards_row = tk.Frame(cards_container, bg='#1e293b')
            cards_row.pack(fill='x')

            overview_data = summary['overview']

            # Card 1: Total Revenue
            card1 = tk.Frame(cards_row, bg='#334155', padx=18, pady=14)
            card1.pack(side='left', fill='x', expand=True, padx=(0, 8))
            tk.Label(card1, text="💰 Total Revenue", font=('Segoe UI', 11),
                    fg='#94a3b8', bg='#334155').pack(anchor='w')
            tk.Label(card1, text=f"ZMW {overview_data['total_revenue']:.2f}",
                    font=('Segoe UI', 16, 'bold'), fg='#10b981', bg='#334155').pack(anchor='w')

            # Card 2: Total Transactions
            card2 = tk.Frame(cards_row, bg='#334155', padx=18, pady=14)
            card2.pack(side='left', fill='x', expand=True, padx=(4, 8))
            tk.Label(card2, text="🧾 Transactions", font=('Segoe UI', 11),
                    fg='#94a3b8', bg='#334155').pack(anchor='w')
            tk.Label(card2, text=f"{overview_data['total_transactions']}",
                    font=('Segoe UI', 16, 'bold'), fg='#3b82f6', bg='#334155').pack(anchor='w')

            # Card 3: Average Daily Sales
            card3 = tk.Frame(cards_row, bg='#334155', padx=18, pady=14)
            card3.pack(side='left', fill='x', expand=True, padx=(4, 8))
            tk.Label(card3, text="📊 Avg Daily Sales", font=('Segoe UI', 11),
                    fg='#94a3b8', bg='#334155').pack(anchor='w')
            tk.Label(card3, text=f"ZMW {overview_data['avg_daily_sales']:.2f}",
                    font=('Segoe UI', 16, 'bold'), fg='#8b5cf6', bg='#334155').pack(anchor='w')

            # Card 4: Best Day
            card4 = tk.Frame(cards_row, bg='#334155', padx=18, pady=14)
            card4.pack(side='left', fill='x', expand=True, padx=(4, 8))
            tk.Label(card4, text="🔥 Best Day", font=('Segoe UI', 11),
                    fg='#94a3b8', bg='#334155').pack(anchor='w')
            tk.Label(card4, text=f"{overview_data['best_day']['date']}",
                    font=('Segoe UI', 12, 'bold'), fg='#f59e0b', bg='#334155').pack(anchor='w')
            tk.Label(card4, text=f"ZMW {overview_data['best_day']['amount']:.2f}",
                    font=('Segoe UI', 10), fg='#fbbf24', bg='#334155').pack(anchor='w')

            # Card 5: Worst Day
            card5 = tk.Frame(cards_row, bg='#334155', padx=18, pady=14)
            card5.pack(side='left', fill='x', expand=True, padx=(4, 0))
            tk.Label(card5, text="📉 Lowest Day", font=('Segoe UI', 11),
                    fg='#94a3b8', bg='#334155').pack(anchor='w')
            tk.Label(card5, text=f"{overview_data['worst_day']['date']}",
                    font=('Segoe UI', 12, 'bold'), fg='#ef4444', bg='#334155').pack(anchor='w')
            tk.Label(card5, text=f"ZMW {overview_data['worst_day']['amount']:.2f}",
                    font=('Segoe UI', 10), fg='#f87171', bg='#334155').pack(anchor='w')

            # ===== DETAILED SECTIONS =====
            # Create notebook for organized sections
            notebook = ttk.Notebook(details_frame)
            notebook.pack(fill='both', expand=True)

            # Tab 1: Daily Breakdown
            daily_tab = tk.Frame(notebook, bg='#ffffff')
            notebook.add(daily_tab, text='📅 Daily Breakdown')

            tk.Label(daily_tab, text="Daily Sales Breakdown", bg='#ffffff',
                    font=('Segoe UI', 12, 'bold'), fg='#2c3e50').pack(pady=10)

            # Treeview for daily breakdown
            daily_tree_container = tk.Frame(daily_tab, bg='#ffffff')
            daily_tree_container.pack(fill='both', expand=True, padx=10, pady=(0, 10))

            daily_cols = ('date', 'transactions', 'total')
            daily_tree = ttk.Treeview(daily_tree_container, columns=daily_cols,
                                     show='headings', height=18)

            daily_tree.heading('date', text='Date')
            daily_tree.heading('transactions', text='Transactions')
            daily_tree.heading('total', text='Daily Total (ZMW)')

            daily_tree.column('date', width=200, anchor='center')
            daily_tree.column('transactions', width=150, anchor='center')
            daily_tree.column('total', width=200, anchor='e')

            scrollbar_daily = ttk.Scrollbar(daily_tree_container, orient='vertical',
                                           command=daily_tree.yview)
            daily_tree.configure(yscrollcommand=scrollbar_daily.set)

            daily_tree.pack(side='left', fill='both', expand=True)
            scrollbar_daily.pack(side='right', fill='y')

            for idx, day_data in enumerate(summary['daily_breakdown']):
                tag = 'odd' if (idx % 2) else 'even'
                daily_tree.insert('', 'end', values=(
                    day_data['date'],
                    day_data['transaction_count'],
                    f"ZMW {day_data['daily_total']:.2f}"
                ), tags=(tag,))

            daily_tree.tag_configure('odd', background='#f9fafb')
            daily_tree.tag_configure('even', background='#ffffff')

            # Tab 2: Payment Summary
            payment_tab = tk.Frame(notebook, bg='#ffffff')
            notebook.add(payment_tab, text='💳 Payment Methods')

            tk.Label(payment_tab, text="Payment Method Analysis", bg='#ffffff',
                    font=('Segoe UI', 12, 'bold'), fg='#2c3e50').pack(pady=10)

            payment_tree_container = tk.Frame(payment_tab, bg='#ffffff')
            payment_tree_container.pack(fill='both', expand=True, padx=10, pady=(0, 10))

            payment_cols = ('method', 'amount', 'percentage')
            payment_tree = ttk.Treeview(payment_tree_container, columns=payment_cols,
                                       show='headings', height=10)

            payment_tree.heading('method', text='Payment Method')
            payment_tree.heading('amount', text='Total Amount')
            payment_tree.heading('percentage', text='Percentage')

            payment_tree.column('method', width=250, anchor='w')
            payment_tree.column('amount', width=200, anchor='e')
            payment_tree.column('percentage', width=150, anchor='center')

            payment_tree.pack(fill='both', expand=True)

            for method, data in summary['payment_summary'].items():
                payment_tree.insert('', 'end', values=(
                    method,
                    f"ZMW {data['amount']:.2f}",
                    f"{data['percentage']:.1f}%"
                ))

            # Tab 3: Top Selling Items
            items_tab = tk.Frame(notebook, bg='#ffffff')
            notebook.add(items_tab, text='🏆 Top Selling Items')

            tk.Label(items_tab, text="Top 20 Best Selling Items", bg='#ffffff',
                    font=('Segoe UI', 12, 'bold'), fg='#2c3e50').pack(pady=10)

            items_tree_container = tk.Frame(items_tab, bg='#ffffff')
            items_tree_container.pack(fill='both', expand=True, padx=10, pady=(0, 10))

            items_cols = ('rank', 'item', 'quantity', 'revenue')
            items_tree = ttk.Treeview(items_tree_container, columns=items_cols,
                                     show='headings', height=18)

            items_tree.heading('rank', text='Rank')
            items_tree.heading('item', text='Item Name')
            items_tree.heading('quantity', text='Quantity Sold')
            items_tree.heading('revenue', text='Revenue')

            items_tree.column('rank', width=80, anchor='center')
            items_tree.column('item', width=350, anchor='w')
            items_tree.column('quantity', width=150, anchor='center')
            items_tree.column('revenue', width=200, anchor='e')

            scrollbar_items = ttk.Scrollbar(items_tree_container, orient='vertical',
                                           command=items_tree.yview)
            items_tree.configure(yscrollcommand=scrollbar_items.set)

            items_tree.pack(side='left', fill='both', expand=True)
            scrollbar_items.pack(side='right', fill='y')

            for idx, item_data in enumerate(summary['top_items'], 1):
                tag = 'gold' if idx <= 3 else ('odd' if (idx % 2) else 'even')
                items_tree.insert('', 'end', values=(
                    f"#{idx}",
                    item_data['item_name'],
                    item_data['quantity_sold'],
                    f"ZMW {item_data['revenue']:.2f}"
                ), tags=(tag,))

            items_tree.tag_configure('gold', background='#fef3c7', foreground='#92400e')
            items_tree.tag_configure('odd', background='#f9fafb')
            items_tree.tag_configure('even', background='#ffffff')

            # Update status
            status_label.config(
                text=f"✅ Loaded report for {summary['month_name']} | "
                     f"Total Revenue: ZMW {overview_data['total_revenue']:.2f} | "
                     f"Transactions: {overview_data['total_transactions']}",
                fg='#27ae60'
            )

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load monthly report:\n{str(e)}")
            status_label.config(text=f"❌ Error loading report", fg='#e74c3c')

    # Load button
    tk.Button(select_row, text="📊 Load Report", command=load_monthly_report,
             bg='#3498db', fg='white', font=('Segoe UI', 11, 'bold'),
             padx=25, pady=8, relief='flat', cursor='hand2').pack(side='left', padx=15)

    # Export button
    def export_monthly_report():
        """Export monthly report to Excel"""
        selected_month_name = month_var.get()
        selected_month_num = months.index(selected_month_name) + 1
        selected_year = int(year_var.get())

        try:
            filepath, total_revenue = reporting_system.export_monthly_sales_to_excel(
                selected_year, selected_month_num
            )

            messagebox.showinfo(
                "Export Complete",
                f"✅ Monthly report exported successfully!\n\n"
                f"File: {filepath}\n"
                f"Month: {selected_month_name} {selected_year}\n"
                f"Total Revenue: ZMW {total_revenue:.2f}"
            )
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export report:\n{str(e)}")

    tk.Button(select_row, text="📥 Export to Excel", command=export_monthly_report,
             bg='#27ae60', fg='white', font=('Segoe UI', 11, 'bold'),
             padx=25, pady=8, relief='flat', cursor='hand2').pack(side='left', padx=5)

    # Auto-load current month on open
    load_monthly_report()


def create_cashier_interface(main_frame, root):
    """Redesigned cashier interface with visual item buttons and streamlined workflow"""
    
    # Main container with split design - cleaner background
    main_container = tk.Frame(main_frame, bg='#e8ecef')
    main_container.pack(fill='both', expand=True, padx=8, pady=8)

    # Theme toggle (Dark/Light) - more compact
    theme_frame = tk.Frame(main_container, bg='#e8ecef')
    theme_frame.pack(fill='x', pady=(0, 6))
    tk.Label(theme_frame, text="Theme:", bg='#e8ecef', font=('Segoe UI', 9)).pack(side='left', padx=(4, 2))
    def apply_theme(mode: str):
        try:
            if mode == 'dark':
                root.tk_setPalette(background='#2c3e50', foreground='#ecf0f1', activeBackground='#34495e', activeForeground='#ecf0f1')
            else:
                root.tk_setPalette(background='#ecf0f1', foreground='#2c3e50', activeBackground='#bdc3c7', activeForeground='#2c3e50')
        except Exception:
            pass
    tk.Button(theme_frame, text="☀️ Light", command=lambda: apply_theme('light'), bg='#f1f3f4', fg='#374151',
              font=('Segoe UI', 8), relief='flat', padx=8, pady=2, cursor='hand2').pack(side='left', padx=2)
    tk.Button(theme_frame, text="🌙 Dark", command=lambda: apply_theme('dark'), bg='#374151', fg='#f1f3f4',
              font=('Segoe UI', 8), relief='flat', padx=8, pady=2, cursor='hand2').pack(side='left', padx=2)

    # Left panel for categories and items (wider for more items)
    left_panel = tk.Frame(main_container, bg='#ffffff', relief='flat', bd=0)
    left_panel.pack(side='left', fill='both', expand=True, padx=(0, 6))

    # Right panel for cart - fixed width, card-like appearance
    right_panel = tk.Frame(main_container, bg='#ffffff', width=340, relief='flat', bd=0)
    right_panel.pack(side='right', fill='both', expand=False, padx=(6, 0))
    right_panel.pack_propagate(False)
    
    # Cart data
    cart = []  # list of dicts: {item, quantity, unit_price, notes}
    # Track stock actually visible in UI (from bags/inventory) - fixes Black Label issue
    visible_stock_map = {}

    # Helper: hover effect (defined early so other dialogs can use it)
    def add_hover_effect(button, normal_color, hover_color):
        def on_enter(e):
            button.config(bg=hover_color)
        def on_leave(e):
            button.config(bg=normal_color)
        button.bind("<Enter>", on_enter)
        button.bind("<Leave>", on_leave)

    # Quantity dialog (defined early so item click handlers can reference it)
    def show_quantity_dialog(item_name, unit_price):
        """Show dialog to select quantity and add to cart"""
        # Get current stock: prefer UI-visible stock, fallback to inventory
        try:
            available_stock = visible_stock_map.get(item_name)
            if available_stock is None:
                conn = get_db()
                cur = conn.cursor()
                cur.execute('SELECT quantity FROM inventory WHERE item=?', (item_name,))
                row = cur.fetchone()
                conn.close()
                available_stock = row[0] if row else 0

            # Check what's already in cart
            cart_quantity = sum(it['quantity'] for it in cart if it['item'] == item_name)
            remaining_stock = max(available_stock - cart_quantity, 0)
        except Exception:
            available_stock = 0
            cart_quantity = 0
            remaining_stock = 0

        dialog = tk.Toplevel(root)
        dialog.title(f"Add {item_name}")
        dialog.geometry("400x350")
        dialog.configure(bg='#ffffff')
        dialog.transient(root)
        dialog.grab_set()
        dialog.geometry("+%d+%d" % (root.winfo_rootx() + 100, root.winfo_rooty() + 100))

        tk.Label(dialog, text=item_name, font=('Arial', 14, 'bold'),
                bg='#ffffff', fg='#2c3e50').pack(pady=(15, 5))
        tk.Label(dialog, text=f"ZMW {unit_price:.2f}", font=('Arial', 12),
                bg='#ffffff', fg='#27ae60').pack(pady=(0, 5))

        # Stock availability indicator
        stock_frame = tk.Frame(dialog, bg='#ecf0f1', relief='solid', bd=1)
        stock_frame.pack(fill='x', padx=20, pady=(0, 15))

        stock_color = '#27ae60' if remaining_stock > 5 else ('#f39c12' if remaining_stock > 0 else '#e74c3c')
        stock_icon = '✅' if remaining_stock > 5 else ('⚠️' if remaining_stock > 0 else '❌')

        tk.Label(stock_frame, text=f"{stock_icon} Available: {remaining_stock} units",
                font=('Arial', 10, 'bold'), bg='#ecf0f1', fg=stock_color).pack(pady=8)

        if cart_quantity > 0:
            tk.Label(stock_frame, text=f"(Already in cart: {cart_quantity})",
                    font=('Arial', 9), bg='#ecf0f1', fg='#7f8c8d').pack(pady=(0, 8))

        qty_frame = tk.Frame(dialog, bg='#ffffff')
        qty_frame.pack(pady=10)
        qty_var = tk.StringVar(value="1")

        def validate_qty(P):
            if P == "":
                return True
            try:
                val = int(P)
                return val > 0
            except ValueError:
                return False
        vcmd = (dialog.register(validate_qty), '%P')
        dec_btn = tk.Button(qty_frame, text="-", font=('Arial', 16, 'bold'), 
                          bg='#e74c3c', fg='white', width=2, command=lambda: update_qty(-1))
        dec_btn.pack(side='left', padx=(0, 5))
        qty_entry = tk.Entry(qty_frame, textvariable=qty_var, font=('Arial', 14),
                           width=4, justify='center', validate='key', validatecommand=vcmd)
        qty_entry.pack(side='left')
        inc_btn = tk.Button(qty_frame, text="+", font=('Arial', 16, 'bold'), 
                          bg='#27ae60', fg='white', width=2, command=lambda: update_qty(1))
        inc_btn.pack(side='left', padx=(5, 0))
        def update_qty(delta):
            try:
                current = int(qty_var.get() or "1")
                new_val = current + delta
                if new_val > 0:
                    qty_var.set(str(new_val))
            except ValueError:
                qty_var.set("1")
        tk.Label(dialog, text="Notes (optional):", bg='#ffffff', anchor='w').pack(anchor='w', padx=20)
        notes_entry = tk.Entry(dialog, font=('Arial', 11), width=30)
        notes_entry.pack(padx=20, pady=(0, 10), fill='x')
        btn_frame = tk.Frame(dialog, bg='#ffffff')
        btn_frame.pack(pady=15)
        cancel_btn = tk.Button(btn_frame, text="Cancel", command=dialog.destroy,
                              bg='#95a5a6', fg='white', font=('Arial', 11))
        cancel_btn.pack(side='left', padx=10)
        def add_and_close():
            try:
                quantity = int(qty_var.get() or "1")
                notes = notes_entry.get().strip()
                add_to_cart(item_name, quantity, unit_price, notes)
                dialog.destroy()
            except ValueError:
                messagebox.showerror("Error", "Please enter a valid quantity")
        add_btn = tk.Button(btn_frame, text="Add to Order", command=add_and_close,
                         bg='#3498db', fg='white', font=('Arial', 11, 'bold'))
        add_btn.pack(side='left', padx=10)
        add_hover_effect(dec_btn, '#e74c3c', '#c0392b')
        add_hover_effect(inc_btn, '#27ae60', '#2ecc71')
        add_hover_effect(add_btn, '#3498db', '#2980b9')
        add_hover_effect(cancel_btn, '#95a5a6', '#7f8c8d')
        qty_entry.bind("<FocusIn>", lambda e: qty_entry.selection_range(0, tk.END))
        qty_entry.focus_set()
        qty_entry.bind("<Return>", lambda e: add_and_close())
        notes_entry.bind("<Return>", lambda e: add_and_close())
    
    # ---------- LEFT PANEL (ITEMS) ----------
    
    # Category tabs at the top
    category_frame = tk.Frame(left_panel, bg='#34495e', height=50)
    category_frame.pack(fill='x')
    
    # Item grid area
    items_frame = tk.Frame(left_panel, bg='#ffffff', padx=10, pady=10)
    items_frame.pack(fill='both', expand=True)
    
    # Fetch all available bags (bag-based inventory)
    try:
        from sales_utils import get_bags
        all_bags = get_bags()
        # Convert to list of (id, name) tuples
        bag_list = [("all", "All Items")] + all_bags
    except Exception as e:
        print(f"Error loading bags: {e}")
        bag_list = [("all", "All Items")]

    # Bag selection
    selected_bag = tk.StringVar()
    selected_bag.set("all")

    # Create bag buttons - more compact with modern styling
    bag_buttons = []
    for i, (bag_id, bag_name) in enumerate(bag_list):
        # Truncate long bag names with ellipsis
        display_name = bag_name[:12] + '...' if len(bag_name) > 12 else bag_name
        # First button (All Items) starts highlighted
        if i == 0:
            btn = tk.Button(category_frame, text=display_name, bg='#4299e1', fg='#ffffff',
                           font=('Segoe UI', 9, 'bold'), bd=0, padx=10, pady=6, relief='flat',
                           activebackground='#3182ce', activeforeground='#ffffff', cursor='hand2')
        else:
            btn = tk.Button(category_frame, text=display_name, bg='#2d3748', fg='#e2e8f0',
                           font=('Segoe UI', 9), bd=0, padx=10, pady=6, relief='flat',
                           activebackground='#4a5568', activeforeground='#ffffff', cursor='hand2')
        btn.pack(side='left', padx=1, pady=4)
        bag_buttons.append((btn, bag_id, bag_name))

    # Payment button in category bar - next to All Items (on right side)
    payment_btn = tk.Button(category_frame, text="💳 COMPLETE SALE",
                         bg='#16a34a', fg='white', font=('Segoe UI', 10, 'bold'),
                         padx=16, pady=6, state='disabled', relief='flat', cursor='hand2',
                         activebackground='#22c55e', activeforeground='#ffffff')
    payment_btn.pack(side='right', padx=6, pady=4)

    # Function to populate items based on selected bag
    def populate_items(bag_selection):
        # Clear existing items
        for widget in items_frame.winfo_children():
            widget.destroy()
        
        # Show loading indicator immediately - cleaner styling
        loading_label = tk.Label(items_frame, text="⏳ Loading...",
                                bg='#fafbfc', fg='#3b82f6', font=('Segoe UI', 11))
        loading_label.pack(pady=40)

        # Force UI update to show loading message
        items_frame.update_idletasks()

        # Get items from BOTH bag-based inventory AND legacy inventory
        try:
            from sales_utils import get_items_in_bag, get_bags, get_item_prices
            import sqlite3
            from sales_utils import DB_NAME

            # Remove loading label
            loading_label.destroy()

            if bag_selection == "all":
                # Show ONLY bags with toggle button to switch to legacy items
                all_bags = get_bags()

                # Top bar - cleaner, more compact
                top_bar = tk.Frame(items_frame, bg='#f1f5f9', relief='flat', bd=0)
                top_bar.pack(fill='x', pady=(0, 10))

                tk.Label(top_bar, text="📦 SELECT A BAG", font=('Segoe UI', 12, 'bold'),
                        bg='#f1f5f9', fg='#1e293b').pack(side='left', padx=8, pady=6)
                tk.Label(top_bar, text="Click any bag to view items", font=('Segoe UI', 9),
                        bg='#f1f5f9', fg='#64748b').pack(side='left', padx=(0, 8))

                # Display bag cards
                if all_bags:
                    # Single bag cards container
                    bags_container = tk.Frame(items_frame, bg='#fafbfc')
                    bags_container.pack(fill='both', expand=True, pady=4)

                    cols = 4  # More columns for compact view
                    for i, (bag_id, bag_name) in enumerate(all_bags):
                        # Get item count and total stock in this bag
                        bag_items = get_items_in_bag(bag_id)
                        item_count = len(bag_items)
                        total_stock = sum(stock for _, _, _, stock in bag_items)

                        # Calculate position in grid
                        row, col = divmod(i, cols)

                        # Bag card frame - compact design
                        bag_frame = tk.Frame(bags_container, bg='#3b82f6', width=150, height=110,
                                            relief='flat', bd=0, cursor='hand2')
                        bag_frame.grid(row=row, column=col, padx=6, pady=6, sticky='nsew')
                        bag_frame.grid_propagate(False)

                        # Bag icon - smaller
                        tk.Label(bag_frame, text="📦", bg='#3b82f6', font=('Segoe UI', 24)).pack(pady=(12, 4))

                        # Bag name - truncate if needed
                        display_name = bag_name[:12] + '...' if len(bag_name) > 12 else bag_name
                        tk.Label(bag_frame, text=display_name, bg='#3b82f6', fg='#ffffff',
                                font=('Segoe UI', 10, 'bold'), wraplength=140).pack(pady=2)

                        # Item count - compact
                        tk.Label(bag_frame, text=f"{item_count} items • {total_stock} units",
                                bg='#3b82f6', fg='#bfdbfe', font=('Segoe UI', 8)).pack()

                        # Make the entire card clickable to open the bag
                        def make_bag_click_handler(bid, bname):
                            def handler(e=None):
                                selected_bag.set(bid)
                                # Update button highlighting
                                for btn, _, _ in bag_buttons:
                                    btn.configure(bg='#2d3748', fg='#e2e8f0', font=('Segoe UI', 9))
                                # Find and highlight the clicked bag's button
                                for btn, btn_id, btn_name in bag_buttons:
                                    if btn_id == bid:
                                        btn.configure(bg='#4299e1', fg='#ffffff', font=('Segoe UI', 9, 'bold'))
                                        break
                                populate_items(bid)
                            return handler

                        bag_frame.bind("<Button-1>", make_bag_click_handler(bag_id, bag_name))
                        for child in bag_frame.winfo_children():
                            child.bind("<Button-1>", make_bag_click_handler(bag_id, bag_name))

                    # Configure grid to expand
                    for i in range(cols):
                        bags_container.grid_columnconfigure(i, weight=1)
                else:
                    # No bags message - cleaner styling
                    msg_frame = tk.Frame(items_frame, bg='#fafbfc')
                    msg_frame.pack(expand=True)
                    tk.Label(msg_frame,
                            text="No bags created yet!\n\nAdmin needs to:\n1. Click '📦 Manage Bags'\n2. Create bags\n3. Add items to bags",
                            font=('Segoe UI', 11), bg='#fafbfc', fg='#d97706',
                            justify='center').pack(pady=40)

                return  # Don't process items, just show bags

            else:
                # Show items from specific bag only (no legacy items here)
                bag_items = get_items_in_bag(int(bag_selection))
                # bag_items format: (id, item_name, price, stock)
                items = []
                for item_id, name, price, stock in bag_items:
                    items.append((name, stock, f"Bag {bag_selection}", price))
                    # Track visible stock for validation
                    visible_stock_map[name] = stock

            # If no items at all, show message
            if not items or len(items) == 0:
                msg_frame = tk.Frame(items_frame, bg='#ffffff')
                msg_frame.pack(expand=True)
                if bag_selection == "all":
                    message = "No items found!\n\nAdmin needs to:\n1. Click '➕ Add Item to Bag' to add bag items\n   OR\n2. Use 'Add/Edit Stock' to add inventory"
                else:
                    message = f"No items in this bag!\n\nAdmin needs to:\n1. Click '➕ Add Item to Bag'\n2. Select this bag\n3. Add items"
                tk.Label(msg_frame,
                        text=message,
                        font=('Arial', 12), bg='#ffffff', fg='#e67e22',
                        justify='center').pack(pady=50)
                return

        except Exception as e:
            # Remove loading label if it still exists
            try:
                loading_label.destroy()
            except:
                pass

            # Show error message in UI instead of popup (less disruptive)
            error_frame = tk.Frame(items_frame, bg='#ffffff')
            error_frame.pack(expand=True)
            tk.Label(error_frame,
                    text=f"❌ Error loading items:\n{str(e)}\n\nPlease try refreshing or contact support.",
                    font=('Arial', 11), bg='#ffffff', fg='#e74c3c',
                    justify='center').pack(pady=50)

            print(f"Error loading inventory: {e}")
            import traceback
            traceback.print_exc()
            return
        
        # Create item grid with 5 items per row - more compact
        cols = 5
        item_count = 0
        for item_data in items:
            if len(item_data) == 4:
                # New format: (item_name, stock, bag_name, price)
                item, qty, bag_name, sell_price = item_data
            else:
                # Fallback for legacy format: (item_name, stock, bag_name)
                item, qty, bag_name = item_data
                sell_price = 0

            # Track visible stock for this item
            visible_stock_map[item] = qty

            if qty <= 0:  # Skip out-of-stock items
                continue

            # Format price for display
            if sell_price and sell_price > 0:
                price_display = f"{sell_price:.2f}"
            else:
                price_display = "N/A"
                sell_price = 0
                
            # Calculate position in grid
            row, col = divmod(item_count, cols)
            item_count += 1

            # Determine stock-based background color for visual identification
            if qty <= 3:
                card_bg = '#fef2f2'  # Low stock - light red tint
                stock_fg = '#dc2626'
            elif qty <= 10:
                card_bg = '#fffbeb'  # Medium stock - light yellow tint
                stock_fg = '#d97706'
            else:
                card_bg = '#f0fdf4'  # Good stock - light green tint
                stock_fg = '#16a34a'

            # Item frame - smaller, compact card with subtle shadow effect
            item_frame = tk.Frame(items_frame, bg=card_bg, width=115, height=90,
                               relief='solid', bd=1, cursor='hand2')
            item_frame.grid(row=row, column=col, padx=4, pady=4, sticky='nsew')
            item_frame.grid_propagate(False)  # Maintain fixed size
            
            # Item name with wrapping - truncate long names
            display_name = item[:14] + '...' if len(item) > 14 else item
            name_label = tk.Label(item_frame, text=display_name, bg=card_bg, fg='#1e293b',
                               font=('Segoe UI', 9, 'bold'), wraplength=105, anchor='center')
            name_label.pack(pady=(8, 2), fill='x')

            # Price - prominent green text
            price_label = tk.Label(item_frame, text=f"K{price_display}", bg=card_bg,
                               fg='#059669', font=('Segoe UI', 9, 'bold'))
            price_label.pack()
            
            # Stock indicator - color-coded
            stock_label = tk.Label(item_frame, text=f"📦 {qty}", bg=card_bg,
                               fg=stock_fg, font=('Segoe UI', 8))
            stock_label.pack(pady=(0, 4))

            # Make the entire frame clickable with hover effect
            def make_click_handler(item_name=item, unit_price=sell_price):
                return lambda e: show_quantity_dialog(item_name, unit_price)
            
            def make_hover_handlers(frame, bg_color):
                def on_enter(e):
                    frame.config(relief='raised', bd=2)
                    for child in frame.winfo_children():
                        try:
                            child.config(bg='#e0f2fe')
                        except:
                            pass
                    frame.config(bg='#e0f2fe')
                def on_leave(e):
                    frame.config(relief='solid', bd=1, bg=bg_color)
                    for child in frame.winfo_children():
                        try:
                            child.config(bg=bg_color)
                        except:
                            pass
                return on_enter, on_leave

            on_enter, on_leave = make_hover_handlers(item_frame, card_bg)

            item_frame.bind("<Button-1>", make_click_handler())
            item_frame.bind("<Enter>", on_enter)
            item_frame.bind("<Leave>", on_leave)
            name_label.bind("<Button-1>", make_click_handler())
            price_label.bind("<Button-1>", make_click_handler())
            stock_label.bind("<Button-1>", make_click_handler())
            
        # Configure grid to expand
        for i in range(cols):
            items_frame.grid_columnconfigure(i, weight=1)
    
    # Set up bag button clicks
    def make_bag_handler(bag_id, bag_name, button):
        def handler():
            selected_bag.set(bag_id)
            for btn, _, _ in bag_buttons:
                btn.configure(bg='#34495e', fg='#ecf0f1')
            button.configure(bg='#ecf0f1', fg='#34495e')
            populate_items(bag_id)
        return handler
    
    for btn, bag_id, bag_name in bag_buttons:
        btn.config(command=make_bag_handler(bag_id, bag_name, btn))

    # Search box at the bottom of left panel - more compact design
    search_frame = tk.Frame(left_panel, bg='#ffffff', pady=6)
    search_frame.pack(fill='x')
    
    search_var = tk.StringVar()
    search_entry = tk.Entry(search_frame, textvariable=search_var, font=('Segoe UI', 10),
                         width=28, relief='solid', bd=1)
    search_entry.pack(side='left', padx=(8, 4), ipady=3)

    search_btn = tk.Button(search_frame, text="🔍 Search", font=('Segoe UI', 9, 'bold'),
                        bg='#4299e1', fg='white', relief='flat', padx=12, pady=4, cursor='hand2',
                        command=lambda: search_items())
    search_btn.pack(side='left')
    
    def search_items(*args):
        search_text = search_var.get().strip().lower()
        if search_text:
            # Clear existing items
            for widget in items_frame.winfo_children():
                widget.destroy()
            
            # Search in BOTH bag items AND legacy inventory
            try:
                from sales_utils import get_bags, get_items_in_bag, get_item_prices
                import sqlite3
                from sales_utils import DB_NAME

                # Get all items from all bags with prices
                all_items_with_prices = []
                all_bags = get_bags()
                for bag_id, bag_name in all_bags:
                    bag_items = get_items_in_bag(bag_id)
                    # bag_items format: (id, item_name, price, stock)
                    for item_id, item_name, price, stock in bag_items:
                        all_items_with_prices.append((item_name, stock, bag_name, price))

                # Also search legacy inventory
                try:
                    conn = sqlite3.connect(DB_NAME, timeout=10)
                    cur = conn.cursor()
                    cur.execute('SELECT item, quantity, category FROM inventory WHERE quantity > 0')
                    legacy_items = cur.fetchall()
                    conn.close()

                    for item_name, quantity, category in legacy_items:
                        prices = get_item_prices(item_name)
                        if prices:
                            _, sell_price = prices
                        else:
                            sell_price = 0
                        cat_display = category if category else "Other"
                        all_items_with_prices.append((item_name, quantity, cat_display, sell_price))
                except Exception as e:
                    print(f"No legacy inventory: {e}")

                # Filter by search text
                matching_items = [(item, qty, cat, price) for item, qty, cat, price in all_items_with_prices
                                if search_text in item.lower()]

                # Display matching items - compact 5-column grid
                cols = 5
                item_count = 0
                for item, qty, cat, sell_price in matching_items:
                    if qty <= 0:  # Skip out-of-stock items
                        continue

                    # Format price
                    if sell_price and sell_price > 0:
                        price_display = f"{sell_price:.2f}"
                    else:
                        price_display = "N/A"
                        sell_price = 0
                        
                    # Calculate position in grid
                    row, col = divmod(item_count, cols)
                    item_count += 1

                    # Determine stock-based background color
                    if qty <= 3:
                        card_bg = '#fef2f2'
                        stock_fg = '#dc2626'
                    elif qty <= 10:
                        card_bg = '#fffbeb'
                        stock_fg = '#d97706'
                    else:
                        card_bg = '#f0fdf4'
                        stock_fg = '#16a34a'

                    # Item frame - compact card
                    item_frame = tk.Frame(items_frame, bg=card_bg, width=115, height=90,
                                      relief='solid', bd=1, cursor='hand2')
                    item_frame.grid(row=row, column=col, padx=4, pady=4, sticky='nsew')
                    item_frame.grid_propagate(False)

                    # Item name with truncation
                    display_name = item[:14] + '...' if len(item) > 14 else item
                    name_label = tk.Label(item_frame, text=display_name, bg=card_bg, fg='#1e293b',
                                      font=('Segoe UI', 9, 'bold'), wraplength=105, anchor='center')
                    name_label.pack(pady=(8, 2), fill='x')

                    # Price
                    price_label = tk.Label(item_frame, text=f"K{price_display}", bg=card_bg,
                                      fg='#059669', font=('Segoe UI', 9, 'bold'))
                    price_label.pack()
                    
                    # Stock indicator
                    stock_label = tk.Label(item_frame, text=f"📦 {qty}", bg=card_bg,
                                      fg=stock_fg, font=('Segoe UI', 8))
                    stock_label.pack(pady=(0, 4))

                    # Make the entire frame clickable
                    def make_click_handler(item_name=item, unit_price=sell_price):
                        return lambda e: show_quantity_dialog(item_name, unit_price)
                    
                    item_frame.bind("<Button-1>", make_click_handler())
                    name_label.bind("<Button-1>", make_click_handler())
                    price_label.bind("<Button-1>", make_click_handler())
                    stock_label.bind("<Button-1>", make_click_handler())
                
                # Configure grid to expand
                for i in range(cols):
                    items_frame.grid_columnconfigure(i, weight=1)
            except Exception as e:
                messagebox.showerror("Search Error", f"Error searching items: {str(e)}")
    
    search_var.trace("w", search_items)
    search_entry.bind("<Return>", lambda e: search_items())
    
    # ---------- RIGHT PANEL (CART) ----------
    
    # Cart header with icon - cleaner styling
    cart_header = tk.Frame(right_panel, bg='#1e40af')
    cart_header.pack(fill='x')
    cart_label = tk.Label(cart_header, text="🛒 Current Order", font=('Segoe UI', 12, 'bold'),
                        bg='#1e40af', fg='#ffffff')
    cart_label.pack(pady=8, padx=10, anchor='w')

    # Cart treeview container
    cart_container = tk.Frame(right_panel, bg='#f8fafc')
    cart_container.pack(fill='both', expand=True, padx=4, pady=4)

    # Cart treeview with compact styling
    cart_columns = ("Item", "Qty", "Price", "Total")
    cart_tree = ttk.Treeview(cart_container, columns=cart_columns, show='headings', height=14)

    # Configure Treeview style for compact rows
    style = ttk.Style()
    style.configure("Treeview", font=('Segoe UI', 9), rowheight=24, background='#ffffff',
                   fieldbackground='#ffffff', foreground='#1e293b')
    style.configure("Treeview.Heading", font=('Segoe UI', 9, 'bold'), background='#e2e8f0',
                   foreground='#374151')
    style.map("Treeview", background=[('selected', '#dbeafe')], foreground=[('selected', '#1e40af')])

    cart_tree.pack(fill='both', expand=True, side='left')

    for c in cart_columns:
        cart_tree.heading(c, text=c)
        if c == "Item":
            cart_tree.column(c, width=110, stretch=True, anchor='w')
        elif c == "Qty":
            cart_tree.column(c, width=35, anchor='center', stretch=False)
        elif c == "Price":
            cart_tree.column(c, width=55, anchor='e', stretch=False)
        else:
            cart_tree.column(c, width=55, anchor='e', stretch=False)

    # Add scrollbar inside container
    cart_scroll = ttk.Scrollbar(cart_container, orient="vertical", command=cart_tree.yview)
    cart_scroll.pack(side='right', fill='y')
    cart_tree.configure(yscrollcommand=cart_scroll.set)
    
    # Cart functions
    def refresh_cart_tree():
        """Update the cart display and buttons"""
        for i in cart_tree.get_children():
            cart_tree.delete(i)
            
        for idx, it in enumerate(cart):
            subtotal = float(it['quantity']) * float(it['unit_price'])
            # Add alternating row tags for visual distinction
            tag = 'oddrow' if idx % 2 else 'evenrow'
            cart_tree.insert('', 'end', values=(it['item'], it['quantity'],
                                              f"{float(it['unit_price']):.2f}", 
                                              f"{subtotal:.2f}"), tags=(tag,))

        # Configure alternating row colors
        cart_tree.tag_configure('oddrow', background='#f8fafc')
        cart_tree.tag_configure('evenrow', background='#ffffff')

        # Update total display - clean format
        cart_total = sum(float(it['quantity']) * float(it['unit_price']) for it in cart)
        total_label.config(text=f"K {cart_total:,.2f}")

        # Enable/disable buttons
        if cart:
            payment_btn.config(state='normal')
            clear_btn.config(state='normal')
            remove_btn.config(state='normal')
        else:
            payment_btn.config(state='disabled')
            clear_btn.config(state='disabled')
            remove_btn.config(state='disabled')
    
    def add_to_cart(item_name, quantity, unit_price, notes=""):
        """Add item to cart, merging with existing items if needed"""
        # Determine available stock using same source as UI
        try:
            available_stock = visible_stock_map.get(item_name)
            if available_stock is None:
                conn = get_db()
                cur = conn.cursor()
                cur.execute('SELECT quantity FROM inventory WHERE item=?', (item_name,))
                row = cur.fetchone()
                conn.close()
                available_stock = row[0] if row else 0

            # Calculate total quantity including what's already in cart
            cart_quantity = 0
            for it in cart:
                if it['item'] == item_name:
                    cart_quantity += it['quantity']

            total_requested = cart_quantity + quantity

            if total_requested > available_stock:
                messagebox.showerror(
                    "Insufficient Stock",
                    f"Not enough stock for {item_name}\n\n"
                    f"Available: {available_stock}\n"
                    f"Already in cart: {cart_quantity}\n"
                    f"Requesting: {quantity}\n"
                    f"Total needed: {total_requested}\n\n"
                    f"Please reduce quantity or remove from cart first."
                )
                return
        except Exception as e:
            messagebox.showerror("Error", f"Failed to check stock: {str(e)}")
            return

        # Check if item exists in cart
        for it in cart:
            if it['item'] == item_name and abs(float(it['unit_price']) - float(unit_price)) < 0.01:
                it['quantity'] += quantity
                if notes and not it.get('notes'):
                    it['notes'] = notes
                refresh_cart_tree()
                return
        
        # Add new item
        cart.append({
            'item': item_name,
            'quantity': quantity,
            'unit_price': unit_price,
            'notes': notes
        })
        refresh_cart_tree()
    
    def remove_from_cart():
        """Remove selected item from cart"""
        sel = cart_tree.selection()
        if not sel:
            messagebox.showinfo("Remove Item", "Please select an item to remove")
            return
            
        idx = cart_tree.index(sel[0])
        cart.pop(idx)
        refresh_cart_tree()
    
    def clear_cart():
        """Clear all items from cart"""
        if not cart:
            return
            
        if messagebox.askyesno("Clear Order", "Are you sure you want to clear the entire order?"):
            cart.clear()
            refresh_cart_tree()
    
    # Compact Gen Z styled total display - cleaner design
    total_container = tk.Frame(right_panel, bg='#ffffff', relief='flat')
    total_container.pack(fill='x', padx=4, pady=(6, 4))

    # Modern gradient-style background
    total_bg = tk.Frame(total_container, bg='#059669', relief='flat', bd=0)
    total_bg.pack(fill='x')

    # Total display layout
    total_inner = tk.Frame(total_bg, bg='#059669')
    total_inner.pack(fill='x', padx=12, pady=10)

    # "TOTAL" label - compact
    tk.Label(total_inner, text="TOTAL", font=('Segoe UI', 10, 'bold'),
            bg='#059669', fg='#d1fae5').pack(side='left')

    # Amount display - prominent but not oversized
    total_label = tk.Label(total_inner, text="K 0.00", font=('Segoe UI', 20, 'bold'),
                         bg='#059669', fg='#ffffff')
    total_label.pack(side='right')

    # Separator line
    tk.Frame(right_panel, height=1, bg='#e2e8f0').pack(fill='x', padx=4)

    # Action buttons row - compact, grouped
    action_frame = tk.Frame(right_panel, bg='#ffffff')
    action_frame.pack(fill='x', padx=4, pady=6)

    # Button to remove selected item - compact styling
    remove_btn = tk.Button(action_frame, text="✕ Remove", command=remove_from_cart,
                         bg='#fecaca', fg='#dc2626', font=('Segoe UI', 9, 'bold'),
                         padx=12, pady=4, state='disabled', relief='flat', cursor='hand2',
                         activebackground='#fca5a5', activeforeground='#b91c1c')
    remove_btn.pack(side='left', padx=(0, 4))

    # Button to clear cart - compact styling
    clear_btn = tk.Button(action_frame, text="🗑 Clear", command=clear_cart,
                        bg='#e2e8f0', fg='#475569', font=('Segoe UI', 9, 'bold'),
                        padx=12, pady=4, state='disabled', relief='flat', cursor='hand2',
                        activebackground='#cbd5e1', activeforeground='#334155')
    clear_btn.pack(side='left')


    
    # Hover effects for buttons
    def add_hover_effect(button, normal_color, hover_color):
        """Add hover effect to a button"""
        def on_enter(e):
            button.config(bg=hover_color)
        def on_leave(e):
            button.config(bg=normal_color)
        button.bind("<Enter>", on_enter)
        button.bind("<Leave>", on_leave)
    
    # Add a button for reporting loss/drawn
    def report_loss_dialog():
        # Reporting loss/drawn feature disabled
        try:
            messagebox.showinfo("Not Available", "Reporting Loss/Drawn has been disabled.")
        except Exception:
            pass



    
    # Create the Report Loss/Drawn button after the handler is defined
    # Report Loss/Drawn button removed
    add_hover_effect(remove_btn, '#fecaca', '#fca5a5')
    add_hover_effect(clear_btn, '#e2e8f0', '#cbd5e1')
    add_hover_effect(payment_btn, '#16a34a', '#22c55e')
    add_hover_effect(search_btn, '#4299e1', '#3b82f6')

    # ---------- DIALOGS ----------
    
    # Quantity dialog
    def show_quantity_dialog(item_name, unit_price):
        """Show dialog to select quantity and add to cart"""
        dialog = tk.Toplevel(root)
        dialog.title(f"Add {item_name}")
        dialog.geometry("360x280")
        dialog.configure(bg='#ffffff')
        dialog.transient(root)
        dialog.grab_set()
        
        # Center the dialog
        dialog.geometry("+%d+%d" % (root.winfo_rootx() + 100, root.winfo_rooty() + 100))
        
        # Item info - cleaner header
        header_frame = tk.Frame(dialog, bg='#f1f5f9')
        header_frame.pack(fill='x')

        display_name = item_name[:25] + '...' if len(item_name) > 25 else item_name
        tk.Label(header_frame, text=display_name, font=('Segoe UI', 12, 'bold'),
                bg='#f1f5f9', fg='#1e293b').pack(pady=(12, 4))

        tk.Label(header_frame, text=f"K {unit_price:.2f}", font=('Segoe UI', 14, 'bold'),
                bg='#f1f5f9', fg='#059669').pack(pady=(0, 10))

        # Quantity frame
        qty_frame = tk.Frame(dialog, bg='#ffffff')
        qty_frame.pack(pady=16)

        tk.Label(qty_frame, text="Quantity:", font=('Segoe UI', 10),
                bg='#ffffff', fg='#64748b').pack(side='left', padx=(0, 8))

        qty_var = tk.StringVar(value="1")
        
        def validate_qty(P):
            """Validate quantity - only allow positive integers"""
            if P == "":
                return True
            try:
                val = int(P)
                return val > 0
            except ValueError:
                return False
        
        vcmd = (dialog.register(validate_qty), '%P')
        
        # Decrease quantity button - compact
        dec_btn = tk.Button(qty_frame, text="−", font=('Segoe UI', 14, 'bold'),
                          bg='#fee2e2', fg='#dc2626', width=3, relief='flat',
                          command=lambda: update_qty(-1), cursor='hand2')
        dec_btn.pack(side='left', padx=2)

        # Quantity entry - cleaner
        qty_entry = tk.Entry(qty_frame, textvariable=qty_var, font=('Segoe UI', 14, 'bold'),
                           width=4, justify='center', validate='key', validatecommand=vcmd,
                           relief='solid', bd=1)
        qty_entry.pack(side='left')
        
        # Increase quantity button - compact
        inc_btn = tk.Button(qty_frame, text="+", font=('Segoe UI', 14, 'bold'),
                          bg='#dcfce7', fg='#16a34a', width=3, relief='flat',
                          command=lambda: update_qty(1), cursor='hand2')
        inc_btn.pack(side='left', padx=2)

        def update_qty(delta):
            """Update quantity by delta"""
            try:
                current = int(qty_var.get() or "1")
                new_val = current + delta
                if new_val > 0:
                    qty_var.set(str(new_val))
            except ValueError:
                qty_var.set("1")
        
        # Notes field - compact
        notes_frame = tk.Frame(dialog, bg='#ffffff')
        notes_frame.pack(fill='x', padx=16, pady=(0, 8))
        tk.Label(notes_frame, text="Notes (optional):", bg='#ffffff',
                font=('Segoe UI', 9), fg='#64748b').pack(anchor='w')
        notes_entry = tk.Entry(notes_frame, font=('Segoe UI', 10), relief='solid', bd=1)
        notes_entry.pack(fill='x', pady=(2, 0), ipady=3)

        # Buttons - cleaner layout
        btn_frame = tk.Frame(dialog, bg='#ffffff')
        btn_frame.pack(pady=12, fill='x', padx=16)

        cancel_btn = tk.Button(btn_frame, text="Cancel", command=dialog.destroy,
                              bg='#e2e8f0', fg='#475569', font=('Segoe UI', 10),
                              relief='flat', padx=16, pady=6, cursor='hand2')
        cancel_btn.pack(side='left')

        def add_and_close():
            """Add item to cart and close dialog"""
            try:
                quantity = int(qty_var.get() or "1")
                notes = notes_entry.get().strip()
                add_to_cart(item_name, quantity, unit_price, notes)
                dialog.destroy()
            except ValueError:
                messagebox.showerror("Error", "Please enter a valid quantity")
        
        add_btn = tk.Button(btn_frame, text="✓ Add to Order", command=add_and_close,
                         bg='#3b82f6', fg='white', font=('Segoe UI', 10, 'bold'),
                         relief='flat', padx=16, pady=6, cursor='hand2')
        add_btn.pack(side='right')

        # Add hover effects
        add_hover_effect(dec_btn, '#fee2e2', '#fecaca')
        add_hover_effect(inc_btn, '#dcfce7', '#bbf7d0')
        add_hover_effect(add_btn, '#3b82f6', '#2563eb')
        add_hover_effect(cancel_btn, '#e2e8f0', '#cbd5e1')

        # Select all text when focusing on qty entry
        qty_entry.bind("<FocusIn>", lambda e: qty_entry.selection_range(0, tk.END))
        qty_entry.focus_set()
        
        # Bind Enter key to add
        qty_entry.bind("<Return>", lambda e: add_and_close())
        notes_entry.bind("<Return>", lambda e: add_and_close())
    
    # Process payment
    def process_payment():
        """Process payment for items in cart"""
        if not cart:
            return
        
        try:
            total_amount = sum(float(it['quantity']) * float(it['unit_price']) for it in cart)
        except:
            total_amount = 0.0
        
        # Payment dialog - cleaner design
        payment_dialog = tk.Toplevel(root)
        payment_dialog.title("Complete Sale")
        payment_dialog.geometry("480x480")
        payment_dialog.configure(bg='#ffffff')
        payment_dialog.transient(root)
        payment_dialog.grab_set()
        
        # Center the dialog
        payment_dialog.geometry("+%d+%d" % (root.winfo_rootx() + 80, root.winfo_rooty() + 60))

        # Header with total - prominent display
        header_frame = tk.Frame(payment_dialog, bg='#059669')
        header_frame.pack(fill='x')
        tk.Label(header_frame, text="💳 COMPLETE SALE", font=('Segoe UI', 11, 'bold'),
                bg='#059669', fg='#d1fae5').pack(pady=(12, 4))
        tk.Label(header_frame, text=f"K {total_amount:,.2f}",
                font=('Segoe UI', 24, 'bold'), bg='#059669', fg='#ffffff').pack(pady=(0, 12))

        # Payment type selection - cleaner card design
        payment_type = tk.StringVar(value="Cash")
        
        payment_frame = tk.LabelFrame(payment_dialog, text="Select Payment Method",
                                    font=('Segoe UI', 10, 'bold'), bg='#ffffff', fg='#374151',
                                    padx=16, pady=12, relief='flat', bd=0)
        payment_frame.pack(fill='x', padx=16, pady=12)

        tk.Radiobutton(payment_frame, text="💵 Cash", variable=payment_type, value="Cash",
                      bg='#ffffff', font=('Segoe UI', 10), cursor='hand2',
                      activebackground='#f0fdf4').pack(anchor='w', pady=3)
        tk.Radiobutton(payment_frame, text="📱 Mobile Money", variable=payment_type, value="Mobile Money",
                      bg='#ffffff', font=('Segoe UI', 10), cursor='hand2',
                      activebackground='#f0fdf4').pack(anchor='w', pady=3)
        tk.Radiobutton(payment_frame, text="💳 Card/Other", variable=payment_type, value="Card",
                      bg='#ffffff', font=('Segoe UI', 10), cursor='hand2',
                      activebackground='#f0fdf4').pack(anchor='w', pady=3)

        # Dynamic input fields
        input_frame = tk.Frame(payment_dialog, bg='#ffffff')
        input_frame.pack(fill='x', padx=16, pady=8)

        # Variables for inputs
        cash_received = tk.StringVar()
        mobile_ref = tk.StringVar()
        
        # Input widgets (initially hidden) - cleaner styling
        cash_label = tk.Label(input_frame, text="Cash Received (K):", bg='#ffffff', font=('Segoe UI', 10))
        cash_entry = tk.Entry(input_frame, textvariable=cash_received, font=('Segoe UI', 12),
                             width=18, relief='solid', bd=1)

        mobile_label = tk.Label(input_frame, text="Transaction Reference:", bg='#ffffff', font=('Segoe UI', 10))
        mobile_entry = tk.Entry(input_frame, textvariable=mobile_ref, font=('Segoe UI', 11),
                               width=22, relief='solid', bd=1)

        change_label = tk.Label(input_frame, text="", bg='#ffffff', font=('Segoe UI', 12, 'bold'), fg='#dc2626')

        def update_payment_fields(*args):
            # Hide all fields first
            for widget in [cash_label, cash_entry, mobile_label, mobile_entry, change_label]:
                widget.pack_forget()
            
            if payment_type.get() == "Cash":
                cash_label.pack(anchor='w', pady=(5, 2))
                cash_entry.pack(anchor='w', pady=(0, 5), ipady=4)
                change_label.pack(anchor='w', pady=5)
                cash_entry.focus_set()
            elif payment_type.get() == "Mobile Money":
                mobile_label.pack(anchor='w', pady=(5, 2))
                mobile_entry.pack(anchor='w', pady=(0, 5), ipady=4)
                mobile_entry.focus_set()
            validate_input()
        
        def calculate_change(*args):
            if payment_type.get() == "Cash":
                try:
                    received = float(cash_received.get() or '0')
                    change = received - total_amount
                    if change >= 0:
                        change_label.config(text=f"✅ Change: K {change:,.2f}", fg='#16a34a')
                    else:
                        change_label.config(text=f"⚠️ Short: K {abs(change):,.2f}", fg='#dc2626')
                except ValueError:
                    change_label.config(text="", fg='#dc2626')
            validate_input()
        
        # Buttons - cleaner layout
        button_frame = tk.Frame(payment_dialog, bg='#ffffff')
        button_frame.pack(pady=16, fill='x', padx=16)

        cancel_btn = tk.Button(button_frame, text="Cancel", command=payment_dialog.destroy,
                             bg='#e2e8f0', fg='#475569', font=('Segoe UI', 10, 'bold'),
                             padx=20, pady=8, relief='flat', cursor='hand2')
        cancel_btn.pack(side='left')

        complete_btn = tk.Button(button_frame, text="✓ Complete Sale", command=lambda: None,
                              bg='#16a34a', fg='white', font=('Segoe UI', 11, 'bold'),
                              padx=24, pady=8, state='disabled', relief='flat', cursor='hand2')
        complete_btn.pack(side='right')

        def validate_input(*args):
            method = payment_type.get()
            if method == "Cash":
                try:
                    received = float(cash_received.get() or '0')
                    complete_btn.config(state='normal' if received >= total_amount else 'disabled')
                except ValueError:
                    complete_btn.config(state='disabled')
            elif method == "Mobile Money":
                complete_btn.config(state='normal' if mobile_ref.get().strip() else 'disabled')
            else:
                complete_btn.config(state='normal')
        
        def finalize_payment():
            payment_method = payment_type.get()
            
            # Validation based on payment type
            if payment_method == "Cash":
                try:
                    received = float(cash_received.get() or '0')
                    if received < total_amount:
                        messagebox.showerror("Error", "Insufficient cash received")
                        return
                    change = received - total_amount
                except ValueError:
                    messagebox.showerror("Error", "Please enter a valid cash amount")
                    return
            elif payment_method == "Mobile Money":
                if not mobile_ref.get().strip():
                    messagebox.showerror("Error", "Please enter transaction reference")
                    return
                received = total_amount
                change = 0
            else:
                received = total_amount
                change = 0
            
            # Process the sale
            try:
                from sales_utils import create_sale_with_items, log_audit_event
                from datetime import datetime
                
                # Prepare cart items for the cart-based system
                cart_items = []
                for item in cart:
                    cart_items.append({
                        'item': item['item'],
                        'quantity': item['quantity'],
                        'unit_price': item['unit_price']
                    })
                
                # Create the sale using the cart-based system
                sale_id, tx_id, total = create_sale_with_items(
                    current_user['username'],
                    cart_items,
                    payment_method=payment_method,
                    mobile_ref=(mobile_ref.get().strip() if payment_method == "Mobile Money" else None)
                )
                
                # Log the transaction
                log_audit_event(f"Payment processed by {current_user['username']} TX={tx_id} Method={payment_method} Total={total}")
                
                # Show receipt
                payment_dialog.destroy()
                show_popup_receipt(
                    root, current_user, sale_id, tx_id, total, payment_method, 
                    received, change,
                    mobile_ref.get() if payment_method == "Mobile Money" else None,
                    cart_items=cart  # Pass cart items for fallback display
                )
                
                # Clear cart and refresh
                cart.clear()
                refresh_cart_tree()
                
            except Exception as e:
                messagebox.showerror("Payment Error", f"Error processing payment: {str(e)}")
                print(f"Payment error: {e}")
        
        # Update the complete button command
        complete_btn.config(command=finalize_payment)
        
        # Set up trace callbacks
        payment_type.trace('w', update_payment_fields)
        cash_received.trace('w', calculate_change)
        mobile_ref.trace('w', validate_input)
        
        # Initialize fields
        update_payment_fields()
        
        # Add hover effects
        add_hover_effect(complete_btn, '#16a34a', '#22c55e')
        add_hover_effect(cancel_btn, '#e2e8f0', '#cbd5e1')

    # Void order
    def void_order():
        """Void the current order"""
        if not cart:
            return
        
        if messagebox.askyesno("Void Order", "Are you sure you want to void this entire order?"):
            from sales_utils import log_audit_event
            
            # Log the void action
            cart_items = ", ".join([f"{it['item']} x{it['quantity']}" for it in cart])
            log_audit_event(f"Order voided by {current_user['username']} - Items: {cart_items}")
            
            # Clear cart
            cart.clear()
            refresh_cart_tree()
            messagebox.showinfo("Order Voided", "The order has been voided and logged.")
    
    # Set the payment button command now that process_payment is defined
    payment_btn.config(command=process_payment)

    # ---------- KEYBOARD SHORTCUTS ----------
    
    def handle_key(event):
        """Handle keyboard shortcuts"""
        # + key for search
        if event.char == "+":
            search_entry.focus_set()
        # p key for payment
        elif event.char == "p" and cart:
            process_payment()
        # v key for void
        elif event.char == "v" and cart:
            void_order()
        # Delete key for removing selected item
        elif event.keysym == "Delete" and cart:
            remove_from_cart()
    
    root.bind("<Key>", handle_key)
    
    # Show loading message - cleaner styling
    loading_frame = tk.Frame(items_frame, bg='#fafbfc')
    loading_frame.pack(expand=True)
    tk.Label(loading_frame, text="⏳ Loading items...",
            bg='#fafbfc', fg='#3b82f6', font=('Segoe UI', 12, 'bold')).pack(pady=40)

    # Defer initial load to prevent UI freeze (100ms delay)
    def deferred_load():
        try:
            loading_frame.destroy()
        except:
            pass
        populate_items("all")

    root.after(100, deferred_load)

    # Register the refresh callback for admin stock management
    def refresh_items():
        try:
            populate_items(selected_bag.get())
        except Exception:
            pass

    register_refresh_items_cb(refresh_items, lambda: selected_bag.get())


# ==================== APPLICATION ENTRY POINT ====================
if __name__ == '__main__':
    # Run installer/activation gate (will exit if password not provided)
    try:
        require_installer_password_if_needed()
    except SystemExit:
        raise
    except Exception as e:
        print(f"Activation check failed: {e}")
        pass

    # Start the login / main UI
    try:
        restart_login()
    except Exception as e:
        import traceback, sys
        traceback.print_exc()
        messagebox.showerror("Startup Error", f"Failed to start application:\n{str(e)}")
        sys.exit(1)