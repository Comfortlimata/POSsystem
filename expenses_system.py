"""
Daily Expenses Management System for Cashier
Tracks daily expenses like lunch, supplies, etc.
"""

import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
import csv
import os
from pathlib import Path
from sales_utils import log_audit_event

# Use the same DB_NAME as sales_utils to ensure consistency
from sales_utils import DB_NAME

# ========================
# Database Functions
# ========================

def init_expenses_db():
    """Initialize expenses table in database"""
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                category TEXT NOT NULL,
                description TEXT,
                amount REAL NOT NULL,
                cashier TEXT NOT NULL,
                created_at TEXT NOT NULL,
                notes TEXT
            )
        """)
        
        # Create index for better performance
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_date ON daily_expenses(date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_cashier ON daily_expenses(cashier)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_category ON daily_expenses(category)")
        
        conn.commit()
    except Exception as e:
        print(f"Error initializing expenses database: {e}")
    finally:
        conn.close()

def save_expense(date, category, description, amount, cashier, notes=""):
    """Save a new expense record"""
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cur.execute("""
            INSERT INTO daily_expenses 
            (date, category, description, amount, cashier, created_at, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (date, category, description, amount, cashier, now, notes))
        conn.commit()
        expense_id = cur.lastrowid
        
        # Log audit event
        try:
            log_audit_event(f"Expense recorded: {category} - ZMW {amount:.2f} by {cashier}")
        except Exception:
            pass
        
        return expense_id
    except Exception as e:
        raise Exception(f"Failed to save expense: {e}")
    finally:
        conn.close()

def get_expenses_by_date(date):
    """Get all expenses for a specific date"""
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, date, category, description, amount, cashier, created_at, notes
            FROM daily_expenses
            WHERE date = ?
            ORDER BY created_at DESC
        """, (date,))
        return cur.fetchall()
    finally:
        conn.close()

def get_expenses_by_date_range(start_date, end_date):
    """Get all expenses within a date range"""
    # Initialize table if it doesn't exist
    init_expenses_db()

    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, date, category, description, amount, cashier, created_at, notes
            FROM daily_expenses
            WHERE date BETWEEN ? AND ?
            ORDER BY date DESC, created_at DESC
        """, (start_date, end_date))
        return cur.fetchall()
    finally:
        conn.close()

def get_expenses_by_cashier(cashier, date):
    """Get all expenses for a specific cashier on a specific date"""
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, date, category, description, amount, cashier, created_at, notes
            FROM daily_expenses
            WHERE cashier = ? AND date = ?
            ORDER BY created_at DESC
        """, (cashier, date))
        return cur.fetchall()
    finally:
        conn.close()

def delete_expense(expense_id):
    """Delete an expense record"""
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM daily_expenses WHERE id = ?", (expense_id,))
        conn.commit()
        return True
    except Exception as e:
        raise Exception(f"Failed to delete expense: {e}")
    finally:
        conn.close()

def get_daily_total(date):
    """Get total expenses for a specific date"""
    conn = sqlite3.connect(DB_NAME, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT SUM(amount) FROM daily_expenses WHERE date = ?
        """, (date,))
        result = cur.fetchone()
        return result[0] if result[0] is not None else 0.0
    finally:
        conn.close()

def export_expenses_to_csv(start_date, end_date, export_format='CSV'):
    """Export expenses to CSV or Excel"""
    try:
        if not os.path.exists('exports'):
            os.makedirs('exports')
        
        records = get_expenses_by_date_range(start_date, end_date)
        today = datetime.now().strftime('%Y-%m-%d')
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')

        if export_format == 'Excel':
            try:
                from excel_styler import excel_styler

                filename = f"exports/expenses_{start_date}_to_{end_date}_{ts}.xlsx"

                # Create professionally styled workbook
                wb, ws = excel_styler.create_workbook("Expenses Report")

                # Add business header
                current_row = excel_styler.add_business_header(
                    ws,
                    "Daily Expenses Report",
                    f"{start_date} to {end_date}",
                    "Expense Management System"
                )

                # Calculate totals and category breakdown
                total_amount = sum(record[4] for record in records)
                category_totals = {}
                for record in records:
                    category = record[2]
                    amount = record[4]
                    category_totals[category] = category_totals.get(category, 0) + amount

                # Summary section with automatic color detection
                current_row = excel_styler.format_section_title_auto(
                    ws, "💰 EXPENSE SUMMARY DASHBOARD", current_row, 4
                )

                summary_headers = ['Metric', 'Value', 'Amount (ZMW)', 'Details']
                current_row = excel_styler.format_header(ws, summary_headers, current_row)

                summary_data = [
                    ['Total Expenses', len(records), f'{total_amount:.2f}', f'{len(category_totals)} Categories'],
                    ['Date Range', f'{start_date} to {end_date}', '', f'{len(records)} Records'],
                    ['Average per Day', '', f'{total_amount/max(1,(datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days + 1):.2f}', 'Daily Average']
                ]

                current_row = excel_styler.format_data_rows(ws, summary_data, current_row, 'summary')
                current_row += 2

                # Category breakdown section with automatic gray theme
                current_row = excel_styler.format_section_title_auto(
                    ws, "📊 EXPENSES BY CATEGORY BREAKDOWN", current_row, 4
                )

                category_headers = ['Category', 'Amount (ZMW)', 'Percentage', 'Count']
                current_row = excel_styler.format_header(ws, category_headers, current_row)

                category_data = []
                for category, amount in sorted(category_totals.items(), key=lambda x: x[1], reverse=True):
                    count = len([r for r in records if r[2] == category])
                    percentage = f"{(amount/total_amount*100 if total_amount > 0 else 0):.1f}%"
                    category_data.append([category, f"{amount:.2f}", percentage, str(count)])

                current_row = excel_styler.format_data_rows(ws, category_data, current_row, 'expenses')
                current_row += 2

                # Detailed records section with automatic gray theme
                current_row = excel_styler.format_section_title_auto(
                    ws, "📋 DETAILED EXPENSE RECORDS", current_row, 8
                )

                detail_headers = [
                    'ID', 'Date', 'Category', 'Description', 'Amount (ZMW)',
                    'Cashier', 'Created At', 'Notes'
                ]
                current_row = excel_styler.format_header(ws, detail_headers, current_row)

                # Format detailed records
                detailed_data = []
                for record in records:
                    detailed_data.append([
                        record[0], record[1], record[2], record[3],
                        f"{record[4]:.2f}", record[5], record[6], record[7] or ''
                    ])

                current_row = excel_styler.format_data_rows_auto(ws, detailed_data, current_row, "expense records")

                # Add total row
                total_row = ['', 'TOTAL EXPENSES', '', '', f'{total_amount:.2f}', '', f'{len(records)} Records', '']
                excel_styler.format_total_row(ws, total_row, current_row, 'expenses')

                # Auto-size columns
                excel_styler.auto_size_columns(ws)

                # Save Excel file
                filepath = excel_styler.save_workbook(wb, os.path.basename(filename))

            except ImportError:
                # Fall back to CSV if Excel not available
                export_format = 'CSV'

        if export_format == 'CSV':
            filename = f"exports/expenses_{start_date}_to_{end_date}_{ts}.csv"

            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['DAILY EXPENSES REPORT'])
                writer.writerow([f'Period: {start_date} to {end_date}'])
                writer.writerow([f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}'])
                writer.writerow([])
                writer.writerow([
                    'ID', 'Date', 'Category', 'Description', 'Amount (ZMW)',
                    'Cashier', 'Created At', 'Notes'
                ])

                total_amount = 0
                for record in records:
                    writer.writerow([
                        record[0], record[1], record[2], record[3],
                        f"{record[4]:.2f}", record[5], record[6], record[7] or ''
                    ])
                    total_amount += record[4]

                # Add summary row
                writer.writerow([])
                writer.writerow(['TOTAL EXPENSES', '', '', '', f"{total_amount:.2f}"])

            filepath = filename

        # Audit log
        try:
            log_audit_event(f"Expenses export created: {filepath}")
        except Exception:
            pass
        
        return filepath
    except Exception as e:
        raise Exception(f"Failed to export expenses: {e}")

# ========================
# UI Functions
# ========================

def show_expenses_window(parent, current_user):
    """Show the daily expenses window"""
    
    # Initialize DB
    init_expenses_db()
    
    # Create window
    window = tk.Toplevel(parent)
    window.title("💰 Daily Expenses Management")
    window.geometry("900x600")
    window.configure(bg='#ecf0f1')
    
    # Make window resizable
    window.resizable(True, True)
    window.minsize(800, 500)
    
    # Header
    header_frame = tk.Frame(window, bg='#2c3e50', height=70)
    header_frame.pack(fill='x')
    header_frame.pack_propagate(False)
    
    tk.Label(header_frame, text="💰 DAILY EXPENSES MANAGEMENT",
            font=('Arial', 16, 'bold'), bg='#2c3e50', fg='#ecf0f1').pack(pady=10)
    tk.Label(header_frame, text="Record and track daily expenses like lunch, supplies, etc.",
            font=('Arial', 10), bg='#2c3e50', fg='#bdc3c7').pack()
    
    # Main content frame
    content_frame = tk.Frame(window, bg='#ecf0f1')
    content_frame.pack(fill='both', expand=True, padx=20, pady=15)
    
    # Form section
    form_frame = tk.LabelFrame(content_frame, text="Add New Expense",
                              font=('Arial', 11, 'bold'), bg='#ffffff', padx=15, pady=15)
    form_frame.pack(fill='x', pady=(0, 15))
    
    # Date
    tk.Label(form_frame, text="Date (YYYY-MM-DD):", bg='#ffffff', font=('Arial', 10)).grid(row=0, column=0, sticky='e', padx=5, pady=5)
    date_var = tk.StringVar(value=datetime.now().strftime('%Y-%m-%d'))
    date_entry = tk.Entry(form_frame, textvariable=date_var, font=('Arial', 10), width=20)
    date_entry.grid(row=0, column=1, sticky='w', padx=5, pady=5)
    
    # Category
    tk.Label(form_frame, text="Category:", bg='#ffffff', font=('Arial', 10)).grid(row=1, column=0, sticky='e', padx=5, pady=5)
    category_var = tk.StringVar()
    category_options = ['Lunch', 'Supplies', 'Transport', 'Utilities', 'Maintenance', 'Other']
    category_combo = ttk.Combobox(form_frame, textvariable=category_var, values=category_options,
                                 font=('Arial', 10), width=28, state='readonly')
    category_combo.grid(row=1, column=1, sticky='w', padx=5, pady=5)
    
    # Description
    tk.Label(form_frame, text="Description:", bg='#ffffff', font=('Arial', 10)).grid(row=2, column=0, sticky='e', padx=5, pady=5)
    description_var = tk.StringVar()
    description_entry = tk.Entry(form_frame, textvariable=description_var, font=('Arial', 10), width=40)
    description_entry.grid(row=2, column=1, sticky='w', padx=5, pady=5)
    
    # Amount
    tk.Label(form_frame, text="Amount (ZMW):", bg='#ffffff', font=('Arial', 10)).grid(row=3, column=0, sticky='e', padx=5, pady=5)
    amount_var = tk.StringVar()
    amount_entry = tk.Entry(form_frame, textvariable=amount_var, font=('Arial', 10), width=20)
    amount_entry.grid(row=3, column=1, sticky='w', padx=5, pady=5)
    
    # Notes
    tk.Label(form_frame, text="Notes (optional):", bg='#ffffff', font=('Arial', 10)).grid(row=4, column=0, sticky='ne', padx=5, pady=5)
    notes_text = tk.Text(form_frame, font=('Arial', 10), width=40, height=3)
    notes_text.grid(row=4, column=1, sticky='w', padx=5, pady=5)
    
    # Validation and save functions
    def validate_expense():
        date = date_var.get().strip()
        category = category_var.get().strip()
        description = description_var.get().strip()
        amount = amount_var.get().strip()
        
        if not date:
            return False, "Date is required"
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return False, "Invalid date format (use YYYY-MM-DD)"
        
        if not category:
            return False, "Category is required"
        
        if not description:
            return False, "Description is required"
        
        if not amount:
            return False, "Amount is required"
        
        try:
            amt = float(amount)
            if amt <= 0:
                return False, "Amount must be positive"
        except ValueError:
            return False, "Amount must be a number"
        
        return True, ""
    
    def save_expense_record():
        valid, msg = validate_expense()
        if not valid:
            messagebox.showerror("Validation Error", msg)
            return
        
        try:
            expense_id = save_expense(
                date=date_var.get().strip(),
                category=category_var.get().strip(),
                description=description_var.get().strip(),
                amount=float(amount_var.get().strip()),
                cashier=current_user.get('username', 'Unknown'),
                notes=notes_text.get('1.0', 'end-1c').strip()
            )
            
            messagebox.showinfo("Success", f"Expense #{expense_id} recorded successfully!")
            clear_form()
            load_expenses()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save expense:\n{str(e)}")
    
    def clear_form():
        date_var.set(datetime.now().strftime('%Y-%m-%d'))
        category_var.set('')
        description_var.set('')
        amount_var.set('')
        notes_text.delete('1.0', 'end')
    
    # Buttons for form
    form_btn_frame = tk.Frame(form_frame, bg='#ffffff')
    form_btn_frame.grid(row=5, column=0, columnspan=2, pady=10)
    
    tk.Button(form_btn_frame, text="💾 Save Expense", command=save_expense_record,
             bg='#27ae60', fg='white', font=('Arial', 10, 'bold'),
             padx=15, pady=8).pack(side='left', padx=5)
    
    tk.Button(form_btn_frame, text="🗑️ Clear", command=clear_form,
             bg='#f39c12', fg='white', font=('Arial', 10, 'bold'),
             padx=15, pady=8).pack(side='left', padx=5)
    
    # Expenses list section
    list_frame = tk.LabelFrame(content_frame, text="Today's Expenses",
                              font=('Arial', 11, 'bold'), bg='#ffffff', padx=10, pady=10)
    list_frame.pack(fill='both', expand=True, pady=(0, 15))
    
    # Table
    columns = ('ID', 'Category', 'Description', 'Amount', 'Cashier', 'Time')
    tree_style = ttk.Style()
    tree_style.configure("Expenses.Treeview", rowheight=25, font=('Arial', 9))
    tree_style.configure("Expenses.Treeview.Heading", font=('Arial', 10, 'bold'))
    
    tree = ttk.Treeview(list_frame, columns=columns, show='headings', height=10, style="Expenses.Treeview")
    
    tree.column('ID', width=40, anchor='center')
    tree.column('Category', width=100)
    tree.column('Description', width=250)
    tree.column('Amount', width=100, anchor='e')
    tree.column('Cashier', width=100)
    tree.column('Time', width=100)
    
    for col in columns:
        tree.heading(col, text=col)
    
    # Color tags
    tree.tag_configure('expense', background='#fff9e6', foreground='#2c3e50')
    
    # Scrollbar
    scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=scrollbar.set)
    
    tree.pack(side='left', fill='both', expand=True)
    scrollbar.pack(side='right', fill='y')
    
    def load_expenses():
        # Clear existing
        for item in tree.get_children():
            tree.delete(item)
        
        today = datetime.now().strftime('%Y-%m-%d')
        expenses = get_expenses_by_date(today)
        
        total = 0
        for expense in expenses:
            tree.insert('', 'end', values=(
                expense[0], expense[2], expense[3], 
                f"ZMW {expense[4]:.2f}", expense[5], 
                expense[6].split(' ')[1] if ' ' in expense[6] else expense[6]
            ), tags=('expense',))
            total += expense[4]
        
        # Update total label
        total_label.config(text=f"Today's Total Expenses: ZMW {total:.2f}")
    
    def delete_selected():
        selection = tree.selection()
        if not selection:
            messagebox.showwarning("No Selection", "Please select an expense to delete")
            return
        
        item = tree.item(selection[0])
        expense_id = item['values'][0]
        
        if messagebox.askyesno("Confirm Delete", "Are you sure you want to delete this expense?"):
            try:
                delete_expense(expense_id)
                messagebox.showinfo("Success", "Expense deleted successfully!")
                load_expenses()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to delete expense:\n{str(e)}")
    
    # Bottom section with total and buttons
    bottom_frame = tk.Frame(window, bg='#ecf0f1')
    bottom_frame.pack(fill='x', padx=20, pady=10)
    
    total_label = tk.Label(bottom_frame, text="Today's Total Expenses: ZMW 0.00",
                          bg='#ecf0f1', font=('Arial', 11, 'bold'), fg='#27ae60')
    total_label.pack(side='left', padx=10)
    
    # Action buttons
    btn_frame = tk.Frame(window, bg='#ecf0f1')
    btn_frame.pack(fill='x', padx=20, pady=10)
    
    tk.Button(btn_frame, text="🗑️ Delete Selected", command=delete_selected,
             bg='#e74c3c', fg='white', font=('Arial', 10, 'bold'),
             padx=15, pady=8).pack(side='left', padx=5)
    
    tk.Button(btn_frame, text="🔄 Refresh", command=load_expenses,
             bg='#3498db', fg='white', font=('Arial', 10, 'bold'),
             padx=15, pady=8).pack(side='left', padx=5)
    
    tk.Button(btn_frame, text="❌ Close", command=window.destroy,
             bg='#95a5a6', fg='white', font=('Arial', 10, 'bold'),
             padx=15, pady=8).pack(side='right', padx=5)
    
    # Initial load
    load_expenses()
