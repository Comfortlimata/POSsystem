# dashboard.py - Professional Admin Dashboard
import tkinter as tk
from tkinter import ttk, messagebox, font
from sales_utils import get_total_sales, export_to_csv, get_all_stock, init_db
import sqlite3
from datetime import datetime, timedelta
import os

class AdminDashboard:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Bar Sales Admin Dashboard – Gorgeous Brides Boutique")
        self.root.geometry("1200x800")
        self.root.configure(bg='#f0f0f0')
        
        # Initialize database
        init_db()
        
        # Configure styles
        self.setup_styles()
        
        # Create main layout
        self.create_layout()
        
        # Load dashboard data
        self.refresh_dashboard()
        
    def setup_styles(self):
        """Configure professional styling"""
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # Configure colors and fonts
        self.colors = {
            'primary': '#2c3e50',
            'secondary': '#3498db',
            'success': '#27ae60',
            'warning': '#f39c12',
            'danger': '#e74c3c',
            'light': '#ecf0f1',
            'dark': '#34495e'
        }
        
        # Configure ttk styles
        self.style.configure('Title.TLabel', font=('Arial', 16, 'bold'), background='#f0f0f0')
        self.style.configure('Heading.TLabel', font=('Arial', 12, 'bold'), background='#f0f0f0')
        self.style.configure('Card.TFrame', relief='raised', borderwidth=2, background='white')
        
    def create_layout(self):
        """Create the main dashboard layout"""
        # Header
        header_frame = tk.Frame(self.root, bg=self.colors['primary'], height=80)
        header_frame.pack(fill='x', padx=0, pady=0)
        header_frame.pack_propagate(False)
        
        title_label = tk.Label(header_frame, text="Admin Dashboard", 
                              font=('Arial', 20, 'bold'), 
                              fg='white', bg=self.colors['primary'])
        title_label.pack(pady=20)
        
        # Main content area
        main_frame = tk.Frame(self.root, bg='#f0f0f0')
        main_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Statistics cards row
        stats_frame = tk.Frame(main_frame, bg='#f0f0f0')
        stats_frame.pack(fill='x', pady=(0, 20))
        
        self.create_stats_cards(stats_frame)
        
        # Content area with tabs
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill='both', expand=True)
        
        # Sales Overview Tab
        sales_frame = ttk.Frame(notebook)
        notebook.add(sales_frame, text="Sales Overview")
        self.create_sales_overview(sales_frame)
        
        # Inventory Tab
        inventory_frame = ttk.Frame(notebook)
        notebook.add(inventory_frame, text="Inventory Management")
        self.create_inventory_tab(inventory_frame)
        
        # Reports Tab
        reports_frame = ttk.Frame(notebook)
        notebook.add(reports_frame, text="Reports & Export")
        self.create_reports_tab(reports_frame)
        
    def create_stats_cards(self, parent):
        """Create statistics cards"""
        # Today's Sales Card
        self.today_sales_card = self.create_stat_card(parent, "Today's Sales", "ZMW 0.00", self.colors['success'])
        self.today_sales_card.pack(side='left', padx=(0, 10), fill='x', expand=True)
        
        # Total Sales Card
        self.total_sales_card = self.create_stat_card(parent, "Total Sales", "ZMW 0.00", self.colors['secondary'])
        self.total_sales_card.pack(side='left', padx=10, fill='x', expand=True)
        
        # Low Stock Items Card
        self.low_stock_card = self.create_stat_card(parent, "Low Stock Items", "0", self.colors['warning'])
        self.low_stock_card.pack(side='left', padx=10, fill='x', expand=True)
        
        # Total Items Card
        self.total_items_card = self.create_stat_card(parent, "Total Items", "0", self.colors['dark'])
        self.total_items_card.pack(side='left', padx=(10, 0), fill='x', expand=True)
        
    def create_stat_card(self, parent, title, value, color):
        """Create a statistics card"""
        card = tk.Frame(parent, bg='white', relief='raised', bd=2)
        
        # Color bar at top
        color_bar = tk.Frame(card, bg=color, height=4)
        color_bar.pack(fill='x')
        
        # Content
        content_frame = tk.Frame(card, bg='white')
        content_frame.pack(fill='both', expand=True, padx=15, pady=15)
        
        title_label = tk.Label(content_frame, text=title, font=('Arial', 10), 
                              fg='#7f8c8d', bg='white')
        title_label.pack(anchor='w')
        
        value_label = tk.Label(content_frame, text=value, font=('Arial', 18, 'bold'), 
                              fg=color, bg='white')
        value_label.pack(anchor='w')
        
        # Store reference to value label for updates
        card.value_label = value_label
        
        return card
        
    def create_sales_overview(self, parent):
        """Create sales overview tab content"""
        # Recent sales frame
        recent_frame = ttk.LabelFrame(parent, text="Recent Sales", padding=10)
        recent_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Treeview for recent sales
        columns = ('Time', 'User', 'Item', 'Quantity', 'Price', 'Total')
        self.sales_tree = ttk.Treeview(recent_frame, columns=columns, show='headings', height=15)
        
        for col in columns:
            self.sales_tree.heading(col, text=col)
            self.sales_tree.column(col, width=120)
            
        # Scrollbar for treeview
        scrollbar = ttk.Scrollbar(recent_frame, orient='vertical', command=self.sales_tree.yview)
        self.sales_tree.configure(yscrollcommand=scrollbar.set)
        
        self.sales_tree.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
        
    def create_inventory_tab(self, parent):
        """Create inventory management tab"""
        # Controls frame
        controls_frame = ttk.Frame(parent)
        controls_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(controls_frame, text="Add New Item", 
                  command=self.add_new_item).pack(side='left', padx=(0, 10))
        ttk.Button(controls_frame, text="Update Stock", 
                  command=self.update_stock).pack(side='left', padx=10)
        ttk.Button(controls_frame, text="Check Low Stock", 
                  command=self.check_low_stock).pack(side='left', padx=10)
        ttk.Button(controls_frame, text="Refresh", 
                  command=self.refresh_inventory).pack(side='right')
        
        # Inventory frame
        inventory_frame = ttk.LabelFrame(parent, text="Current Inventory", padding=10)
        inventory_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Inventory treeview
        inv_columns = ('Item', 'Quantity', 'Category', 'Cost Price', 'Selling Price')
        self.inventory_tree = ttk.Treeview(inventory_frame, columns=inv_columns, show='headings', height=15)
        
        for col in inv_columns:
            self.inventory_tree.heading(col, text=col)
            self.inventory_tree.column(col, width=150)
            
        # Scrollbar for inventory
        inv_scrollbar = ttk.Scrollbar(inventory_frame, orient='vertical', command=self.inventory_tree.yview)
        self.inventory_tree.configure(yscrollcommand=inv_scrollbar.set)
        
        self.inventory_tree.pack(side='left', fill='both', expand=True)
        inv_scrollbar.pack(side='right', fill='y')
        
    def create_reports_tab(self, parent):
        """Create reports and export tab"""
        # Export controls
        export_frame = ttk.LabelFrame(parent, text="Export Options", padding=10)
        export_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(export_frame, text="Export All Sales", 
                  command=self.export_all_sales).pack(side='left', padx=(0, 10))
        ttk.Button(export_frame, text="Export Today's Sales", 
                  command=self.export_today_sales).pack(side='left', padx=10)
        ttk.Button(export_frame, text="Export Inventory", 
                  command=self.export_inventory).pack(side='left', padx=10)
        
        # Reports frame
        reports_frame = ttk.LabelFrame(parent, text="Quick Reports", padding=10)
        reports_frame.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Report text area
        self.report_text = tk.Text(reports_frame, wrap='word', font=('Consolas', 10))
        report_scrollbar = ttk.Scrollbar(reports_frame, orient='vertical', command=self.report_text.yview)
        self.report_text.configure(yscrollcommand=report_scrollbar.set)
        
        self.report_text.pack(side='left', fill='both', expand=True)
        report_scrollbar.pack(side='right', fill='y')
        
    def refresh_dashboard(self):
        """Refresh all dashboard data"""
        self.update_stats_cards()
        self.load_recent_sales()
        self.load_inventory()
        self.generate_quick_report()
        
    def update_stats_cards(self):
        """Update statistics cards with current data"""
        try:
            # Today's sales
            today = datetime.now().strftime("%Y-%m-%d")
            conn = sqlite3.connect("bar_sales.db", timeout=30)
            cur = conn.cursor()
            
            cur.execute("SELECT SUM(total) FROM sales WHERE DATE(timestamp)=?", (today,))
            today_sales = cur.fetchone()[0] or 0
            self.today_sales_card.value_label.config(text=f"ZMW {today_sales:.2f}")
            
            # Total sales
            total_sales = get_total_sales()
            self.total_sales_card.value_label.config(text=f"ZMW {total_sales:.2f}")
            
            # Low stock items (threshold: 5)
            cur.execute("SELECT COUNT(*) FROM inventory WHERE quantity <= 5")
            low_stock_count = cur.fetchone()[0] or 0
            self.low_stock_card.value_label.config(text=str(low_stock_count))
            
            # Total items
            cur.execute("SELECT COUNT(*) FROM inventory")
            total_items = cur.fetchone()[0] or 0
            self.total_items_card.value_label.config(text=str(total_items))
            
            conn.close()
        except Exception as e:
            print(f"Error updating stats: {e}")
            
    def load_recent_sales(self):
        """Load recent sales into treeview"""
        try:
            # Clear existing items
            for item in self.sales_tree.get_children():
                self.sales_tree.delete(item)
                
            conn = sqlite3.connect("bar_sales.db", timeout=30)
            cur = conn.cursor()
            cur.execute("""SELECT timestamp, username, item, quantity, price_per_unit, total 
                          FROM sales ORDER BY timestamp DESC LIMIT 50""")
            
            for row in cur.fetchall():
                # Format timestamp
                timestamp = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S").strftime("%m/%d %H:%M")
                formatted_row = (timestamp, row[1], row[2], row[3], f"ZMW {row[4]:.2f}", f"ZMW {row[5]:.2f}")
                self.sales_tree.insert('', 'end', values=formatted_row)
                
            conn.close()
        except Exception as e:
            print(f"Error loading sales: {e}")
            
    def load_inventory(self):
        """Load inventory into treeview"""
        try:
            # Clear existing items
            for item in self.inventory_tree.get_children():
                self.inventory_tree.delete(item)
                
            stock_list = get_all_stock()
            conn = sqlite3.connect("bar_sales.db", timeout=30)
            cur = conn.cursor()
            for item, qty, category in stock_list:
                # Get prices
                cur.execute("SELECT cost_price, selling_price FROM inventory WHERE item=?", (item,))
                prices = cur.fetchone()
                
                cost_price = f"ZMW {prices[0]:.2f}" if prices and prices[0] else "N/A"
                sell_price = f"ZMW {prices[1]:.2f}" if prices and prices[1] else "N/A"
                
                # Color code low stock items
                tags = ('low_stock',) if qty <= 5 else ()
                self.inventory_tree.insert('', 'end', values=(item, qty, category or "N/A", cost_price, sell_price), tags=tags)
                
            # Configure tag colors
            self.inventory_tree.tag_configure('low_stock', background='#ffebee')
            conn.close()
            
        except Exception as e:
            print(f"Error loading inventory: {e}")
            
    def generate_quick_report(self):
        """Generate a quick summary report"""
        try:
            self.report_text.delete(1.0, tk.END)
            
            report = f"=== SALES REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M')} ===\n\n"
            
            conn = sqlite3.connect("bar_sales.db", timeout=30)
            cur = conn.cursor()
            
            # Today's summary
            today = datetime.now().strftime("%Y-%m-%d")
            cur.execute("SELECT COUNT(*), SUM(total) FROM sales WHERE DATE(timestamp)=?", (today,))
            today_count, today_total = cur.fetchone()
            today_count = today_count or 0
            today_total = today_total or 0
            
            report += f"TODAY'S PERFORMANCE:\n"
            report += f"  Transactions: {today_count}\n"
            report += f"  Total Sales: ZMW {today_total:.2f}\n\n"
            
            # Top selling items today
            cur.execute("""SELECT item, SUM(quantity) as qty, SUM(total) as total_sales 
                          FROM sales WHERE DATE(timestamp)=? 
                          GROUP BY item ORDER BY qty DESC LIMIT 5""", (today,))
            
            report += "TOP SELLING ITEMS TODAY:\n"
            for item, qty, total_sales in cur.fetchall():
                report += f"  {item}: {qty} units (ZMW {total_sales:.2f})\n"
            
            # Low stock alerts
            cur.execute("SELECT item, quantity FROM inventory WHERE quantity <= 5 ORDER BY quantity")
            low_stock = cur.fetchall()
            
            if low_stock:
                report += f"\nLOW STOCK ALERTS:\n"
                for item, qty in low_stock:
                    report += f"  {item}: {qty} remaining\n"
            else:
                report += f"\nAll items are sufficiently stocked.\n"
                
            conn.close()
            
            self.report_text.insert(1.0, report)
            
        except Exception as e:
            print(f"Error generating report: {e}")
            
    # Event handlers
    def add_new_item(self):
        """Add new inventory item"""
        messagebox.showinfo("Add Item", "This feature will open the inventory management window.")
        
    def update_stock(self):
        """Update stock levels"""
        messagebox.showinfo("Update Stock", "This feature will open the stock update window.")
        
    def check_low_stock(self):
        """Check and display low stock items"""
        try:
            conn = sqlite3.connect("bar_sales.db", timeout=30)
            cur = conn.cursor()
            cur.execute("SELECT item, quantity FROM inventory WHERE quantity <= 5 ORDER BY quantity")
            low_stock = cur.fetchall()
            conn.close()
            
            if low_stock:
                items = "\n".join([f"• {item}: {qty} remaining" for item, qty in low_stock])
                messagebox.showwarning("Low Stock Alert", f"The following items are low in stock:\n\n{items}")
            else:
                messagebox.showinfo("Stock Status", "All items are sufficiently stocked!")
                
        except Exception as e:
            messagebox.showerror("Error", f"Error checking stock: {e}")
            
    def refresh_inventory(self):
        """Refresh inventory display"""
        self.load_inventory()
        messagebox.showinfo("Refresh", "Inventory data refreshed!")
        
    def export_all_sales(self):
        """Export all sales data"""
        try:
            export_to_csv()
            messagebox.showinfo("Export Complete", "All sales data exported to 'exports/sales.csv'")
        except Exception as e:
            messagebox.showerror("Export Error", f"Error exporting sales: {e}")
            
    def export_today_sales(self):
        """Export today's sales"""
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            conn = sqlite3.connect("bar_sales.db", timeout=30)
            cur = conn.cursor()
            cur.execute("SELECT * FROM sales WHERE DATE(timestamp)=?", (today,))
            rows = cur.fetchall()
            conn.close()
            
            if not os.path.exists("exports"):
                os.makedirs("exports")
                
            import csv
            filename = f"exports/today_sales_{today}.csv"
            with open(filename, "w", newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(["ID", "Username", "Item", "Quantity", "Price", "Total", "Timestamp"])
                writer.writerows(rows)
                
            messagebox.showinfo("Export Complete", f"Today's sales exported to '{filename}'")
            
        except Exception as e:
            messagebox.showerror("Export Error", f"Error exporting today's sales: {e}")
            
    def export_inventory(self):
        """Export inventory data"""
        try:
            stock_list = get_all_stock()
            
            if not os.path.exists("exports"):
                os.makedirs("exports")
                
            import csv
            today = datetime.now().strftime("%Y-%m-%d")
            filename = f"exports/inventory_{today}.csv"
            
            with open(filename, "w", newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(["Item", "Quantity", "Category"])
                writer.writerows(stock_list)
                
            messagebox.showinfo("Export Complete", f"Inventory exported to '{filename}'")
            
        except Exception as e:
            messagebox.showerror("Export Error", f"Error exporting inventory: {e}")
            
    def run(self):
        """Start the dashboard"""
        self.root.mainloop()

def main():
    """Main function to run the dashboard"""
    try:
        dashboard = AdminDashboard()
        dashboard.run()
    except Exception as e:
        print(f"Error starting dashboard: {e}")
        messagebox.showerror("Startup Error", f"Failed to start dashboard: {e}")

if __name__ == "__main__":
    main()
