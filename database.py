import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
import hashlib
import os
from collections import Counter

DB_NAME = "inventory_system.db"

def get_connection():
    """Returns a connection to the SQLite database."""
    return sqlite3.connect(DB_NAME, check_same_thread=False, timeout=30)

def init_db():
    """Initializes the database, tables, and seeds default data."""
    conn = get_connection()
    c = conn.cursor()
    
    # Products table - Stores product details
    c.execute('''CREATE TABLE IF NOT EXISTS products
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  name TEXT NOT NULL, 
                  category TEXT, 
                  price REAL, 
                  stock INTEGER, 
                  cost_price REAL, 
                  sales_count INTEGER DEFAULT 0,
                  last_restock_date TEXT,
                  expiry_date TEXT,
                  image_data BLOB)''')
    
    # Sales table - Stores transaction history
    # We ensure items_data exists. If user has old DB, we might need to migrate.
    c.execute('''CREATE TABLE IF NOT EXISTS sales
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  timestamp TEXT, 
                  total_amount REAL, 
                  items_data TEXT, 
                  integrity_hash TEXT, 
                  operator TEXT, 
                  payment_mode TEXT, 
                  status TEXT DEFAULT 'Completed', 
                  time_taken REAL DEFAULT 0, 
                  pos_id TEXT DEFAULT 'POS-1',
                  customer_mobile TEXT,
                  tax_amount REAL DEFAULT 0.0,
                  discount_amount REAL DEFAULT 0.0,
                  coupon_applied TEXT,
                  cancellation_reason TEXT,
                  cancelled_by TEXT,
                  cancellation_timestamp TEXT)''')
    
    # Migration Check: Ensure items_data column exists
    try:
        c.execute("SELECT items_data FROM sales LIMIT 1")
    except sqlite3.OperationalError:
        # Column missing, alter table
        try:
            c.execute("ALTER TABLE sales ADD COLUMN items_data TEXT")
        except:
            pass # Might fail if table is locked or other issue, but usually works

    c.execute('''CREATE TABLE IF NOT EXISTS system_settings
                 (key TEXT PRIMARY KEY, value TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS categories
                 (name TEXT PRIMARY KEY)''')

    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (username TEXT PRIMARY KEY, 
                  password_hash TEXT, 
                  role TEXT, 
                  full_name TEXT,
                  status TEXT DEFAULT 'Active')''')
                 
    c.execute('''CREATE TABLE IF NOT EXISTS logs
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, 
                  user TEXT, action TEXT, details TEXT)''')
    
    # Customers table
    c.execute('''CREATE TABLE IF NOT EXISTS customers
                 (mobile TEXT PRIMARY KEY, 
                  name TEXT, 
                  email TEXT, 
                  visits INTEGER DEFAULT 0, 
                  total_spend REAL DEFAULT 0.0,
                  segment TEXT DEFAULT 'New')''')

    c.execute('''CREATE TABLE IF NOT EXISTS terminals
                 (id TEXT PRIMARY KEY, 
                  name TEXT, 
                  location TEXT, 
                  status TEXT DEFAULT 'Active')''')

    c.execute('''CREATE TABLE IF NOT EXISTS lucky_draw_history
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  date TEXT,
                  winner_name TEXT,
                  winner_mobile TEXT,
                  prize TEXT)''')

    defaults = {
        "store_name": "SmartInventory Enterprise",
        "upi_id": "merchant@okaxis",
        "currency_symbol": "₹",
        "tax_rate": "18",
        "gst_enabled": "False",
        "default_bill_mode": "Non-GST"
    }
    for k, v in defaults.items():
        c.execute("INSERT OR IGNORE INTO system_settings (key, value) VALUES (?, ?)", (k, v))

    default_cats = ["Electronics", "Groceries", "Beverages", "Fashion", "Stationery", "Health"]
    for cat in default_cats:
        c.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (cat,))

    users = [
        ('admin', 'admin123', 'Admin', 'System Admin'),
        ('operator', 'pos123', 'Operator', 'POS Operator')
    ]
    for u, p, r, n in users:
        ph = hashlib.sha256(p.encode()).hexdigest()
        c.execute("INSERT OR REPLACE INTO users (username, password_hash, role, full_name, status) VALUES (?, ?, ?, ?, 'Active')", (u, ph, r, n))

    conn.commit()
    conn.close()

def get_setting(key):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT value FROM system_settings WHERE key=?", (key,))
    res = c.fetchone()
    conn.close()
    return res[0] if res else None

def set_setting(key, value):
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO system_settings (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

def log_activity(user, action, details):
    conn = get_connection()
    c = conn.cursor()
    c.execute("INSERT INTO logs (timestamp, user, action, details) VALUES (?, ?, ?, ?)",
              (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), user, action, details))
    conn.commit()
    conn.close()

def process_sale_transaction(cart_items, total, mode, operator, pos_id, customer_mobile, 
                             tax_amount, integrity_hash, time_taken):
    """
    Saves a sales transaction with strict stock validation.
    """
    conn = get_connection()
    c = conn.cursor()
    sale_id = None
    try:
        # 1. STRICT STOCK VALIDATION
        # Calculate required quantities per product ID
        item_ids = [i['id'] for i in cart_items]
        req_counts = Counter(item_ids)

        for pid, qty in req_counts.items():
            c.execute("SELECT stock, name FROM products WHERE id=?", (pid,))
            row = c.fetchone()
            if not row:
                raise Exception(f"Product ID {pid} not found in database.")
            
            curr_stock, p_name = row
            if curr_stock < qty:
                raise Exception(f"Insufficient stock for '{p_name}'. Available: {curr_stock}, Required: {qty}. Sale blocked.")

        # 2. Update Stock (Only if validation passes)
        for pid, qty in req_counts.items():
            c.execute("UPDATE products SET stock = stock - ?, sales_count = sales_count + ? WHERE id=?", (qty, qty, pid))
        
        # 3. Create Sales Record
        items_data_str = ",".join([str(pid) for pid in item_ids])
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        c.execute("""INSERT INTO sales (timestamp, total_amount, items_data, integrity_hash, 
                     operator, payment_mode, time_taken, pos_id, customer_mobile, 
                     tax_amount, discount_amount, coupon_applied) 
                     VALUES (?,?,?,?,?,?,?,?,?,?,0.0,NULL)""",
                (timestamp, total, items_data_str, integrity_hash, operator, mode, time_taken, 
                 pos_id, customer_mobile, tax_amount))
        sale_id = c.lastrowid

        # 4. Update Customer Metrics
        if customer_mobile:
            customer_mobile = customer_mobile.strip()
            c.execute("SELECT total_spend FROM customers WHERE mobile=?", (customer_mobile,))
            res = c.fetchone()
            if res:
                curr_spend = res[0]
                new_spend = curr_spend + total
                
                new_seg = "New"
                if new_spend > 50000: new_seg = "High-Value"
                elif new_spend > 10000: new_seg = "Regular"
                else: new_seg = "Occasional"
                
                c.execute("""UPDATE customers SET visits = visits + 1, total_spend = ?, 
                             segment = ? WHERE mobile=?""", 
                          (new_spend, new_seg, customer_mobile))

        conn.commit()
        return sale_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def cancel_sale_transaction(sale_id, operator, role, reason, password):
    if not reason or len(reason.strip()) < 3:
        return False, "Cancellation reason is mandatory and must be descriptive."

    conn = get_connection()
    c = conn.cursor()
    
    try:
        ph = hashlib.sha256(password.encode()).hexdigest()
        c.execute("SELECT 1 FROM users WHERE username=? AND password_hash=?", (operator, ph))
        if not c.fetchone():
            return False, "Invalid Password. Identity verification failed."

        c.execute("SELECT items_data, status, operator, total_amount, timestamp, customer_mobile FROM sales WHERE id=?", (sale_id,))
        res = c.fetchone()
        if not res:
            return False, "Sale ID not found"
        
        items_data_str, status, sale_operator, total_amount, sale_timestamp_str, cust_mobile = res
        
        if status == 'Cancelled':
            return False, "Sale already cancelled"
            
        # Parse comma-separated string
        if items_data_str:
            items_ids = [int(x) for x in items_data_str.split(',') if x.strip()]
            # Restore stock
            for pid in items_ids:
                c.execute("UPDATE products SET stock = stock + 1, sales_count = sales_count - 1 WHERE id=?", (pid,))
        
        # Adjust customer spend if linked
        if cust_mobile:
            c.execute("UPDATE customers SET total_spend = total_spend - ? WHERE mobile=?", (total_amount, cust_mobile))

        cancel_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""UPDATE sales SET status = 'Cancelled', cancellation_reason = ?, cancelled_by = ?, cancellation_timestamp = ? 
                     WHERE id=?""", (reason, operator, cancel_time, sale_id))
        
        log_msg = f"Cancelled Sale #{sale_id}. Value: {total_amount}. Reason: {reason}"
        c.execute("INSERT INTO logs (timestamp, user, action, details) VALUES (?, ?, ?, ?)",
                  (cancel_time, operator, "Undo Sale", log_msg))
        
        conn.commit()
        return True, "Success. Order cancelled."
        
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

def get_customer(mobile):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM customers WHERE mobile=?", (mobile.strip(),))
    row = c.fetchone()
    conn.close()
    if row:
        seg = row[5] if len(row) > 5 and row[5] is not None else 'New'
        return {"mobile": row[0], "name": row[1], "email": row[2], "visits": row[3], "total_spend": row[4], "segment": seg}
    return None

def upsert_customer(mobile, name, email):
    mobile = mobile.strip()
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT visits FROM customers WHERE mobile=?", (mobile,))
    res = c.fetchone()
    if res:
        c.execute("UPDATE customers SET name=?, email=? WHERE mobile=?", (name, email, mobile))
    else:
        c.execute("INSERT INTO customers (mobile, name, email, visits, total_spend, segment) VALUES (?, ?, ?, 0, 0, 'New')", (mobile, name, email))
    conn.commit()
    conn.close()

def get_all_customers():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM customers", conn)
    conn.close()
    return df

def create_user(username, password, role, fullname):
    conn = get_connection()
    c = conn.cursor()
    ph = hashlib.sha256(password.encode()).hexdigest()
    try:
        c.execute("INSERT INTO users (username, password_hash, role, full_name, status) VALUES (?, ?, ?, ?, 'Active')", (username, ph, role, fullname))
        conn.commit()
        return True
    except:
        return False
    finally:
        conn.close()

def update_user_status(username, status):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE users SET status=? WHERE username=?", (status, username))
    conn.commit()
    conn.close()

def get_user_status(username):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT status FROM users WHERE username=?", (username,))
    res = c.fetchone()
    conn.close()
    return res[0] if res else "Active"

def update_password(username, new_password):
    conn = get_connection()
    c = conn.cursor()
    ph = hashlib.sha256(new_password.encode()).hexdigest()
    c.execute("UPDATE users SET password_hash=? WHERE username=?", (ph, username))
    conn.commit()
    conn.close()

def update_fullname(username, name):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE users SET full_name=? WHERE username=?", (name, username))
    conn.commit()
    conn.close()

def get_all_users():
    conn = get_connection()
    df = pd.read_sql("SELECT username, role, full_name, status FROM users", conn)
    conn.close()
    return df

def verify_password(username, password):
    conn = get_connection()
    c = conn.cursor()
    ph = hashlib.sha256(password.encode()).hexdigest()
    c.execute("SELECT 1 FROM users WHERE username=? AND password_hash=?", (username, ph))
    res = c.fetchone()
    conn.close()
    return res is not None

def pick_lucky_winner(lookback_days, min_spend, prize_desc):
    conn = get_connection()
    c = conn.cursor()
    
    cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d %H:%M:%S")
    
    # Find eligible customers based on sales in period
    query = """
    SELECT customer_mobile, SUM(total_amount) as spent 
    FROM sales 
    WHERE timestamp >= ? AND customer_mobile IS NOT NULL AND status != 'Cancelled'
    GROUP BY customer_mobile 
    HAVING spent >= ?
    """
    c.execute(query, (cutoff_date, min_spend))
    candidates = c.fetchall()
    
    if not candidates:
        conn.close()
        return None
        
    winner_mobile = random.choice(candidates)[0]
    
    # Get Customer Details
    c.execute("SELECT name, mobile FROM customers WHERE mobile=?", (winner_mobile,))
    cust_row = c.fetchone()
    
    if cust_row:
        # Record Winner
        c.execute("INSERT INTO lucky_draw_history (date, winner_name, winner_mobile, prize) VALUES (?, ?, ?, ?)",
                  (datetime.now().strftime("%Y-%m-%d"), cust_row[0], cust_row[1], prize_desc))
        conn.commit()
        conn.close()
        return {"name": cust_row[0], "mobile": cust_row[1]}
    
    conn.close()
    return None

def get_lucky_draw_history():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM lucky_draw_history ORDER BY id DESC", conn)
    conn.close()
    return df

def add_product(name, category, price, stock, cost_price, expiry_date=None, image_data=None):
    conn = get_connection()
    c = conn.cursor()
    expiry_str = "NA"
    try:
        img_blob = sqlite3.Binary(image_data) if image_data else None
        c.execute("INSERT INTO products (name, category, price, stock, cost_price, sales_count, last_restock_date, expiry_date, image_data) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)",
                  (name, category, price, stock, cost_price, datetime.now().strftime("%Y-%m-%d"), expiry_str, img_blob))
        conn.commit()
        return True
    except Exception as e:
        print(e)
        return False
    finally:
        conn.close()

def update_product(p_id, name, category, price, stock, cost_price):
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE products SET name=?, category=?, price=?, stock=?, cost_price=? WHERE id=?",
              (name, category, price, stock, cost_price, p_id))
    conn.commit()
    conn.close()

def delete_product(p_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("DELETE FROM products WHERE id=?", (p_id,))
    conn.commit()
    conn.close()

def get_all_products():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    return df

def get_product_by_id(p_id):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM products WHERE id=?", (p_id,))
    row = c.fetchone()
    conn.close()
    if row:
        col_names = [description[0] for description in c.description]
        data = dict(zip(col_names, row))
        return data
    return None

def restock_product(p_id, quantity):
    if quantity <= 0: return False
    conn = get_connection()
    c = conn.cursor()
    c.execute("UPDATE products SET stock = stock + ?, last_restock_date = ? WHERE id=?",
              (quantity, datetime.now().strftime("%Y-%m-%d"), p_id))
    conn.commit()
    conn.close()

def get_sales_data():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM sales", conn)
    conn.close()
    return df

def seed_advanced_demo_data():
    """
    Generates realistic demo data DIRECTLY in the database.
    Does NOT use CSV files.
    Ensures data consistency and stock availability.
    """
    conn = get_connection()
    c = conn.cursor()

    # 1. Seed Categories if empty
    demo_categories = [
        "Snacks", "Beverages", "Grocery", "Dairy", "Bakery", 
        "Frozen", "Personal Care", "Stationery", "Electronics", "Household"
    ]
    for cat in demo_categories:
        c.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (cat,))

    # 2. Seed Products if empty
    c.execute("SELECT count(*) FROM products")
    if c.fetchone()[0] < 50:
        demo_products = {
            "Snacks": [("Lays Classic", 20, 15), ("Doritos Cheese", 30, 25), ("Pringles", 100, 80), ("Oreo", 40, 30), ("KitKat", 25, 18), ("Lays Chili", 20, 15), ("Cheetos", 25, 18), ("Popcorn", 50, 35), ("Pretzels", 60, 45), ("Biscuits", 30, 20)],
            "Beverages": [("Coke 500ml", 40, 30), ("Pepsi 500ml", 40, 30), ("Red Bull", 125, 90), ("Tropicana Juice", 110, 80), ("Water Bottle", 20, 10), ("Fanta", 40, 30), ("Sprite", 40, 30), ("Iced Tea", 60, 40), ("Cold Coffee", 80, 50), ("Lemonade", 30, 15)],
            "Grocery": [("Rice 1kg", 80, 60), ("Wheat Flour 1kg", 60, 45), ("Sugar 1kg", 50, 40), ("Salt", 20, 10), ("Cooking Oil 1L", 180, 150), ("Dal", 120, 90), ("Spices Pack", 200, 150), ("Pasta", 70, 50), ("Noodles", 20, 15), ("Ketchup", 90, 70)],
            "Dairy": [("Milk 1L", 60, 50), ("Cheese Slices", 120, 90), ("Butter 100g", 55, 45), ("Yogurt", 30, 20), ("Cream", 80, 60)],
            "Bakery": [("Bread", 40, 30), ("Bun", 20, 10), ("Croissant", 80, 50), ("Muffin", 50, 30), ("Cake Slice", 100, 60)],
            "Frozen": [("Frozen Peas", 90, 60), ("Ice Cream Tub", 250, 180), ("French Fries", 150, 100), ("Chicken Nuggets", 300, 220), ("Pizza", 200, 150)],
            "Personal Care": [("Shampoo", 200, 150), ("Soap", 40, 25), ("Toothpaste", 80, 60), ("Face Wash", 150, 100), ("Deodorant", 180, 120)],
            "Stationery": [("Notebook", 50, 30), ("Pen Set", 100, 70), ("Pencil Box", 80, 50), ("A4 Paper Rim", 300, 220), ("Stapler", 120, 80)],
            "Electronics": [("USB Cable", 150, 50), ("Earphones", 500, 300), ("Charger", 400, 200), ("Power Bank", 1200, 900), ("Mouse", 600, 400)],
            "Household": [("Detergent", 200, 160), ("Dish Soap", 80, 50), ("Sponge", 30, 10), ("Trash Bags", 100, 70), ("Air Freshener", 150, 100)]
        }
        for cat, items in demo_products.items():
            for name, price, cost in items:
                stock = random.randint(200, 500) 
                expiry = "NA"
                c.execute("INSERT INTO products (name, category, price, stock, cost_price, last_restock_date, expiry_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (name, cat, price, stock, cost, datetime.now().strftime("%Y-%m-%d"), expiry))
    
    # 3. Seed Users if empty
    demo_users = [
        ('admin', 'admin123', 'Admin', 'System Admin'),
        ('operator', 'pos123', 'Operator', 'POS Operator')
    ]
    for u, p, r, n in demo_users:
        ph = hashlib.sha256(p.encode()).hexdigest()
        c.execute("INSERT OR REPLACE INTO users (username, password_hash, role, full_name, status) VALUES (?, ?, ?, ?, 'Active')", (u, ph, r, n))

    # 4. Seed Sales and Customers if sales are low
    c.execute("SELECT count(*) FROM sales")
    sales_count = c.fetchone()[0]

    if sales_count < 10:
        c.execute("SELECT id, price FROM products")
        prods = c.fetchall()
        # Remove invalid products if any
        prods = [p for p in prods if p[0] is not None]
        
        # Consistent Customer List with Mandatory Names
        demo_customers = [
            ("+919876500001", "Amit Sharma", "amit.s@example.com"),
            ("+919876500002", "Priya Singh", "priya.s@example.com"),
            ("+919876500003", "Rahul Verma", "rahul.v@example.com"),
            ("+919876500004", "Vikram Malhotra", "vikram.m@example.com"),
            ("+919876500005", "Sneha Kapoor", "sneha.k@example.com"),
            ("+919876500006", "Arjun Das", "arjun.d@example.com"),
            ("+919876500007", "Riya Gupta", "riya.g@example.com"),
            ("+919876500008", "Karan Johar", "karan.j@example.com"),
            ("+919876500009", "Meera Reddy", "meera.r@example.com"),
            ("+919876500010", "Suresh Raina", "suresh.r@example.com"),
            ("+919876500011", "Anjali Mehta", "anjali.m@example.com"),
            ("+919876500012", "Kabir Singh", "kabir.s@example.com")
        ]
        
        # Seed Customers
        for mob, name, email in demo_customers:
            c.execute("INSERT OR IGNORE INTO customers (mobile, name, email, segment, visits, total_spend) VALUES (?, ?, ?, 'New', 0, 0)", 
                      (mob, name, email))
        
        if prods:
            modes = ["Cash", "UPI", "Card"]
            operators = ["admin", "operator"]
            
            cust_metrics = {mob: {'visits': 0, 'spend': 0} for mob, _, _ in demo_customers}
            
            # Generate 85 Sales
            for i in range(85):
                # Randomize time: Past 3 months
                days_ago = random.randint(0, 90)
                txn_time = (datetime.now() - timedelta(days=days_ago, hours=random.randint(9, 21), minutes=random.randint(0, 59))).strftime("%Y-%m-%d %H:%M:%S")
                
                # Select Customer
                cust_data = random.choice(demo_customers)
                cust_mob = cust_data[0]
                
                # Select Products
                num_items = random.randint(1, 6)
                chosen = random.choices(prods, k=num_items)
                
                # Ensure no empty items data
                item_ids = [str(x[0]) for x in chosen if x[0] is not None]
                if not item_ids: continue
                
                items_data_str = ",".join(item_ids)
                total = sum([x[1] for x in chosen])
                
                mode = random.choice(modes)
                op = random.choice(operators)
                
                # Determine Status (10% Cancelled)
                status = "Completed"
                cancel_reason = None
                cancelled_by = None
                cancel_time = None
                
                if random.random() < 0.10: # 10% Chance
                    status = "Cancelled"
                    cancel_reason = random.choice(["Customer changed mind", "Payment Failed", "Duplicate Order", "Item Issue"])
                    cancelled_by = op
                    cancel_time = txn_time 
                
                # Insert Sale
                c.execute("""INSERT INTO sales (timestamp, total_amount, items_data, integrity_hash, 
                            operator, payment_mode, time_taken, pos_id, customer_mobile, 
                            tax_amount, discount_amount, coupon_applied, status, 
                            cancellation_reason, cancelled_by, cancellation_timestamp) 
                            VALUES (?,?,?, 'DUMMY', ?, ?, 45, 'POS-1', ?, 0, 0, NULL, ?, ?, ?, ?)""",
                        (txn_time, total, items_data_str, op, mode, cust_mob, status, cancel_reason, cancelled_by, cancel_time))
                
                # Update Metrics if Completed
                if status == "Completed":
                    cust_metrics[cust_mob]['visits'] += 1
                    cust_metrics[cust_mob]['spend'] += total
                    
                    # Decrement Stock
                    for item in chosen:
                        c.execute("UPDATE products SET stock = stock - 1, sales_count = sales_count + 1 WHERE id=?", (item[0],))

            # Update Customer Tables with Calculated Metrics
            for mob, metrics in cust_metrics.items():
                spend = metrics['spend']
                visits = metrics['visits']
                
                if spend > 50000: segment = "High-Value"
                elif spend > 15000: segment = "Regular"
                else: segment = "Occasional"
                if visits == 0: segment = "New"

                c.execute("UPDATE customers SET visits=?, total_spend=?, segment=? WHERE mobile=?",
                          (visits, spend, segment, mob))

    conn.commit()
    conn.close()

def get_transaction_history(filters=None):
    # Added Left Join to get customer name/email/mobile for display
    query = """
        SELECT s.id, s.timestamp, s.total_amount, s.payment_mode, s.operator, 
               s.customer_mobile, s.status, s.pos_id, s.integrity_hash,
               c.name as customer_name, c.email as customer_email
        FROM sales s
        LEFT JOIN customers c ON s.customer_mobile = c.mobile
        WHERE 1=1
    """
    params = []
    
    if filters:
        if filters.get('bill_no'):
            query += " AND s.id = ?"
            params.append(filters['bill_no'])
        if filters.get('operator'):
            query += " AND s.operator LIKE ?"
            params.append(f"%{filters['operator']}%")
        if filters.get('date'):
            query += " AND s.timestamp LIKE ?"
            params.append(f"{filters['date']}%")
            
    query += " ORDER BY s.id DESC"
    
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn, params=params)
    except:
        df = pd.DataFrame()
    finally:
        conn.close()
    return df

def get_full_logs():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM logs ORDER BY id DESC", conn)
    conn.close()
    return df

def get_cancellation_audit_log():
    conn = get_connection()
    query = """SELECT id, timestamp, operator, cancellation_reason, cancelled_by, cancellation_timestamp 
               FROM sales WHERE status = 'Cancelled' ORDER BY id DESC"""
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_category_performance():
    conn = get_connection()
    # Using items_data as per schema
    sales = pd.read_sql("SELECT items_data, total_amount FROM sales WHERE status != 'Cancelled'", conn)
    products = pd.read_sql("SELECT id, category FROM products", conn)
    conn.close()
    
    cat_map = products.set_index('id')['category'].to_dict()
    cat_sales = {}
    
    for _, row in sales.iterrows():
        try:
            if row['items_data']:
                item_ids = [int(x) for x in str(row['items_data']).split(',') if x.strip()]
                if not item_ids: continue
                
                for iid in item_ids:
                    cat = cat_map.get(iid, "Unknown")
                    share = row['total_amount'] / len(item_ids) 
                    cat_sales[cat] = cat_sales.get(cat, 0) + share
        except: continue
        
    return pd.DataFrame(list(cat_sales.items()), columns=['Category', 'Revenue']).sort_values('Revenue', ascending=False)

def get_categories_list():
    """Fetches distinct categories for UI filters."""
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT name FROM categories")
    cats = [row[0] for row in c.fetchall()]
    conn.close()
    return cats

def add_category(name):
    """Adds a new category."""
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO categories (name) VALUES (?)", (name,))
        conn.commit()
        return True
    except:
        return False
    finally:
        conn.close()

def delete_category(name):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("DELETE FROM categories WHERE name=?", (name,))
        conn.commit()
        return True
    except:
        return False
    finally:
        conn.close()

def rename_category(old_name, new_name):
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("UPDATE categories SET name=? WHERE name=?", (new_name, old_name))
        conn.commit()
        return True
    except:
        return False
    finally:
        conn.close()
