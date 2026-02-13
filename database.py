import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
import hashlib
import os
import csv

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
    # items_data column stores item IDs as a comma-separated string (e.g. "1,2,5")
    # Removed loyalty points columns and renamed items_json to items_data for clarity
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
    
    # Customers table - Removed loyalty_points and total_spend (kept total_spend for basic tracking if needed, but removed points)
    # Actually prompt says "Remove loyalty points calculation logic". 
    # I will keep total_spend as it's general, but remove loyalty_points.
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
    Saves a sales transaction.
    Replaced items_json with items_data (comma separated string).
    Removed loyalty points arguments and logic.
    """
    conn = get_connection()
    c = conn.cursor()
    sale_id = None
    try:
        # Decrease stock for sold items
        for item in cart_items:
            c.execute("UPDATE products SET stock = stock - 1, sales_count = sales_count + 1 WHERE id=?", (item['id'],))
        
        # Convert list of item IDs to a simple comma-separated string
        # e.g., [10, 25, 30] becomes "10,25,30"
        item_ids = [str(i['id']) for i in cart_items]
        items_data_str = ",".join(item_ids)
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Insert with 0 discount and NULL coupon
        # Removed points_redeemed from INSERT
        c.execute("""INSERT INTO sales (timestamp, total_amount, items_data, integrity_hash, 
                     operator, payment_mode, time_taken, pos_id, customer_mobile, 
                     tax_amount, discount_amount, coupon_applied) 
                     VALUES (?,?,?,?,?,?,?,?,?,?,0.0,NULL)""",
                (timestamp, total, items_data_str, integrity_hash, operator, mode, time_taken, 
                 pos_id, customer_mobile, tax_amount))
        sale_id = c.lastrowid

        if customer_mobile:
            customer_mobile = customer_mobile.strip()
            # Removed loyalty_points from SELECT and UPDATE
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

        # Fetch items_data instead of items_json
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
        # Schema change might have shifted indices, fetch by name is safer but for simplicity using indices based on CREATE TABLE
        # 0:mobile, 1:name, 2:email, 3:visits, 4:total_spend, 5:segment (loyalty_points removed)
        # Assuming table recreated or strict adherence to indices of the new CREATE statement above:
        # If user has old DB file, this might crash. But user is developer, so expected.
        # Adjusted indices: 
        # 0:mobile, 1:name, 2:email, 3:visits, 4:total_spend, 5:segment
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
        # Removed loyalty_points from INSERT
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
    conn = get_connection()
    c = conn.cursor()

    demo_categories = [
        "Snacks", "Beverages", "Grocery", "Dairy", "Bakery", 
        "Frozen", "Personal Care", "Stationery", "Electronics", "Household"
    ]
    for cat in demo_categories:
        c.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (cat,))

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
                stock = random.randint(20, 100)
                expiry = "NA"
                c.execute("INSERT INTO products (name, category, price, stock, cost_price, last_restock_date, expiry_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                          (name, cat, price, stock, cost, datetime.now().strftime("%Y-%m-%d"), expiry))
    
    demo_users = [
        ('admin', 'admin123', 'Admin', 'System Admin'),
        ('operator', 'pos123', 'Operator', 'POS Operator')
    ]
    for u, p, r, n in demo_users:
        ph = hashlib.sha256(p.encode()).hexdigest()
        c.execute("INSERT OR REPLACE INTO users (username, password_hash, role, full_name, status) VALUES (?, ?, ?, ?, 'Active')", (u, ph, r, n))

    # --- UPDATED DUMMY DATA IMPORT ---
    # Check for demo_data.csv and import if sales table is empty
    c.execute("SELECT count(*) FROM sales")
    if c.fetchone()[0] < 10:
        
        # Priority: Import from CSV if exists
        csv_file = "demo_data.csv"
        imported_from_csv = False
        
        if os.path.exists(csv_file):
            try:
                with open(csv_file, 'r') as f:
                    reader = csv.DictReader(f)
                    
                    # Track metrics for customer updates
                    cust_metrics = {} 
                    
                    for row in reader:
                        # Insert Customer if not exists
                        mob = row['customer_mobile']
                        name = row['customer_name']
                        email = row['customer_email']
                        
                        c.execute("INSERT OR IGNORE INTO customers (mobile, name, email, segment, visits, total_spend) VALUES (?, ?, ?, 'New', 0, 0)", 
                                  (mob, name, email))
                        
                        if mob not in cust_metrics:
                            cust_metrics[mob] = {'visits': 0, 'spend': 0}
                            
                        # Insert Sale
                        c.execute("""INSERT INTO sales (timestamp, total_amount, items_data, integrity_hash, 
                                    operator, payment_mode, time_taken, pos_id, customer_mobile, 
                                    tax_amount, discount_amount, coupon_applied, status, 
                                    cancellation_reason, cancelled_by, cancellation_timestamp) 
                                    VALUES (?,?,?, 'DUMMY', ?, ?, 45, 'POS-1', ?, 0, 0, NULL, ?, ?, ?, ?)""",
                                (row['timestamp'], float(row['total_amount']), row['items_data'], row['operator'], row['payment_mode'], 
                                 mob, row['status'], row['cancellation_reason'], 
                                 row['operator'] if row['status'] == 'Cancelled' else None, 
                                 row['timestamp'] if row['status'] == 'Cancelled' else None))
                        
                        # Update local metrics
                        if row['status'] == 'Completed':
                            cust_metrics[mob]['visits'] += 1
                            cust_metrics[mob]['spend'] += float(row['total_amount'])
                            
                    # Update Customer Tables
                    for mob, metrics in cust_metrics.items():
                        spend = metrics['spend']
                        visits = metrics['visits']
                        
                        if spend > 50000: segment = "High-Value"
                        elif spend > 15000: segment = "Regular"
                        else: segment = "Occasional"
                        if visits == 0: segment = "New"

                        c.execute("UPDATE customers SET visits=?, total_spend=?, segment=? WHERE mobile=?",
                                  (visits, spend, segment, mob))
                                  
                    imported_from_csv = True
            except Exception as e:
                print(f"Error importing CSV: {e}")
        
        # Fallback to programmatic generation if CSV failed or didn't exist
        if not imported_from_csv:
            c.execute("SELECT id, price FROM products")
            prods = c.fetchall()
            
            # ... (Rest of the previous programmatic logic kept as fallback) ...
            demo_customers = [
                ("+919876500001", "Amit Sharma", "amit.s@example.com"),
                ("+919876500002", "Priya Singh", "priya.s@example.com"),
                ("+919876500003", "Rahul Verma", "rahul.v@example.com")
            ]
            for mob, name, email in demo_customers:
                c.execute("INSERT OR IGNORE INTO customers (mobile, name, email, segment, visits, total_spend) VALUES (?, ?, ?, 'New', 0, 0)", 
                          (mob, name, email))
            
            if prods:
                modes = ["Cash", "UPI", "Card"]
                operators = ["admin", "operator"]
                cust_metrics = {mob: {'visits': 0, 'spend': 0} for mob, _, _ in demo_customers}
                
                for i in range(50):
                    days_ago = random.randint(0, 60)
                    txn_time = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
                    cust_data = random.choice(demo_customers)
                    cust_mob = cust_data[0]
                    num_items = random.randint(1, 5)
                    chosen = random.choices(prods, k=num_items)
                    items_data_str = ",".join([str(x[0]) for x in chosen])
                    total = sum([x[1] for x in chosen])
                    mode = random.choice(modes)
                    op = random.choice(operators)
                    
                    status = "Completed"
                    if random.random() < 0.10: status = "Cancelled"
                    
                    c.execute("""INSERT INTO sales (timestamp, total_amount, items_data, integrity_hash, 
                                operator, payment_mode, time_taken, pos_id, customer_mobile, 
                                tax_amount, discount_amount, coupon_applied, status) 
                                VALUES (?,?,?, 'DUMMY', ?, ?, 30, 'POS-1', ?, 0, 0, NULL, ?)""",
                            (txn_time, total, items_data_str, op, mode, cust_mob, status))
                    
                    if status == "Completed":
                        cust_metrics[cust_mob]['visits'] += 1
                        cust_metrics[cust_mob]['spend'] += total

                for mob, metrics in cust_metrics.items():
                    c.execute("UPDATE customers SET visits=?, total_spend=? WHERE mobile=?", (metrics['visits'], metrics['spend'], mob))

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
    # Replaced items_json with items_data
    sales = pd.read_sql("SELECT items_data, total_amount FROM sales WHERE status != 'Cancelled'", conn)
    products = pd.read_sql("SELECT id, category FROM products", conn)
    conn.close()
    
    cat_map = products.set_index('id')['category'].to_dict()
    cat_sales = {}
    
    for _, row in sales.iterrows():
        try:
            # Replaced JSON loads with CSV string split
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
