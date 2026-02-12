import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
import hashlib
import json
import os

DB_NAME = "inventory_system.db"

def get_connection():
    """Returns a connection to the SQLite database."""
    return sqlite3.connect(DB_NAME, check_same_thread=False, timeout=30)

def init_db():
    """Initializes the database, tables, and seeds default data."""
    conn = get_connection()
    c = conn.cursor()
    
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
                  is_dead_stock TEXT DEFAULT 'False',
                  image_data BLOB)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS sales
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  timestamp TEXT, 
                  total_amount REAL, 
                  items_json TEXT, 
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
                  points_redeemed INTEGER DEFAULT 0,
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
                  
    c.execute('''CREATE TABLE IF NOT EXISTS active_sessions
                 (pos_id TEXT PRIMARY KEY, username TEXT, login_time TEXT, role TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS customers
                 (mobile TEXT PRIMARY KEY, 
                  name TEXT, 
                  email TEXT, 
                  visits INTEGER DEFAULT 0, 
                  total_spend REAL DEFAULT 0.0,
                  loyalty_points INTEGER DEFAULT 0,
                  segment TEXT DEFAULT 'New')''')

    c.execute('''CREATE TABLE IF NOT EXISTS terminals
                 (id TEXT PRIMARY KEY, 
                  name TEXT, 
                  location TEXT, 
                  status TEXT DEFAULT 'Active')''')

    c.execute('''CREATE TABLE IF NOT EXISTS coupons
                 (code TEXT PRIMARY KEY, 
                  discount_type TEXT, 
                  value REAL, 
                  min_bill REAL, 
                  valid_until TEXT, 
                  usage_limit INTEGER, 
                  used_count INTEGER DEFAULT 0,
                  bound_mobile TEXT)''')

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

    terminals = [
        ('POS-1', 'Main Counter', 'Entrance', 'Active')
    ]
    for t_id, t_name, t_loc, t_stat in terminals:
        c.execute("INSERT OR IGNORE INTO terminals (id, name, location, status) VALUES (?, ?, ?, ?)", (t_id, t_name, t_loc, t_stat))

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
                             tax_amount, discount_amount, coupon_code, points_redeemed, 
                             points_earned, integrity_hash, time_taken):
    conn = get_connection()
    c = conn.cursor()
    sale_id = None
    try:
        for item in cart_items:
            c.execute("UPDATE products SET stock = stock - 1, sales_count = sales_count + 1 WHERE id=?", (item['id'],))
        
        if coupon_code:
            c.execute("UPDATE coupons SET used_count = used_count + 1 WHERE code=?", (coupon_code,))

        items_json = json.dumps([i['id'] for i in cart_items])
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        c.execute("""INSERT INTO sales (timestamp, total_amount, items_json, integrity_hash, 
                     operator, payment_mode, time_taken, pos_id, customer_mobile, 
                     tax_amount, discount_amount, coupon_applied, points_redeemed) 
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (timestamp, total, items_json, integrity_hash, operator, mode, time_taken, 
                 pos_id, customer_mobile, tax_amount, discount_amount, coupon_code, points_redeemed))
        sale_id = c.lastrowid

        if customer_mobile:
            customer_mobile = customer_mobile.strip()
            c.execute("SELECT total_spend, loyalty_points FROM customers WHERE mobile=?", (customer_mobile,))
            res = c.fetchone()
            if res:
                curr_spend, curr_points = res
                new_spend = curr_spend + total
                
                new_seg = "New"
                if new_spend > 50000: new_seg = "High-Value"
                elif new_spend > 10000: new_seg = "Regular"
                else: new_seg = "Occasional"
                
                new_points = curr_points + points_earned - points_redeemed
                c.execute("""UPDATE customers SET visits = visits + 1, total_spend = ?, 
                             loyalty_points = ?, segment = ? WHERE mobile=?""", 
                          (new_spend, new_points, new_seg, customer_mobile))

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
        # 1. Password Verification
        ph = hashlib.sha256(password.encode()).hexdigest()
        c.execute("SELECT 1 FROM users WHERE username=? AND password_hash=?", (operator, ph))
        if not c.fetchone():
            return False, "Invalid Password. Identity verification failed."

        # 2. Get Sale Details
        c.execute("SELECT items_json, status, operator, total_amount, timestamp FROM sales WHERE id=?", (sale_id,))
        res = c.fetchone()
        if not res:
            return False, "Sale ID not found"
        
        items_json_str, status, sale_operator, total_amount, sale_timestamp_str = res
        
        if status == 'Cancelled':
            return False, "Sale already cancelled"
            
        # 3. Permission Check
        if role == 'Operator' and sale_operator != operator:
            return False, "Permission Denied: POS Operators can only cancel their own sales."

        # 4. Restore Inventory
        items_ids = json.loads(items_json_str)
        for pid in items_ids:
            c.execute("UPDATE products SET stock = stock + 1, sales_count = sales_count - 1 WHERE id=?", (pid,))
            
        # 5. Update Sale Record
        cancel_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""UPDATE sales SET status = 'Cancelled', cancellation_reason = ?, cancelled_by = ?, cancellation_timestamp = ? 
                     WHERE id=?""", (reason, operator, cancel_time, sale_id))
        
        # 6. Audit Log
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
        lp = row[5] if len(row) > 5 and row[5] is not None else 0
        seg = row[6] if len(row) > 6 and row[6] is not None else 'New'
        return {"mobile": row[0], "name": row[1], "email": row[2], "visits": row[3], "total_spend": row[4], "loyalty_points": lp, "segment": seg}
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
        c.execute("INSERT INTO customers (mobile, name, email, visits, total_spend, loyalty_points, segment) VALUES (?, ?, ?, 0, 0, 0, 'New')", (mobile, name, email))
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

def create_coupon(code, dtype, value, min_bill, days_valid, limit, bound_mobile=None):
    valid_until = (datetime.now() + timedelta(days=days_valid)).strftime("%Y-%m-%d")
    conn = get_connection()
    c = conn.cursor()
    try:
        c.execute("INSERT INTO coupons (code, discount_type, value, min_bill, valid_until, usage_limit, bound_mobile) VALUES (?, ?, ?, ?, ?, ?, ?)",
                  (code, dtype, value, min_bill, valid_until, limit, bound_mobile))
        conn.commit()
        return True
    except: return False
    finally: conn.close()

def get_coupon(code, customer_mobile=None):
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM coupons WHERE code=?", (code,))
    c_data = c.fetchone()
    conn.close()
    if c_data:
        # columns: 0:code, 1:type, 2:value, 3:min, 4:expiry, 5:limit, 6:used, 7:bound_mobile
        expiry = c_data[4]
        limit = c_data[5]
        used = c_data[6]
        bound_mobile = c_data[7] if len(c_data) > 7 else None
        
        if datetime.now() > datetime.strptime(expiry, "%Y-%m-%d"):
            return None, "Expired"
        if used >= limit:
            return None, "Usage Limit Reached"
        
        if bound_mobile and bound_mobile != 'None':
             if not customer_mobile:
                 return None, "Customer identification required for this coupon"
             if bound_mobile.strip() != customer_mobile.strip():
                 return None, "Coupon not valid for this customer"
        
        return {
            "code": c_data[0], "type": c_data[1], "value": c_data[2],
            "min_bill": c_data[3], "bound_mobile": bound_mobile, "expiry": expiry
        }, "Valid"
    return None, "Invalid Code"

def get_customer_coupons(mobile):
    if not mobile: return pd.DataFrame()
    conn = get_connection()
    now_str = datetime.now().strftime("%Y-%m-%d")
    query = """
    SELECT code, discount_type, value, min_bill, valid_until 
    FROM coupons 
    WHERE bound_mobile = ? AND valid_until >= ? AND used_count < usage_limit
    """
    df = pd.read_sql(query, conn, params=(mobile, now_str))
    conn.close()
    return df

def get_all_coupons():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM coupons", conn)
    conn.close()
    return df

def add_product(name, category, price, stock, cost_price, expiry_date=None, image_data=None):
    conn = get_connection()
    c = conn.cursor()
    
    # Expiry ignored/simplified
    expiry_str = "NA"

    try:
        img_blob = sqlite3.Binary(image_data) if image_data else None
        c.execute("INSERT INTO products (name, category, price, stock, cost_price, sales_count, last_restock_date, expiry_date, is_dead_stock, image_data) VALUES (?, ?, ?, ?, ?, 0, ?, ?, 'False', ?)",
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

def toggle_dead_stock(p_id, is_dead):
    conn = get_connection()
    c = conn.cursor()
    val = 'True' if is_dead else 'False'
    c.execute("UPDATE products SET is_dead_stock=? WHERE id=?", (val, p_id))
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
                # No Expiry
                expiry = "NA"
                c.execute("INSERT INTO products (name, category, price, stock, cost_price, last_restock_date, expiry_date, is_dead_stock) VALUES (?, ?, ?, ?, ?, ?, ?, 'False')",
                          (name, cat, price, stock, cost, datetime.now().strftime("%Y-%m-%d"), expiry))
    
    demo_users = [
        ('admin', 'admin123', 'Admin', 'System Admin'),
        ('operator', 'pos123', 'Operator', 'POS Operator')
    ]
    for u, p, r, n in demo_users:
        ph = hashlib.sha256(p.encode()).hexdigest()
        c.execute("INSERT OR REPLACE INTO users (username, password_hash, role, full_name, status) VALUES (?, ?, ?, ?, 'Active')", (u, ph, r, n))

    demo_customers = [
        ("9876500001", "Amit Sharma", "amit.s@example.com", "Regular"),
        ("9876500002", "Priya Singh", "priya.s@example.com", "High-Value"),
        ("9876500003", "Rahul Verma", "rahul.v@example.com", "Occasional")
    ]
    for mob, name, email, seg in demo_customers:
        c.execute("INSERT OR IGNORE INTO customers (mobile, name, email, segment, visits, total_spend, loyalty_points) VALUES (?, ?, ?, ?, 0, 0, 0)", 
                  (mob, name, email, seg))

    conn.commit()
    conn.close()

def get_transaction_history(filters=None):
    query = "SELECT id, timestamp, total_amount, payment_mode, operator, customer_mobile, status, pos_id, integrity_hash FROM sales WHERE 1=1"
    params = []
    
    if filters:
        if filters.get('bill_no'):
            query += " AND id = ?"
            params.append(filters['bill_no'])
        if filters.get('operator'):
            query += " AND operator LIKE ?"
            params.append(f"%{filters['operator']}%")
        if filters.get('date'):
            query += " AND timestamp LIKE ?"
            params.append(f"{filters['date']}%")
            
    query += " ORDER BY id DESC"
    
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

def get_category_performance():
    conn = get_connection()
    sales = pd.read_sql("SELECT items_json, total_amount FROM sales WHERE status != 'Cancelled'", conn)
    products = pd.read_sql("SELECT id, category FROM products", conn)
    conn.close()
    
    cat_map = products.set_index('id')['category'].to_dict()
    cat_sales = {}
    
    for _, row in sales.iterrows():
        try:
            item_ids = json.loads(row['items_json'])
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
