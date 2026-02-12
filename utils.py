import hashlib
import time
import math
import random
import pandas as pd
import numpy as np
import qrcode
from io import BytesIO
from fpdf import FPDF
import urllib.parse
from datetime import datetime, timedelta
import shutil
import os
import json
from PIL import Image
import re

# --- SYSTEM TIME HELPER ---
def get_system_time():
    """Returns current system time. Useful for centralized time sync."""
    return datetime.now()

# --- SECURITY: PASSWORD STRENGTH ---
def check_password_strength(password):
    """
    Validates password strength.
    Returns: (score [0-4], label, color)
    """
    score = 0
    if len(password) >= 8: score += 1
    if re.search(r"[A-Z]", password): score += 1
    if re.search(r"[a-z]", password): score += 1
    if re.search(r"\d", password) or re.search(r"[ !@#$%^&*()_+\-=\[\]{};':\"\\|,.<>\/?]", password): score += 1
    
    if score == 0: return 0, "Very Weak", "#ef4444"
    elif score == 1: return 1, "Weak", "#ef4444"
    elif score == 2: return 2, "Medium", "#f59e0b"
    elif score == 3: return 3, "Strong", "#10b981"
    elif score == 4: return 4, "Very Strong", "#059669"
    return 0, "Unknown", "#ef4444"

# --- EMAIL VALIDATION ---
def validate_email(email):
    """
    Validates email format using Regex.
    """
    if not email: return False
    # Basic format: chars + @ + chars + . + chars
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return re.match(pattern, email) is not None

# --- MOBILE VALIDATION (COUNTRY SPECIFIC) ---
def validate_mobile_number(number_str, country_code):
    """
    Validates mobile number based on country specific rules.
    Returns: (is_valid, normalized_number, message)
    """
    if not number_str or not number_str.isdigit():
        return False, None, "Mobile number must contain digits only."
        
    clean_num = number_str.strip()
    
    if country_code == "+91": # India
        if len(clean_num) != 10:
            return False, None, "India (+91) numbers must be exactly 10 digits."
        if int(clean_num[0]) < 6:
            return False, None, "India (+91) numbers must start with 6, 7, 8, or 9."
            
    elif country_code == "+971": # UAE
        if len(clean_num) != 9:
            return False, None, "UAE (+971) numbers must be exactly 9 digits."
        if not clean_num.startswith("5"):
            return False, None, "UAE (+971) numbers must start with 5."
            
    elif country_code == "+965": # Kuwait
        if len(clean_num) != 8:
            return False, None, "Kuwait (+965) numbers must be exactly 8 digits."
        if clean_num[0] not in ['5', '6', '9']:
            return False, None, "Kuwait (+965) numbers must start with 5, 6, or 9."
            
    elif country_code == "+966": # Saudi Arabia
        if len(clean_num) != 9:
            return False, None, "Saudi Arabia (+966) numbers must be exactly 9 digits."
        if not clean_num.startswith("5"):
            return False, None, "Saudi Arabia (+966) numbers must start with 5."
            
    elif country_code == "+1": # USA
        if len(clean_num) != 10:
            return False, None, "USA (+1) numbers must be exactly 10 digits."
        if clean_num[0] in ['0', '1']:
            return False, None, "USA (+1) area code cannot start with 0 or 1."
            
    # E.164 Normalization
    normalized = f"{country_code}{clean_num}"
    return True, normalized, "Valid"

# --- LOYALTY ---
def calculate_loyalty_points(amount):
    return int(amount // 100)

def get_sound_html(sound_type):
    if sound_type == 'success':
        src = "https://www.soundjay.com/buttons/sounds/button-3.mp3"
    elif sound_type == 'error':
        src = "https://www.soundjay.com/buttons/sounds/button-10.mp3"
    elif sound_type == 'celebration':
        # Short cheer/tada sound
        src = "https://www.soundjay.com/human/sounds/applause-01.mp3"
    else: 
        src = "https://www.soundjay.com/buttons/sounds/button-16.mp3"
        
    return f"""
    <audio autoplay>
        <source src="{src}" type="audio/mpeg">
    </audio>
    """

def generate_hash(data_string):
    return hashlib.sha256(data_string.encode()).hexdigest()

def generate_integrity_hash(txn_data):
    raw_string = f"{txn_data[0]}|{txn_data[1]}|{txn_data[2]}|{txn_data[3]}"
    return hashlib.sha256(raw_string.encode()).hexdigest()

# --- TRIE ---
class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end_of_word = False
        self.data = None

class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, word, data):
        node = self.root
        for char in word.lower():
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end_of_word = True
        node.data = data

    def search_prefix(self, prefix):
        node = self.root
        for char in prefix.lower():
            if char not in node.children:
                return []
            node = node.children[char]
        return self._collect_words(node)

    def _collect_words(self, node):
        results = []
        if node.is_end_of_word:
            results.append(node.data)
        for child in node.children.values():
            results.extend(self._collect_words(child))
        return results

def linear_search(data_list, key, value):
    for item in data_list:
        if str(item.get(key)).lower() == str(value).lower():
            return item
    return None

def binary_search(sorted_list, key, value):
    low = 0
    high = len(sorted_list) - 1
    
    while low <= high:
        mid = (low + high) // 2
        mid_val = sorted_list[mid].get(key)
        
        if mid_val < value:
            low = mid + 1
        elif mid_val > value:
            high = mid - 1
        else:
            return sorted_list[mid]
    return None

def calculate_inventory_metrics(df_sales, df_products):
    # Simplified metrics
    if 'status' in df_sales.columns:
        df_sales = df_sales[df_sales['status'] != 'Cancelled']

    metrics = []
    for _, prod in df_products.iterrows():
        annual_demand = prod['sales_count'] * 12 if prod['sales_count'] > 0 else 10 
        
        metrics.append({
            "name": prod['name'],
            "stock": prod['stock'],
            "annual_demand_est": annual_demand,
        })
    return pd.DataFrame(metrics)

def forecast_next_period(sales_array, window=5):
    if len(sales_array) < window:
        return np.mean(sales_array) if len(sales_array) > 0 else 0
        
    recent = sales_array[-window:]
    weights = np.arange(1, len(recent) + 1)
    return np.dot(recent, weights) / weights.sum()

def analyze_trend_slope(sales_series):
    if len(sales_series) < 2: return "Stable"
    x = np.arange(len(sales_series))
    y = np.array(sales_series)
    slope, _ = np.polyfit(x, y, 1)
    
    if slope > 0.5: return "↗️ Increasing"
    elif slope < -0.5: return "↘️ Decreasing"
    else: return "➡️ Stable"

def rank_products(df_sales, df_products):
    if df_sales.empty: return pd.DataFrame()
    
    if 'status' in df_sales.columns:
        active_sales = df_sales[df_sales['status'] != 'Cancelled']
    else:
        active_sales = df_sales

    import json
    all_items = []
    for _, row in active_sales.iterrows():
        try:
            ids = json.loads(row['items_json'])
            all_items.extend(ids)
        except: continue
        
    from collections import Counter
    counts = Counter(all_items)
    
    ranking_data = []
    for _, prod in df_products.iterrows():
        qty = counts.get(prod['id'], 0)
        rev = qty * prod['price']
        ranking_data.append({
            "name": prod['name'],
            "qty_sold": qty,
            "revenue": rev,
            "score": (qty * 10) + (rev * 0.01) 
        })
        
    ranking_data.sort(key=lambda x: x['score'], reverse=True)
    
    for i, item in enumerate(ranking_data):
        if i == 0: item['rank'] = "🥇 Top Seller"
        elif i < len(ranking_data)/2: item['rank'] = "🥈 Average Performer"
        else: item['rank'] = "🥉 Low Performer"
        
    return pd.DataFrame(ranking_data)

def get_product_performance_lists(df_sales, df_products):
    if df_sales.empty: return [], [], []
    
    df_rank = rank_products(df_sales, df_products)
    if df_rank.empty: return [], [], []
    
    # Logic: High = Top 10% sold, Low = Bottom 10% sold, Star = High Rev
    high = df_rank.head(5)['name'].tolist()
    low = df_rank.tail(5)['name'].tolist()
    
    df_rank = df_rank.sort_values('revenue', ascending=False)
    star = df_rank.head(3)['name'].tolist()
    
    return high, low, star

# --- PROFIT & LOSS ANALYSIS ---
def calculate_profit_loss(df_sales, df_products):
    """
    Calculates P&L for Enhanced Statement.
    Gross Revenue = Sum of List Prices of sold items
    Marketing Expense = 0 (Fixed Rule)
    Net Revenue = Gross Revenue
    Profit = Net Revenue - COGS
    """
    if df_sales.empty or df_products.empty:
        return {"net_profit": 0, "total_revenue": 0, "total_cost": 0, "margin_percent": 0, "marketing_expense": 0, "net_revenue": 0}, pd.DataFrame()

    if 'status' in df_sales.columns:
        active_sales = df_sales[df_sales['status'] != 'Cancelled']
    else:
        active_sales = df_sales

    # Marketing Expense must be 0
    marketing_expense = 0

    prod_map = df_products.set_index('id')[['name', 'category', 'cost_price', 'price']].to_dict('index')
    
    category_pl = {}
    gross_rev = 0
    total_cost = 0

    for _, row in active_sales.iterrows():
        try:
            items = json.loads(row['items_json'])
            for pid in items:
                if pid in prod_map:
                    p = prod_map[pid]
                    cp = p['cost_price']
                    sp = p['price']
                    
                    gross_rev += sp # Gross is sum of list prices
                    total_cost += cp
                    
                    # Category breakdown
                    profit_gross = sp - cp
                    
                    cat = p['category']
                    if cat not in category_pl:
                        category_pl[cat] = {'revenue': 0, 'cost': 0, 'profit': 0}
                    category_pl[cat]['revenue'] += sp
                    category_pl[cat]['cost'] += cp
                    category_pl[cat]['profit'] += profit_gross
                    
        except: continue

    # Net Revenue is Gross Revenue (since marketing is 0)
    net_revenue = gross_rev 
    net_profit = net_revenue - total_cost
    
    pl_data = []
    for cat, metrics in category_pl.items():
        pl_data.append({
            "Category": cat,
            "Revenue": metrics['revenue'],
            "Cost": metrics['cost'],
            "Profit": metrics['profit'], 
            "Margin %": (metrics['profit'] / metrics['revenue'] * 100) if metrics['revenue'] > 0 else 0
        })
    
    return {
        "net_profit": net_profit, 
        "total_revenue": gross_rev, 
        "net_revenue": net_revenue, 
        "total_cost": total_cost,
        "marketing_expense": marketing_expense,
        "margin_percent": (net_profit / net_revenue * 100) if net_revenue > 0 else 0
    }, pd.DataFrame(pl_data)

def backup_system():
    if not os.path.exists("backups"): os.makedirs("backups")
    fname = f"backups/inventory_backup_{int(time.time())}.db"
    try:
        shutil.copy("inventory_system.db", fname)
        return fname
    except Exception as e:
        return None

class PDFReceipt(FPDF):
    def __init__(self, store_name, logo_path=None):
        super().__init__()
        self.store_name = store_name
        self.logo_path = logo_path

    def header(self):
        if self.logo_path and os.path.exists(self.logo_path):
            try:
                self.image(self.logo_path, 10, 8, 25)
                self.set_xy(40, 10)
            except: pass
        
        self.set_font('Arial', 'B', 15)
        self.cell(0, 10, self.store_name, 0, 1, 'C')
        self.set_font('Arial', '', 9)
        self.cell(0, 5, 'Retail & POS System', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

def generate_receipt_pdf(store_name, txn_id, time_str, items, total, operator, mode, pos, customer=None, tax_info=None, new_coupon=None):
    logo_path = "logo.png" if os.path.exists("logo.png") else None
    
    pdf = PDFReceipt(store_name, logo_path)
    pdf.add_page()
    
    pdf.set_font("Arial", size=10)
    
    def clean_text(text):
        if not text: return ""
        text = str(text)
        text = text.replace("₹", "Rs. ")
        return text.encode('latin-1', 'replace').decode('latin-1')

    pdf.cell(100, 6, clean_text(f"Receipt No: #{txn_id}"), 0, 0)
    pdf.cell(0, 6, clean_text(f"Date: {time_str}"), 0, 1, 'R')
    pdf.cell(100, 6, clean_text(f"Cashier: {operator}"), 0, 0)
    pdf.cell(0, 6, clean_text(f"POS: {pos}"), 0, 1, 'R')
    pdf.cell(100, 6, clean_text(f"Payment Mode: {mode}"), 0, 1, 'L')
    
    if customer:
        pdf.ln(5)
        pdf.set_font("Arial", 'B', 10)
        pdf.cell(0, 6, "Customer Details:", 0, 1, 'L')
        pdf.set_font("Arial", '', 10)
        pdf.cell(0, 5, clean_text(f"Name: {customer.get('name', 'N/A')}"), 0, 1)
        pdf.cell(0, 5, clean_text(f"Mobile: {customer.get('mobile', 'N/A')}"), 0, 1)

    pdf.ln(5)
    
    pdf.set_fill_color(220, 220, 220)
    pdf.set_font("Arial", 'B', 10)
    pdf.cell(100, 8, "Item", 1, 0, 'L', True)
    pdf.cell(30, 8, "Price", 1, 0, 'C', True)
    pdf.cell(20, 8, "Qty", 1, 0, 'C', True)
    pdf.cell(40, 8, "Total", 1, 1, 'R', True)
    
    pdf.set_font("Arial", '', 10)
    item_summary = {}
    for i in items:
        if i['name'] in item_summary:
            item_summary[i['name']]['qty'] += 1
            item_summary[i['name']]['total'] += i['price']
        else:
            item_summary[i['name']] = {'price': i['price'], 'qty': 1, 'total': i['price']}
            
    for name, data in item_summary.items():
        pdf.cell(100, 7, clean_text(name), 1)
        pdf.cell(30, 7, f"{data['price']:.2f}", 1, 0, 'C')
        pdf.cell(20, 7, str(data['qty']), 1, 0, 'C')
        pdf.cell(40, 7, f"{data['total']:.2f}", 1, 1, 'R')
        
    pdf.ln(5)
    pdf.set_font("Arial", '', 10)
    
    if tax_info and tax_info.get('tax_amount', 0) > 0:
        pdf.cell(150, 6, f"GST ({tax_info['tax_percent']}%)", 0, 0, 'R')
        pdf.cell(40, 6, f"{tax_info['tax_amount']:.2f}", 1, 1, 'R')

    pdf.set_font("Arial", 'B', 12)
    pdf.cell(150, 8, "NET TOTAL", 0, 0, 'R')
    pdf.cell(40, 8, clean_text(f"Rs. {total:.2f}"), 1, 1, 'R')
    
    pdf.set_font("Arial", '', 9)
    pdf.ln(10)
    pdf.cell(0, 5, clean_text(f"Payment Mode: {mode}"), 0, 1, 'L')
    pdf.cell(0, 5, "Terms: Non-refundable. Goods once sold cannot be returned.", 0, 1, 'C')
    
    return pdf.output(dest='S').encode('latin-1')

def generate_upi_qr(vpa, name, amount, note):
    if not name: name = "Merchant"
    
    params = {
        "pa": vpa, 
        "pn": name, 
        "am": f"{amount:.2f}", 
        "cu": "INR", 
        "tn": note,
        "mc": "0000", 
        "mode": "02", 
        "orgid": "000000" 
    }
    url = f"upi://pay?{urllib.parse.urlencode(params)}"
    qr = qrcode.QRCode(box_size=10, border=4)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = BytesIO()
    img.save(buf)
    return buf.getvalue()

def validate_card(number, expiry, cvv):
    if not number.isdigit() or not (13 <= len(number) <= 19):
        return False, "Invalid Card Number Length (13-19 digits required)"
    
    if not cvv.isdigit() or not (3 <= len(cvv) <= 4):
        return False, "Invalid CVV (3-4 digits required)"
    
    try:
        if "/" not in expiry: return False, "Invalid Expiry Format (Use MM/YY)"
        exp_m, exp_y = map(int, expiry.split('/'))
        if not (1 <= exp_m <= 12): return False, "Invalid Month"
        current_year = int(datetime.now().strftime("%y"))
        if exp_y < current_year: return False, "Card Expired"
    except:
        return False, "Invalid Expiry Date"

    digits = [int(d) for d in number]
    checksum = digits.pop()
    digits.reverse()
    doubled = []
    for i, d in enumerate(digits):
        if i % 2 == 0:
            d *= 2
            if d > 9: d -= 9
        doubled.append(d)
    total = sum(doubled) + checksum
    
    if total % 10 == 0:
        return True, "Valid"
    else:
        return False, "Invalid Card Number (Luhn Check Failed)"
