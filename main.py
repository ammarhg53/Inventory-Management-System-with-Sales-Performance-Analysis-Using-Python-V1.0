import streamlit as st
import pandas as pd
import json
import time
import random
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
import os

# Internal modules
import database as db
import utils
import styles

# --- APP CONFIGURATION ---
st.set_page_config(
    page_title="POS System", 
    layout="wide", 
    page_icon="🏢",
    initial_sidebar_state="expanded"
)

if 'theme' not in st.session_state:
    st.session_state['theme'] = 'dark'

styles.load_css(st.session_state['theme'])

# --- INITIALIZATION ---
if 'initialized' not in st.session_state:
    db.init_db()
    db.seed_advanced_demo_data() 
    st.session_state['initialized'] = True
    st.session_state['cart'] = []
    st.session_state['user'] = None
    st.session_state['role'] = None
    st.session_state['full_name'] = None
    st.session_state['pos_id'] = "POS-1" # Default single terminal
    st.session_state['checkout_stage'] = 'cart'
    st.session_state['txn_start_time'] = None
    st.session_state['qr_expiry'] = None
    st.session_state['selected_payment_mode'] = None
    st.session_state['undo_stack'] = [] # Kept for logic safety but removed from UI
    st.session_state['redo_stack'] = []
    st.session_state['current_customer'] = None
    st.session_state['bill_mode'] = None
    st.session_state['applied_coupon'] = None
    st.session_state['points_to_redeem'] = 0

    # Session State for Forms
    st.session_state['clear_inventory_form'] = False

# Load Configs
currency = db.get_setting("currency_symbol")
store_name = db.get_setting("store_name")

# --- HELPER FUNCTIONS ---
def refresh_trie():
    conn = db.get_connection()
    df = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    t = utils.Trie()
    for _, row in df.iterrows():
        t.insert(row['name'], row.to_dict())
    return t, df

if 'product_trie' not in st.session_state:
    trie, df_p = refresh_trie()
    st.session_state['product_trie'] = trie
    st.session_state['df_products'] = df_p

# --- AUTHENTICATION MODULE ---
def login_view():
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown(f"""
    <div style="text-align: center; margin-bottom: 30px;">
        <div style="font-size: 4rem; margin-bottom: 10px;">💠</div>
        <h1 style="margin-bottom: 0; font-size: 2.5rem;">{store_name}</h1>
        <p style="opacity: 0.6; font-size: 1.1rem; letter-spacing: 1px;">PROFESSIONAL POS SYSTEM</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<div class='login-box' style='margin: 0 auto;'>", unsafe_allow_html=True)
    st.subheader("🔐 Secure Access")
    
    with st.form("login_frm"):
        user_in = st.text_input("Enter Username", placeholder="e.g. admin").strip().lower()
        pass_in = st.text_input("Enter Password", type="password", placeholder="••••••••")
        
        st.markdown("<br>", unsafe_allow_html=True)
        submit = st.form_submit_button("Access System", type="primary", use_container_width=True)
        
        if submit:
            if not user_in or not pass_in:
                st.error("Fields cannot be empty")
                return
            
            try:
                u_status = db.get_user_status(user_in)
                if u_status != "Active":
                    st.error(f"❌ Account is {u_status}. Contact Admin.")
                    return
            except AttributeError:
                pass

            h = utils.generate_hash(pass_in)
            conn = db.get_connection()
            c = conn.cursor()
            c.execute("SELECT role, full_name FROM users WHERE username=? AND password_hash=?", (user_in, h))
            res = c.fetchone()
            conn.close()
            
            if res:
                role, fname = res
                
                st.session_state['user'] = user_in
                st.session_state['role'] = role
                st.session_state['full_name'] = fname
                st.session_state['pos_id'] = "POS-1" # Default terminal
                
                db.log_activity(user_in, "Login", "Accessed System")
                st.success("Login Successful! Redirecting...")
                time.sleep(0.5)
                st.rerun()
            else:
                st.error("❌ Invalid Username or Password")
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("🔑 View Demo Credentials"):
        st.code("""
User: admin    | Pass: admin123   (Admin)
User: operator | Pass: pos123     (Operator)
        """, language="text")

def logout_user():
    user = st.session_state.get('user')
    if user:
        db.log_activity(user, "Logout", "Session Ended")
    
    st.session_state.clear()
    st.session_state['theme'] = 'dark' 
    st.rerun()

# --- MODULES ---

def pos_interface():
    st.markdown("<div class='card-container'>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([3, 1, 1])
    with c1:
        st.title(f"🛒 POS Terminal")
        st.caption("Sales & Billing • Live")
    with c3:
        st.markdown(f"<div style='text-align:right'><b>{st.session_state['full_name']}</b><br><span style='font-size:0.8em;opacity:0.7'>Operator</span></div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)
        
    trie, df_p = refresh_trie()
    
    # --- STATE MACHINE: CART VIEW ---
    if st.session_state['checkout_stage'] == 'cart':
        
        with st.expander("👤 Customer Details (Required for Bill)", expanded=st.session_state['current_customer'] is None):
            col_cc, col_mob, col_btn = st.columns([1, 2, 1])
            with col_cc:
                country_codes = ["+91", "+1", "+44", "+971", "+966", "+965"]
                country_code = st.selectbox("Code", country_codes, index=0)
            with col_mob:
                cust_phone_input = st.text_input("Mobile Number", placeholder="e.g. 9876543210").strip()
            with col_btn:
                st.write("")
                st.write("")
                if st.button("🔎 Search / Add"):
                    is_valid, normalized_phone, msg = utils.validate_mobile_number(cust_phone_input, country_code)
                    
                    if not is_valid:
                        st.error(msg)
                    else:
                        cust = db.get_customer(normalized_phone)
                        # Remove legacy fallback search by just digits
                        
                        if cust:
                            st.session_state['current_customer'] = cust
                            st.success(f"Welcome back, {cust['name']}")
                        else:
                            st.session_state['temp_new_customer'] = normalized_phone
                            st.warning(f"New Customer: {normalized_phone}")
            
            if st.session_state.get('temp_new_customer') and not st.session_state.get('current_customer'):
                st.markdown("##### 📝 New Customer Details")
                with st.form("new_cust_form"):
                    new_name = st.text_input("Customer Name (Mandatory)")
                    new_email = st.text_input("Email ID (Optional)")
                    
                    if st.form_submit_button("Save Customer"):
                        if not new_name:
                            st.error("Customer Name is mandatory.")
                        else:
                            # Validate Email only if entered
                            if new_email and not utils.validate_email(new_email):
                                st.error("Please enter a valid email address.")
                            else:
                                # Save Name and Email only (using mobile as ID from search)
                                db.upsert_customer(st.session_state['temp_new_customer'], new_name, new_email)
                                st.session_state['current_customer'] = db.get_customer(st.session_state['temp_new_customer'])
                                st.session_state.pop('temp_new_customer', None)
                                st.success("Customer Added Successfully!")
                                st.rerun()
            
            if st.session_state.get('current_customer'):
                st.info(f"Selected: {st.session_state['current_customer']['name']} ({st.session_state['current_customer']['mobile']})")

        st.markdown("---")
        
        col_manual = st.columns([1])[0]
        with col_manual:
            st.markdown("##### ⌨️ Manual Search")
            c_search, c_algo = st.columns([3, 1])
            with c_search:
                query = st.text_input("Search Product", key="pos_search")
            with c_algo:
                # Algo text simplified for user
                algo = st.selectbox("Search Mode", ["Standard", "Legacy"])

        left_panel, right_panel = st.columns([2, 1])

        with left_panel:
            results = []
            if query:
                if algo == "Standard":
                    results = trie.search_prefix(query)
                else:
                    results = df_p[df_p['name'].str.contains(query, case=False)].to_dict('records')
            else:
                results = df_p.to_dict('records')
            
            page_size = 6
            if 'page' not in st.session_state: st.session_state.page = 0
            start_idx = st.session_state.page * page_size
            end_idx = start_idx + page_size
            visible_items = results[start_idx:end_idx]
            
            cols = st.columns(3)
            for i, item in enumerate(visible_items):
                with cols[i % 3]:
                    st.markdown(styles.product_card_html(
                        item['name'], item['price'], item['stock'], item['category'], currency, item.get('image_data')
                    ), unsafe_allow_html=True)
                    
                    cart_qty = sum(1 for x in st.session_state['cart'] if x['id'] == item['id'])
                    
                    if item['stock'] > cart_qty:
                        if st.button("Add ➕", key=f"add_{item['id']}"):
                            st.session_state['cart'].append(item)
                            st.toast(f"Added {item['name']}")
                            st.rerun()
                    else:
                        st.button("🚫 Out of Stock", disabled=True, key=f"no_{item['id']}")
            
            c_prev, c_next = st.columns([1,1])
            if c_prev.button("Previous") and st.session_state.page > 0:
                st.session_state.page -= 1
                st.rerun()
            if c_next.button("Next") and end_idx < len(results):
                st.session_state.page += 1
                st.rerun()

        with right_panel:
            st.markdown("<div class='card-container'>", unsafe_allow_html=True)
            st.markdown("### 🛍️ Cart Summary")
            if st.session_state['cart']:
                cart_df = pd.DataFrame(st.session_state['cart'])
                summary = cart_df.groupby('name').agg({'price': 'first', 'id': 'count'}).rename(columns={'id': 'Qty'})
                summary['Total'] = summary['price'] * summary['Qty']
                st.dataframe(summary[['Qty', 'Total']], use_container_width=True)
                
                raw_total = summary['Total'].sum()
                
                # No Loyalty, No Coupons
                discount = 0
                fest_disc = 0 
                
                # No Points Redemption
                total_after_disc = max(0, raw_total - discount - fest_disc)
                
                gst_enabled = db.get_setting("gst_enabled") == 'True'
                tax_amount = 0.0
                if gst_enabled:
                    tax_rate = float(db.get_setting("tax_rate"))
                    tax_amount = total_after_disc * (tax_rate / 100)
                    st.write(f"GST ({tax_rate}%): {currency}{tax_amount:,.2f}")
                
                final_total = total_after_disc + tax_amount
                
                st.markdown(f"""
                <div style='background:var(--secondary-bg); padding:10px; border-radius:8px; margin-top:10px;'>
                    <div style='display:flex; justify-content:space-between'><span>Subtotal:</span><span>{currency}{raw_total:.2f}</span></div>
                    <div style='display:flex; justify-content:space-between; font-weight:bold; font-size:1.2em; margin-top:5px; border-top:1px solid var(--border-color); padding-top:5px;'>
                        <span>Total:</span><span>{currency}{final_total:,.2f}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown("---")
                c_clear, c_pay = st.columns([1, 2])
                
                with c_clear:
                    if st.button("🗑️ Clear", use_container_width=True):
                        st.session_state['cart'] = []
                        st.rerun()
                
                with c_pay:
                    if st.button("💳 Pay", type="primary", use_container_width=True):
                        if not st.session_state['current_customer']:
                            st.error("Please add Customer Details first!")
                        else:
                            st.session_state['final_calc'] = {
                                "total": final_total, 
                                "tax": tax_amount, 
                                "discount": discount + fest_disc,
                                "points": 0
                            }
                            st.session_state['checkout_stage'] = 'payment_method'
                            st.rerun()
            else:
                st.info("Cart is empty")
            st.markdown("</div>", unsafe_allow_html=True)

    elif st.session_state['checkout_stage'] == 'payment_method':
        st.markdown("<div class='card-container'>", unsafe_allow_html=True)
        st.button("⬅ Back to Cart", on_click=lambda: st.session_state.update({'checkout_stage': 'cart'}))
        
        st.markdown("<h2 style='text-align: center;'>Select Payment Method</h2>", unsafe_allow_html=True)
        
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("💵 Cash", use_container_width=True):
                st.session_state['selected_payment_mode'] = 'Cash'
                st.session_state['checkout_stage'] = 'payment_process'
                st.rerun()
        with c2:
            if st.button("📱 UPI", use_container_width=True):
                st.session_state['selected_payment_mode'] = 'UPI'
                st.session_state['checkout_stage'] = 'payment_process'
                st.session_state['qr_expiry'] = None 
                st.rerun()
        with c3:
            if st.button("💳 Card", use_container_width=True):
                st.session_state['selected_payment_mode'] = 'Card'
                st.session_state['checkout_stage'] = 'payment_process'
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    elif st.session_state['checkout_stage'] == 'payment_process':
        st.markdown("<div class='card-container'>", unsafe_allow_html=True)
        
        st.button("⬅ Cancel", on_click=lambda: st.session_state.update({'checkout_stage': 'payment_method'}))

        calc = st.session_state['final_calc']
        total = calc['total']
        mode = st.session_state['selected_payment_mode']
        
        st.subheader(f"Processing {mode} Payment - Amount: {currency}{total:,.2f}")
        
        if mode == 'Cash':
            if st.button("Confirm Cash Payment", type="primary"):
                finalize_sale(total, "Cash")

        elif mode == 'UPI':
            if st.session_state['qr_expiry'] is None:
                st.session_state['qr_expiry'] = time.time() + 240 
            
            c_qr, c_info = st.columns([1, 1])
            with c_qr:
                upi_id = db.get_setting("upi_id")
                qr_img = utils.generate_upi_qr(upi_id, store_name, total, "Bill Payment")
                st.image(qr_img, width=250, caption=f"Scan to Pay: {currency}{total:.2f}")
            
            with c_info:
                st.markdown("### Verification")
                if st.button("Verify & Print Bill"):
                    finalize_sale(total, "UPI")

        elif mode == 'Card':
            st.info("💳 Card Payment Simulation")
            col_cc1, col_cc2 = st.columns(2)
            with col_cc1:
                cc_num = st.text_input("Card Number", max_chars=16)
            with col_cc2:
                cc_cvv = st.text_input("CVV", type="password", max_chars=4)
            
            if st.button("Process Transaction"):
                with st.spinner("Processing..."):
                    time.sleep(1.5)
                    finalize_sale(total, "Card")

        st.markdown("</div>", unsafe_allow_html=True)

    elif st.session_state['checkout_stage'] == 'receipt':
        st.markdown("<div class='card-container' style='text-align:center'>", unsafe_allow_html=True)
        st.title("✅ Payment Successful")
        st.caption("Transaction has been recorded.")
        
        c_rec1, c_rec2 = st.columns(2)
        with c_rec1:
            if 'last_receipt' in st.session_state:
                st.download_button("📄 Download Receipt PDF", st.session_state['last_receipt'], "receipt.pdf", "application/pdf", use_container_width=True)
        
        with c_rec2:
            if st.button("🛒 Start New Sale", type="primary", use_container_width=True):
                st.session_state['cart'] = []
                st.session_state['current_customer'] = None
                st.session_state['checkout_stage'] = 'cart'
                st.session_state['applied_coupon'] = None
                st.session_state['points_to_redeem'] = 0
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

def finalize_sale(total, mode):
    calc = st.session_state['final_calc']
    txn_time = utils.get_system_time().strftime("%Y-%m-%d %H:%M:%S")
    operator = st.session_state['full_name']
    customer = st.session_state['current_customer']
    customer_mobile = customer['mobile'] if customer else None
    
    points_earned = 0
    if customer:
        points_earned = utils.calculate_loyalty_points(total)
    
    items_json = json.dumps([i['id'] for i in st.session_state['cart']])
    # Internal logic removed from hash generation conceptually, just passing placeholders
    integrity_hash = "NA"
    
    try:
        # Calls db.process_sale_transaction without coupon args
        sale_id = db.process_sale_transaction(
            st.session_state['cart'],
            total,
            mode,
            operator,
            st.session_state['pos_id'],
            customer_mobile,
            calc['tax'],
            calc['points'],
            points_earned,
            integrity_hash,
            30 
        )
        
        db.log_activity(operator, "Sale Completed", f"Sale #{sale_id} for {currency}{total:.2f}")
        
        tax_info = {"tax_amount": calc['tax'], "tax_percent": 18}
        
        pdf = utils.generate_receipt_pdf(store_name, sale_id, txn_time, st.session_state['cart'], total, operator, mode, st.session_state['pos_id'], customer, tax_info, new_coupon=None)
        
        st.session_state['last_receipt'] = pdf
        st.session_state['checkout_stage'] = 'receipt'
        st.rerun()
        
    except Exception as e:
        st.error(f"Transaction Failed: {str(e)}")

def inventory_manager():
    st.title("📦 Inventory Management")
    # Tab 1 renamed from View & Edit to View
    tab_view, tab_add = st.tabs(["View Stock", "Add New Product"])
    
    conn = db.get_connection()
    df = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    
    with tab_view:
        st.markdown("<div class='card-container'>", unsafe_allow_html=True)
        col_f1, col_f2 = st.columns(2)
        cat_filter = col_f1.selectbox("Filter Category", ["All"] + db.get_categories_list())
        search_txt = col_f2.text_input("Search Name")
        df_filtered = df
        if cat_filter != "All": df_filtered = df[df['category'] == cat_filter]
        if search_txt: df_filtered = df_filtered[df_filtered['name'].str.contains(search_txt, case=False)]
        
        st.dataframe(df_filtered[['id', 'name', 'category', 'price', 'stock', 'sales_count']], use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with tab_add:
        # Category Management Section
        with st.expander("📂 Manage Categories"):
            c_m1, c_m2, c_m3 = st.columns(3)
            with c_m1:
                new_cat_name = st.text_input("New Category Name")
                if st.button("Add Category"):
                    if new_cat_name:
                        if db.add_category(new_cat_name):
                            st.success(f"Added {new_cat_name}")
                            time.sleep(1)
                            st.rerun()
                        else: st.error("Failed or already exists")
            with c_m2:
                cat_to_del = st.selectbox("Delete Category", db.get_categories_list(), key="del_cat_sel")
                if st.button("Delete"):
                    db.delete_category(cat_to_del)
                    st.success("Deleted")
                    time.sleep(1)
                    st.rerun()
            with c_m3:
                cat_rename_from = st.selectbox("Rename From", db.get_categories_list(), key="ren_cat_sel")
                cat_rename_to = st.text_input("Rename To")
                if st.button("Rename"):
                    db.rename_category(cat_rename_from, cat_rename_to)
                    st.success("Renamed")
                    time.sleep(1)
                    st.rerun()

        st.markdown("<div class='card-container'>", unsafe_allow_html=True)
        
        # Checking for form clearance
        if st.session_state.get('clear_inventory_form'):
            st.session_state['clear_inventory_form'] = False
            # Rerun logic handled by clearing widgets below? 
            # Streamlit forms don't clear nicely on command without session state hacks
            # Using st.form with clear_on_submit=False for manual control logic or True for auto
            # Requested: On success: Clear. On error: Show error (don't clear).
            
        with st.form("new_prod"):
            n = st.text_input("Product Name")
            c = st.selectbox("Category", db.get_categories_list())
            p = st.number_input("Selling Price", min_value=0.0)
            cp = st.number_input("Cost Price", min_value=0.0)
            s = st.number_input("Initial Stock", min_value=0, step=1)
            img_file = st.file_uploader("Product Image", type=['png', 'jpg', 'jpeg'])
            
            submit_prod = st.form_submit_button("Add Product")
            
            if submit_prod:
                if s < 0:
                    st.error("Stock quantity cannot be negative.")
                elif not n:
                    st.error("Product Name is required.")
                else:
                    img_bytes = img_file.getvalue() if img_file else None
                    if db.add_product(n, c, p, s, cp, None, img_bytes):
                        st.success(f"Product '{n}' Added Successfully!")
                        # To clear the form, we need to rerun. 
                        time.sleep(1)
                        st.rerun()
                    else: 
                        st.error("Error adding product to database.")
        st.markdown("</div>", unsafe_allow_html=True)

def analytics_dashboard():
    st.title("📊 Business Intelligence Dashboard")
    st.markdown("---")

    # Fetch Data
    df_sales = db.get_sales_data()
    conn = db.get_connection()
    df_products = pd.read_sql("SELECT * FROM products", conn)
    conn.close()

    # Data Preprocessing
    if df_sales.empty:
        st.warning("No sales data available to generate analytics.")
        return

    # Filter Cancelled - Strict Rule: Cancelled orders excluded from analytics
    if 'status' in df_sales.columns:
        df_sales = df_sales[df_sales['status'] != 'Cancelled']

    # Convert timestamps
    df_sales['timestamp'] = pd.to_datetime(df_sales['timestamp'])
    df_sales['date'] = df_sales['timestamp'].dt.date
    df_sales['hour'] = df_sales['timestamp'].dt.hour
    df_sales['day_name'] = df_sales['timestamp'].dt.day_name()

    # Detailed Item Level Data
    items_list = []
    prod_dict = df_products.set_index('id').to_dict('index')

    for _, row in df_sales.iterrows():
        try:
            ids = json.loads(row['items_json'])
            for pid in ids:
                if pid in prod_dict:
                    p = prod_dict[pid]
                    items_list.append({
                        'sale_id': row['id'],
                        'timestamp': row['timestamp'],
                        'product_name': p['name'],
                        'category': p['category'],
                        'selling_price': p['price'],
                        'cost_price': p['cost_price'],
                        'profit': p['price'] - p['cost_price']
                    })
        except:
            continue
    
    df_items = pd.DataFrame(items_list)

    if df_items.empty:
        st.warning("Sales data found, but unable to process item details.")
        return

    # --- 1. Total Revenue Analysis ---
    st.subheader("1. 💰 Revenue Analysis")
    col1, col2, col3 = st.columns(3)
    
    total_revenue = df_sales['total_amount'].sum()
    revenue_today = df_sales[df_sales['date'] == datetime.now().date()]['total_amount'].sum()
    
    col1.metric("Total Revenue", f"{currency}{total_revenue:,.2f}")
    col2.metric("Today's Revenue", f"{currency}{revenue_today:,.2f}")
    
    # Revenue Trend (Daily)
    daily_rev = df_sales.groupby('date')['total_amount'].sum()
    st.markdown("**Revenue Trend (Daily)**")
    st.line_chart(daily_rev)

    # --- 2. Profit Analysis ---
    st.subheader("2. 📈 Profitability Insights")
    # Gross Profit = Sum of (Price - Cost) for all sold items
    gross_profit = df_items['profit'].sum()
    margin_pct = (gross_profit / total_revenue * 100) if total_revenue > 0 else 0
    
    c1, c2 = st.columns(2)
    c1.metric("Gross Profit", f"{currency}{gross_profit:,.2f}")
    c2.metric("Net Margin", f"{margin_pct:.1f}%")
    
    # Most Profitable Products
    st.markdown("**Top 5 Most Profitable Products**")
    top_profit_products = df_items.groupby('product_name')['profit'].sum().sort_values(ascending=False).head(5)
    st.bar_chart(top_profit_products)

    # --- 3. Sales Trend Analysis ---
    st.subheader("3. ⏰ Sales Trends")
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown("**Peak Sales Hours**")
        hourly_sales = df_sales.groupby('hour')['total_amount'].count() # Transactions per hour
        st.bar_chart(hourly_sales)
        st.caption("Number of Transactions per Hour")

    with c2:
        st.markdown("**Busiest Days of Week**")
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        daily_activity = df_sales['day_name'].value_counts().reindex(day_order)
        st.bar_chart(daily_activity)

    # --- 4. Product Performance ---
    st.subheader("4. 🏆 Product Performance")
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown("**Top Selling Products (Qty)**")
        top_qty = df_items['product_name'].value_counts().head(5)
        st.table(top_qty)
        
    with c2:
        st.markdown("**Slow Moving Products**")
        # Products in inventory but low sales in df_items
        all_products = df_products['name'].unique()
        sold_counts = df_items['product_name'].value_counts()
        slow_movers = []
        for p in all_products:
            if p not in sold_counts:
                slow_movers.append((p, 0))
            else:
                if sold_counts[p] < 5: # Threshold
                    slow_movers.append((p, sold_counts[p]))
        
        df_slow = pd.DataFrame(slow_movers, columns=['Product', 'Sales Count']).sort_values('Sales Count').head(5)
        st.table(df_slow)

    # --- 5. Category Performance ---
    st.subheader("5. 📂 Category Analytics")
    
    cat_perf = df_items.groupby('category').agg(
        Revenue=('selling_price', 'sum'),
        Profit=('profit', 'sum')
    )
    st.bar_chart(cat_perf)

    # --- 6. Quantity & Demand ---
    st.subheader("6. 📦 Demand Analysis")
    st.info("💡 High Demand Items are driving your revenue. Ensure they are always stocked.")
    
    # --- 7. Payment Mode ---
    st.subheader("7. 💳 Payment Patterns")
    pay_counts = df_sales['payment_mode'].value_counts()
    
    c1, c2 = st.columns([1, 2])
    with c1:
        st.write(pay_counts)
    with c2:
        fig, ax = plt.subplots()
        ax.pie(pay_counts, labels=pay_counts.index, autopct='%1.1f%%', startangle=90)
        ax.axis('equal') 
        st.pyplot(fig)

    # --- 8. Inventory Insights ---
    st.subheader("8. 📋 Inventory Recommendations")
    
    low_stock = df_products[df_products['stock'] < 10][['name', 'stock', 'category']]
    excess_stock = df_products[(df_products['stock'] > 50) & (df_products['sales_count'] < 10)][['name', 'stock', 'sales_count']]
    
    tab1, tab2 = st.tabs(["⚠️ Restock Needed", "🛑 Excess Stock Risk"])
    
    with tab1:
        if not low_stock.empty:
            st.error(f"{len(low_stock)} items are running low.")
            st.dataframe(low_stock, use_container_width=True)
        else:
            st.success("Stock levels look healthy.")
            
    with tab2:
        if not excess_stock.empty:
            st.warning(f"{len(excess_stock)} items have high stock but low sales.")
            st.dataframe(excess_stock, use_container_width=True)
        else:
            st.success("No excess stock risks detected.")
            
    # Optional Advanced: AOV
    st.markdown("---")
    avg_order_value = total_revenue / len(df_sales)
    st.metric("🛒 Average Order Value (AOV)", f"{currency}{avg_order_value:,.2f}")

def marketing_hub():
    st.title("🚀 Retail Marketing Hub")
    
    st.markdown("<div class='card-container'>", unsafe_allow_html=True)
    st.subheader("🎲 Lucky Draw System")
    st.caption("Select winner from eligible customers based on sales history.")
    
    c1, c2, c3 = st.columns(3)
    ld_days = c1.number_input("Sales Lookback (Days)", value=7)
    ld_min = c2.number_input("Minimum Spend", value=1000)
    ld_prize = c3.text_input("Prize", value="Mystery Gift Box")
    
    if st.button("🎰 Pick Winner"):
        winner = db.pick_lucky_winner(ld_days, ld_min, ld_prize)
        if winner:
            st.balloons()
            st.success(f"🎉 Winner: {winner['name']} ({winner['mobile']})")
        else:
            st.warning("No eligible customers found.")
            
    st.markdown("#### Past Winners")
    st.dataframe(db.get_lucky_draw_history(), use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

def orders_page():
    st.title("📜 Order & Payment Details")
    
    st.markdown("<div class='card-container'>", unsafe_allow_html=True)
    st.subheader("🔍 Search & Filters")
    c1, c2 = st.columns(2)
    f_id = c1.number_input("Order ID", min_value=0, value=0)
    f_op = c2.text_input("Customer Mobile")
    
    filters = {}
    if f_id > 0: filters['bill_no'] = f_id
    
    # Updated: Removed POS ID logic, added Customer columns in display
    txns = db.get_transaction_history(filters)
    
    # If the user searched by Mobile, filter client-side if the query doesn't handle it fully 
    # (The query in db filters by operator, we can filter by mobile here if needed, but db.get_transaction_history handles filtering by mobile if operator filter logic was meant for it? 
    # Actually the input says 'Customer Mobile' but the code used it for 'operator' param in filters. I'll check previous code.
    # Previous code passed f_op to filters['operator']. Let's correct this filter logic visually here or assumes DB handles it.
    # The new DB query returns customer_mobile. I will filter dataframe here for simplicity if db query isn't perfect for mobile search)
    if f_op and not txns.empty:
        txns = txns[txns['customer_mobile'].astype(str).str.contains(f_op, na=False)]
    
    # Clean Columns for Display
    if not txns.empty:
        # Select columns to show
        # ID, Date, Amount, Method, Operator, Cust Name, Cust Email, Cust Mobile, Status
        display_df = txns[['id', 'timestamp', 'total_amount', 'payment_mode', 'operator', 
                           'customer_name', 'customer_email', 'customer_mobile', 'status']]
        display_df.columns = ["Order ID", "Date", "Total", "Method", "Cashier", 
                              "Customer Name", "Customer Email", "Mobile", "Status"]
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No records found.")
    
    st.markdown("---")
    st.subheader("❌ Cancel Order (Admin Only)")
    with st.form("cancel_order_form"):
        c_oid = st.number_input("Order ID to Cancel", min_value=1, step=1)
        c_reason = st.text_input("Cancellation Reason (Mandatory)")
        c_pass = st.text_input("Admin Password to Confirm", type="password")
        
        if st.form_submit_button("🚨 Cancel Order"):
            if not c_reason:
                st.error("Reason is mandatory.")
            else:
                success, msg = db.cancel_sale_transaction(c_oid, st.session_state['user'], st.session_state['role'], c_reason, c_pass)
                if success:
                    st.success(msg)
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(msg)
    
    st.markdown("---")
    st.subheader("🚫 Cancelled Orders Audit")
    cancels = db.get_cancellation_audit_log()
    st.dataframe(cancels, use_container_width=True)
    st.markdown("</div>", unsafe_allow_html=True)

def admin_panel():
    st.title("⚙️ Admin Settings")
    st.markdown("<div class='card-container'>", unsafe_allow_html=True)
    
    with st.form("settings_form"):
        s_name = st.text_input("Store Name", value=db.get_setting("store_name"))
        s_upi = st.text_input("UPI ID", value=db.get_setting("upi_id"))
        col_s1, col_s2 = st.columns(2)
        with col_s1: s_tax = st.number_input("GST %", value=float(db.get_setting("tax_rate")))
        with col_s2: s_gst_enable = st.checkbox("Enable GST", value=(db.get_setting("gst_enabled") == 'True'))
        
        if st.form_submit_button("Save Settings"):
            db.set_setting("store_name", s_name)
            db.set_setting("upi_id", s_upi)
            db.set_setting("tax_rate", str(s_tax))
            db.set_setting("gst_enabled", str(s_gst_enable))
            db.log_activity(st.session_state['user'], "Settings Update", "Modified")
            st.success("Settings Saved!")
            time.sleep(1)
            st.rerun()
    
    st.markdown("---")
    st.subheader("👤 Create New Operator")
    with st.form("create_op_form"):
        new_op_name = st.text_input("Operator Name")
        new_op_user = st.text_input("Username").strip().lower()
        new_op_pass = st.text_input("Password", type="password")
        
        if st.form_submit_button("Create Operator"):
            if not new_op_name or not new_op_user or not new_op_pass:
                st.error("All fields are mandatory.")
            else:
                if db.create_user(new_op_user, new_op_pass, "Operator", new_op_name):
                    st.success(f"Operator {new_op_name} created successfully!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Username already exists or error creating user.")

    st.markdown("</div>", unsafe_allow_html=True)

def user_profile_page():
    st.title("👤 My Profile")
    st.markdown("<div class='card-container'>", unsafe_allow_html=True)
    with st.form("profile_upd"):
        new_name = st.text_input("Full Name", value=st.session_state['full_name'])
        if st.form_submit_button("Update Profile"):
            db.update_fullname(st.session_state['user'], new_name)
            st.session_state['full_name'] = new_name
            st.success("Updated")
    st.divider()
    st.subheader("Change Password")
    with st.form("pass_chg"):
        old_p = st.text_input("Old Password", type="password")
        new_p = st.text_input("New Password", type="password")
        if st.form_submit_button("Change Password"):
            if db.verify_password(st.session_state['user'], old_p):
                db.update_password(st.session_state['user'], new_p)
                st.success("Password Changed")
            else: st.error("Incorrect Old Password")
    st.markdown("</div>", unsafe_allow_html=True)

# --- MAIN CONTROLLER ---
def main():
    if not st.session_state.get('user'):
        login_view()
    else:
        with st.sidebar:
            st.markdown(f"""
            <div style="padding: 15px; background: rgba(255,255,255,0.05); border-radius: 12px; margin-bottom: 20px; border: 1px solid rgba(255,255,255,0.1);">
                <div style="font-size: 0.8rem; opacity: 0.7; letter-spacing: 1px;">CURRENT USER</div>
                <div style="font-weight: 600; font-size: 1.1rem; margin-top: 5px;">{st.session_state['user']}</div>
                <div style="font-size: 0.85rem; color: #6366f1; margin-top: 2px;">{st.session_state['role']}</div>
            </div>
            """, unsafe_allow_html=True)
            
            role = st.session_state['role']
            
            nav_opts = []
            if role == "Admin": 
                nav_opts = ["Retail Marketing Hub", "Inventory", "Orders", "Analytics", "Admin Settings", "My Profile"]
            else:
                nav_opts = ["POS Terminal", "My Profile"]
            
            choice = st.radio("Navigate", nav_opts, label_visibility="collapsed")
            
            st.markdown("---")
            if st.button("🚪 Log Out", use_container_width=True): logout_user()
        
        if choice == "POS Terminal": pos_interface()
        elif choice == "Inventory": inventory_manager()
        elif choice == "Analytics": analytics_dashboard()
        elif choice == "Retail Marketing Hub": marketing_hub()
        elif choice == "Orders": orders_page()
        elif choice == "Admin Settings": admin_panel()
        elif choice == "My Profile": user_profile_page()

if __name__ == "__main__":
    main()
