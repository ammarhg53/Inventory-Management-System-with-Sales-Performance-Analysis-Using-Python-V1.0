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
    page_title="SmartInventory ERP", 
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
        <p style="opacity: 0.6; font-size: 1.1rem; letter-spacing: 1px;">NEXT-GEN ERP & POS SYSTEM</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<div class='login-box' style='margin: 0 auto;'>", unsafe_allow_html=True)
    st.subheader("🔐 Secure Access")
    
    with st.form("login_frm"):
        user_in = st.text_input("Username", placeholder="e.g. admin").strip().lower()
        pass_in = st.text_input("Password", type="password", placeholder="••••••••")
        
        st.markdown("<br>", unsafe_allow_html=True)
        submit = st.form_submit_button("🚀 Access System", type="primary", use_container_width=True)
        
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
        st.title(f"🛒 {st.session_state['pos_id']}")
        st.caption("Point of Sale Terminal • Live")
    with c3:
        st.markdown(f"<div style='text-align:right'><b>{st.session_state['full_name']}</b><br><span style='font-size:0.8em;opacity:0.7'>Operator</span></div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)
        
    trie, df_p = refresh_trie()
    
    # --- STATE MACHINE: CART VIEW ---
    if st.session_state['checkout_stage'] == 'cart':
        
        with st.expander("👤 Customer Details (Required for Bill)", expanded=st.session_state['current_customer'] is None):
            col_cc, col_mob, col_btn = st.columns([1, 2, 1])
            with col_cc:
                country_codes = ["+91", "+965", "+971", "+966", "+1"]
                country_code = st.selectbox("Country Code", country_codes, index=0)
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
                        if not cust:
                            cust = db.get_customer(cust_phone_input)
                        
                        if cust:
                            st.session_state['current_customer'] = cust
                            st.success(f"Welcome back, {cust['name']}")
                        else:
                            st.session_state['temp_new_customer'] = normalized_phone
                            st.warning(f"New Customer: {normalized_phone}")
            
            if st.session_state.get('temp_new_customer') and not st.session_state.get('current_customer'):
                with st.form("new_cust_form"):
                    new_name = st.text_input("Full Name")
                    new_email = st.text_input("Email (Optional)")
                    if st.form_submit_button("Save Customer"):
                        if new_name:
                            db.upsert_customer(st.session_state['temp_new_customer'], new_name, new_email)
                            st.session_state['current_customer'] = db.get_customer(st.session_state['temp_new_customer'])
                            st.session_state.pop('temp_new_customer', None)
                            st.success("Customer Added!")
                            st.rerun()
                        else:
                            st.error("Name is required.")
            
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
                algo = st.selectbox("Search Algo", ["Trie (O(L))", "Linear (O(N))"])

        left_panel, right_panel = st.columns([2, 1])

        with left_panel:
            results = []
            if query:
                if algo.startswith("Trie"):
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
                
                discount = 0
                loss_discount = 0 # Feature disabled, kept variable 0
                fest_disc = 0 # Feature disabled, kept 0

                total_after_disc = max(0, raw_total - discount - fest_disc - st.session_state['points_to_redeem'] - loss_discount)
                
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
                                "discount": discount + fest_disc + loss_discount,
                                "points": st.session_state['points_to_redeem']
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
    integrity_hash = utils.generate_integrity_hash((txn_time, total, items_json, operator))
    
    try:
        sale_id = db.process_sale_transaction(
            st.session_state['cart'],
            total,
            mode,
            operator,
            st.session_state['pos_id'],
            customer_mobile,
            calc['tax'],
            calc['discount'],
            None, # Coupon Code
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
    tab_view, tab_add, tab_restock = st.tabs(["View & Edit", "Add New Product", "➕ Restock"])
    
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
        
        st.dataframe(df_filtered[['id', 'name', 'category', 'price', 'stock', 'is_dead_stock']], use_container_width=True)
        
        st.markdown("##### 💀 Manage Dead Stock")
        c1, c2 = st.columns([1, 3])
        ds_id = c1.number_input("Product ID", min_value=1, step=1, key="ds_pid")
        ds_action = c2.radio("Set Status", ["Active", "Dead Stock"], horizontal=True)
        if st.button("Update Status"):
            db.toggle_dead_stock(ds_id, ds_action == "Dead Stock")
            st.success("Status Updated")
            time.sleep(1)
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    with tab_add:
        st.markdown("<div class='card-container'>", unsafe_allow_html=True)
        with st.form("new_prod", clear_on_submit=True):
            n = st.text_input("Product Name")
            c = st.selectbox("Category", db.get_categories_list())
            p = st.number_input("Selling Price", min_value=0.0)
            cp = st.number_input("Cost Price", min_value=0.0)
            s = st.number_input("Initial Stock", min_value=0)
            
            if st.form_submit_button("Add Product"):
                if db.add_product(n, c, p, s, cp, None, None):
                    st.success(f"Product Added: {n}")
                else: 
                    st.error("Error adding product")
        st.markdown("</div>", unsafe_allow_html=True)
                    
    with tab_restock:
        st.markdown("<div class='card-container'>", unsafe_allow_html=True)
        if not df.empty:
            prod_opts = {f"{row['name']} (ID: {row['id']})": row['id'] for idx, row in df.iterrows()}
            sel = st.selectbox("Select Product", list(prod_opts.keys()))
            if sel: 
                pid = prod_opts[sel]
                p_curr = db.get_product_by_id(pid)
                if p_curr:
                    st.write(f"**Selected:** {p_curr['name']} | **Current Stock:** {p_curr['stock']}")
                    qty = st.number_input("Add Quantity", min_value=1, value=10)
                    if st.button("Confirm Restock"):
                        db.restock_product(pid, qty)
                        st.success("Stock Updated!")
                        time.sleep(1)
                        st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

def analytics_dashboard():
    st.title("📈 Enterprise Analytics")
    df_sales = db.get_sales_data()
    
    if 'status' in df_sales.columns:
        active_sales = df_sales[df_sales['status'] != 'Cancelled']
    else:
        active_sales = df_sales

    conn = db.get_connection()
    df_prods = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    
    try:
        active_sales['date'] = pd.to_datetime(active_sales['timestamp'], format='mixed', dayfirst=False, errors='coerce')
        active_sales = active_sales.dropna(subset=['date'])
    except:
        st.error("Date parsing failed.")
        return
    
    # Time Filters
    st.markdown("<div class='card-container'>", unsafe_allow_html=True)
    st.markdown("### 🗓️ Time Filter")
    
    min_date = active_sales['date'].min().date() if not active_sales.empty else datetime.now().date()
    max_date = active_sales['date'].max().date() if not active_sales.empty else datetime.now().date()
    
    date_range = st.date_input("Select Range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_d, end_d = date_range
        mask = (active_sales['date'].dt.date >= start_d) & (active_sales['date'].dt.date <= end_d)
        filtered_sales = active_sales.loc[mask]
    else:
        filtered_sales = active_sales

    st.markdown("</div>", unsafe_allow_html=True)
    
    total_rev = filtered_sales['total_amount'].sum()
    total_txns = len(filtered_sales)
    
    m1, m2, m3 = st.columns(3)
    with m1: st.markdown(f"<div class='kpi-card'><div class='kpi-title'>Total Revenue</div><div class='kpi-value'>{currency}{total_rev:,.0f}</div></div>", unsafe_allow_html=True)
    with m2: st.markdown(f"<div class='kpi-card'><div class='kpi-title'>Active Sales</div><div class='kpi-value'>{total_txns}</div></div>", unsafe_allow_html=True)
    val = total_rev/total_txns if total_txns > 0 else 0
    with m3: st.markdown(f"<div class='kpi-card'><div class='kpi-title'>Avg Order Value</div><div class='kpi-value'>{currency}{val:.0f}</div></div>", unsafe_allow_html=True)

    t1, t2, t3, t4 = st.tabs(["Sales Trends", "Category Performance", "Profit & Loss", "Forecasting"])
    
    with t1:
        st.markdown("<div class='card-container'>", unsafe_allow_html=True)
        if not filtered_sales.empty:
            daily = filtered_sales.groupby(filtered_sales['date'].dt.date)['total_amount'].sum().reset_index()
            st.line_chart(daily.set_index('date')['total_amount'])
        st.markdown("</div>", unsafe_allow_html=True)

    with t2:
        st.markdown("<div class='card-container'>", unsafe_allow_html=True)
        cat_perf_df = pd.DataFrame()
        if not filtered_sales.empty:
            cat_sales = {}
            prod_cat_map = df_prods.set_index('id')['category'].to_dict()
            for _, row in filtered_sales.iterrows():
                try:
                    item_ids = json.loads(row['items_json'])
                    for iid in item_ids:
                        cat = prod_cat_map.get(iid, "Unknown")
                        share = row['total_amount'] / len(item_ids) 
                        cat_sales[cat] = cat_sales.get(cat, 0) + share
                except: continue
            cat_perf_df = pd.DataFrame(list(cat_sales.items()), columns=['Category', 'Revenue']).sort_values('Revenue', ascending=False)

        if not cat_perf_df.empty:
            st.bar_chart(cat_perf_df.set_index('Category'))
        st.markdown("</div>", unsafe_allow_html=True)

    with t3:
        st.markdown("<div class='card-container'>", unsafe_allow_html=True)
        pl_summary, pl_df = utils.calculate_profit_loss(filtered_sales, df_prods) 
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Gross Revenue", f"{currency}{pl_summary['total_revenue']:,.2f}")
        c2.metric("Net Profit", f"{currency}{pl_summary['net_profit']:,.2f}")
        c3.metric("Margin", f"{pl_summary['margin_percent']:.1f}%")
        
        st.dataframe(pl_df, use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with t4:
        st.markdown("<div class='card-container'>", unsafe_allow_html=True)
        if not filtered_sales.empty:
            daily = filtered_sales.groupby(filtered_sales['date'].dt.date)['total_amount'].sum().reset_index()
            daily_vals = daily['total_amount'].values
            prediction = utils.forecast_next_period(daily_vals)
            st.metric("Predicted Next Day Sales", f"{currency}{prediction:.2f}")
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
            nav_opts = ["POS Terminal", "My Profile"]
            if role == "Admin": 
                nav_opts = ["POS Terminal", "Inventory", "Analytics", "Admin Panel", "My Profile"]
            
            choice = st.radio("Navigate", nav_opts, label_visibility="collapsed")
            
            st.markdown("---")
            if st.button("🚪 Log Out", use_container_width=True): logout_user()
        
        if choice == "POS Terminal": pos_interface()
        elif choice == "Inventory": inventory_manager()
        elif choice == "Analytics": analytics_dashboard()
        elif choice == "Admin Panel": admin_panel()
        elif choice == "My Profile": user_profile_page()

if __name__ == "__main__":
    main()
