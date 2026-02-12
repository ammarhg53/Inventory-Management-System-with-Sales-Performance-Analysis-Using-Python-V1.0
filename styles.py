import streamlit as st
import base64

def load_css(theme="dark"):
    # Define Color Palettes
    if theme == "dark":
        primary_bg = "#0f172a" # Slate 900
        secondary_bg = "#1e293b" # Slate 800
        text_color = "#f8fafc" # Slate 50
        accent_color = "#6366f1" # Indigo 500
        success_color = "#10b981" # Emerald 500
        warning_color = "#f59e0b" # Amber 500
        error_color = "#ef4444" # Red 500
        card_bg = "rgba(30, 41, 59, 0.7)"
        border_color = "rgba(255, 255, 255, 0.1)"
        shadow = "0 4px 6px -1px rgba(0, 0, 0, 0.3)"
        muted_text = "#94a3b8"
    elif theme == "adaptive":
        # Professional Adaptive Theme
        primary_bg = "#f3f4f6" # Gray 100
        secondary_bg = "#ffffff" # White
        text_color = "#111827" # Gray 900
        accent_color = "#2563eb" # Blue 600
        success_color = "#059669" # Emerald 600
        warning_color = "#d97706" # Amber 600
        error_color = "#dc2626" # Red 600
        card_bg = "rgba(255, 255, 255, 0.95)"
        border_color = "rgba(0, 0, 0, 0.1)"
        shadow = "0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)"
        muted_text = "#4b5563"
    else: # light (Classic)
        primary_bg = "#f8fafc" # Slate 50
        secondary_bg = "#ffffff" # White
        text_color = "#0f172a" # Slate 900
        accent_color = "#4f46e5" # Indigo 600
        success_color = "#059669" # Emerald 600
        warning_color = "#d97706" # Amber 600
        error_color = "#dc2626" # Red 600
        card_bg = "rgba(255, 255, 255, 0.9)"
        border_color = "rgba(0, 0, 0, 0.08)"
        shadow = "0 10px 15px -3px rgba(0, 0, 0, 0.05)"
        muted_text = "#64748b"

    st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap');

        :root {{
            --primary-bg: {primary_bg};
            --secondary-bg: {secondary_bg};
            --text-color: {text_color};
            --accent-color: {accent_color};
            --success-color: {success_color};
            --warning-color: {warning_color};
            --error-color: {error_color};
            --card-bg: {card_bg};
            --border-color: {border_color};
            --shadow: {shadow};
            --muted-text: {muted_text};
        }}

        /* --- GLOBAL RESET & TYPOGRAPHY --- */
        html, body, [class*="css"] {{
            font-family: 'Inter', sans-serif;
            color: var(--text-color);
            background-color: var(--primary-bg);
        }}
        
        .stApp {{
            background-color: var(--primary-bg);
            background-image: 
                radial-gradient(at 0% 0%, {accent_color}15 0px, transparent 50%),
                radial-gradient(at 100% 100%, {success_color}10 0px, transparent 50%);
            background-attachment: fixed;
        }}

        h1, h2, h3, h4, h5, h6 {{
            font-weight: 700;
            color: var(--text-color);
            letter-spacing: -0.025em;
        }}
        
        /* --- SIDEBAR NAVIGATION --- */
        section[data-testid="stSidebar"] {{
            background-color: var(--secondary-bg);
            border-right: 1px solid var(--border-color);
            box-shadow: 10px 0 20px rgba(0,0,0,0.05);
        }}
        
        /* --- CARDS & CONTAINERS --- */
        .card-container {{
            background-color: var(--card-bg);
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 24px;
            box-shadow: var(--shadow);
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        
        .card-container:hover {{
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.05);
            border-color: {accent_color}40;
        }}

        /* --- METRICS & KPIs --- */
        .kpi-card {{
            background-color: var(--secondary-bg);
            border-radius: 12px;
            padding: 20px;
            border-left: 4px solid var(--accent-color);
            box-shadow: var(--shadow);
            margin-bottom: 10px;
        }}
        
        .kpi-title {{ font-size: 0.85rem; color: var(--muted-text); text-transform: uppercase; font-weight: 600; margin-bottom: 5px; }}
        .kpi-value {{ font-size: 1.8rem; font-weight: 800; color: var(--text-color); font-family: 'JetBrains Mono', monospace; }}
        .kpi-delta {{ font-size: 0.8rem; color: var(--success-color); }}

        /* --- TABLES --- */
        div[data-testid="stDataFrame"] {{
            border: 1px solid var(--border-color);
            border-radius: 12px;
            overflow: hidden;
            box-shadow: var(--shadow);
        }}
        
        thead tr th {{
            background-color: {accent_color}15 !important;
            color: var(--text-color) !important;
            border-bottom: 2px solid var(--border-color) !important;
        }}

        /* --- PRODUCT GRID (POS) --- */
        .product-card {{
            background-color: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 16px;
            text-align: left;
            transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }}

        .product-card:hover {{
            transform: translateY(-4px);
            box-shadow: 0 15px 30px -5px rgba(0, 0, 0, 0.1);
            border-color: var(--accent-color);
        }}
        
        .product-icon {{ font-size: 2.2rem; margin-bottom: 12px; text-align: center; }}
        .product-title {{ font-size: 1rem; font-weight: 600; color: var(--text-color); margin-bottom: 4px; }}
        .product-cat {{ font-size: 0.7rem; text-transform: uppercase; color: var(--muted-text); margin-bottom: 12px; }}
        .product-img {{ width: 100%; height: 120px; object-fit: cover; border-radius: 8px; margin-bottom: 10px; }}

        .product-footer {{
            display: flex; justify-content: space-between; align-items: center;
            margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--border-color);
        }}

        .product-price {{ font-size: 1.1rem; font-weight: 700; color: var(--accent-color); font-family: 'JetBrains Mono', monospace; }}

        /* --- BADGES --- */
        .badge {{ font-size: 0.7rem; padding: 4px 10px; border-radius: 20px; font-weight: 600; display: inline-block; }}
        .badge-success {{ background: {success_color}20; color: var(--success-color); border: 1px solid {success_color}40; }}
        .badge-danger {{ background: {error_color}20; color: var(--error-color); border: 1px solid {error_color}40; }}
        
        .status-tag {{
            display: inline-block; padding: 2px 8px; border-radius: 4px;
            font-size: 0.75rem; font-weight: 600; background: var(--secondary-bg); border: 1px solid var(--border-color);
        }}

        /* --- ANIMATIONS & STATUS --- */
        @keyframes pulse-green {{
            0% {{ box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.4); }}
            70% {{ box-shadow: 0 0 0 10px rgba(16, 185, 129, 0); }}
            100% {{ box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }}
        }}

        .status-dot-online {{
            height: 10px; width: 10px; background-color: {success_color};
            border-radius: 50%; display: inline-block; margin-right: 5px;
            animation: pulse-green 2s infinite;
        }}

        .status-dot-busy {{
            height: 10px; width: 10px; background-color: {warning_color};
            border-radius: 50%; display: inline-block; margin-right: 5px;
        }}
        
        .status-dot-offline {{
             height: 10px; width: 10px; background-color: {muted_text};
             border-radius: 50%; display: inline-block; margin-right: 5px;
        }}

        /* --- LOGIN PAGE --- */
        .login-box {{
            background: var(--card-bg); backdrop-filter: blur(24px); padding: 48px;
            border-radius: 24px; box-shadow: 0 25px 50px -12px rgba(0,0,0,0.25);
            border: 1px solid var(--border-color); width: 100%; max-width: 450px;
        }}

        /* --- TIMER --- */
        .timer-box-normal, .timer-box-alert {{
            background: var(--secondary-bg); border-radius: 12px; padding: 16px;
            text-align: center; font-weight: 700; font-size: 1.8rem; margin-top: 16px;
            border: 2px solid; font-family: 'JetBrains Mono', monospace;
        }}
        .timer-box-normal {{ border-color: var(--success-color); color: var(--success-color); }}
        .timer-box-alert {{ border-color: var(--error-color); color: var(--error-color); animation: pulse 1s infinite; }}

        @keyframes pulse {{
            0% {{ transform: scale(1); }} 50% {{ transform: scale(1.02); }} 100% {{ transform: scale(1); }}
        }}
        
        /* --- CAMPAIGNS --- */
        .campaign-active {{ border: 2px solid {success_color}; }}
        .campaign-expired {{ filter: grayscale(1); opacity: 0.6; }}

    </style>
    """, unsafe_allow_html=True)

def product_card_html(name, price, stock, category, currency_symbol="₹", image_data=None):
    if stock < 5:
        badge_class = "badge-danger"
        stock_text = f"Low: {stock}"
    else:
        badge_class = "badge-success"
        stock_text = f"Stock: {stock}"
    
    # Handle Image
    if image_data:
        try:
            b64_img = base64.b64encode(image_data).decode('utf-8')
            visual = f'<img src="data:image/png;base64,{b64_img}" class="product-img" />'
        except:
            visual = f'<div class="product-icon">📦</div>'
    else:
        icon_map = {
            "Electronics": "💻", "Groceries": "🥦", "Beverages": "🥤",
            "Fashion": "👕", "Stationery": "✏️", "Health": "💊",
            "Snacks": "🍟", "Dairy": "🧀", "Bakery": "🥐", "Frozen": "🧊"
        }
        icon = icon_map.get(category, "📦")
        visual = f'<div class="product-icon">{icon}</div>'

    return f"""
    <div class="product-card">
        <div>
            {visual}
            <div class="product-cat">{category}</div>
            <div class="product-title" title="{name}">{name}</div>
        </div>
        <div class="product-footer">
            <div class="product-price">{currency_symbol}{price:,.0f}</div>
            <div class="badge {badge_class}">{stock_text}</div>
        </div>
    </div>
    """
