import os
from contextlib import contextmanager

import pandas as pd
import psycopg
import streamlit as st
from databricks.sdk import WorkspaceClient
from psycopg.rows import dict_row

st.set_page_config(page_title="Customer360 | Operational Analytics", page_icon="📊", layout="wide")
st.title("Customer360 | Operational Analytics")
st.caption("Olist customer intelligence · Databricks Lakeflow → Gold → Lakebase")

@st.cache_resource
def workspace_client():
    return WorkspaceClient()

@contextmanager
def db_connection():
    required = ["PGHOST", "PGDATABASE", "PGUSER", "ENDPOINT_NAME"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError("Missing app environment variables: " + ", ".join(missing))
    token = workspace_client().postgres.generate_database_credential(
        endpoint=os.environ["ENDPOINT_NAME"]
    ).token
    with psycopg.connect(
        host=os.environ["PGHOST"],
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=token,
        sslmode=os.getenv("PGSSLMODE", "require"),
        connect_timeout=12,
        row_factory=dict_row,
        options="-c default_transaction_read_only=on",
    ) as conn:
        yield conn

def query(sql, params=()):
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            return pd.DataFrame(cursor.fetchall())

@st.cache_data(ttl=180, show_spinner=False)
def overview():
    return query("""
        SELECT (SELECT COUNT(*) FROM gold.dim_customer_360_synced) AS customers,
               (SELECT COUNT(*) FROM gold.fact_orders_synced) AS orders,
               (SELECT COALESCE(SUM(total_payment_value),0) FROM gold.fact_orders_synced) AS revenue
    """)

@st.cache_data(ttl=180, show_spinner=False)
def find_customers(search):
    return query("""
        SELECT customer_unique_id, latest_customer_id, customer_city, customer_state
        FROM gold.dim_customer_360_synced
        WHERE customer_unique_id ILIKE %s
           OR latest_customer_id ILIKE %s
           OR customer_city ILIKE %s
        ORDER BY customer_unique_id LIMIT 50
    """, tuple([f"%{search}%"] * 3))

@st.cache_data(ttl=180, show_spinner=False)
def customer_profile(customer_id):
    return query("""
        SELECT d.customer_unique_id, d.latest_customer_id,
               d.customer_city, d.customer_state, d.customer_zip_code_prefix,
               m.total_orders, m.lifetime_payment_value, m.average_order_payment,
               m.first_order_timestamp, m.last_order_timestamp,
               m.delivered_orders, m.canceled_orders,
               r.recency_days, r.frequency, r.monetary_value,
               r.recency_score, r.frequency_score, r.monetary_score, r.rfm_segment
        FROM gold.dim_customer_360_synced d
        LEFT JOIN gold.customer_revenue_summary_synced m USING (customer_unique_id)
        LEFT JOIN gold.customer_rfm_synced r USING (customer_unique_id)
        WHERE d.customer_unique_id = %s
        LIMIT 1
    """, (customer_id,))

@st.cache_data(ttl=180, show_spinner=False)
def customer_orders(customer_id):
    return query("""
        SELECT o.order_id, o.order_status, o.order_purchase_timestamp,
               o.order_delivered_customer_date, o.total_payment_value,
               o.item_count, o.items_subtotal, o.total_freight
        FROM gold.customer_order_mapping_synced cm
        JOIN gold.fact_orders_synced o ON o.customer_id = cm.customer_id
        WHERE cm.customer_unique_id = %s
        ORDER BY o.order_purchase_timestamp DESC NULLS LAST, o.order_id
        LIMIT 500
    """, (customer_id,))

@st.cache_data(ttl=180, show_spinner=False)
def order_items(order_id):
    return query("""
        SELECT order_item_id, product_id, seller_id, product_category_english,
               seller_city, item_price, freight_value, item_total_value
        FROM gold.fact_order_items_synced
        WHERE order_id = %s ORDER BY order_item_id
    """, (order_id,))

try:
    summary = overview().iloc[0]
    a, b, c = st.columns(3)
    a.metric("Customers", f"{int(summary['customers']):,}")
    b.metric("Orders", f"{int(summary['orders']):,}")
    c.metric("Payments (BRL)", f"R$ {float(summary['revenue']):,.2f}")
except Exception:
    st.error(
        "Unable to connect to the Customer360 serving database. "
        "Please contact the application administrator."
    )
    st.stop()

st.divider()
st.subheader("Find a customer")
search = st.text_input("Search by unique customer ID, latest customer ID, or city", placeholder="Enter at least 2 characters")
if len(search.strip()) < 2:
    st.info("Enter at least 2 characters to search customers.")
    st.stop()

try:
    matches = find_customers(search.strip())
    if matches.empty:
        st.warning("No matching customers found.")
        st.stop()
    labels = {row.customer_unique_id: f"{row.customer_unique_id} · {row.customer_city}, {row.customer_state}" for row in matches.itertuples()}
    chosen = st.selectbox("Select customer (up to 50 results)", list(labels), format_func=lambda x: labels[x])
    profile = customer_profile(chosen)
    if profile.empty:
        st.warning("No profile found for this customer.")
        st.stop()
    p = profile.iloc[0]
    st.subheader("Customer profile")
    st.write(f"**Customer ID:** `{chosen}`  |  **Location:** {p['customer_city']}, {p['customer_state']}")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Lifetime payment", f"R$ {float(p['lifetime_payment_value'] or 0):,.2f}")
    m2.metric("Total orders", f"{int(p['total_orders'] or 0):,}")
    m3.metric("RFM segment", str(p['rfm_segment']))
    m4.metric("Recency (days)", str(p['recency_days']))
    with st.expander("RFM details and customer history"):
        st.dataframe(pd.DataFrame([p.to_dict()]).T.rename(columns={0: "Value"}), use_container_width=True)
    st.subheader("Order history")
    orders = customer_orders(chosen)
    st.dataframe(orders, use_container_width=True, hide_index=True)
    if not orders.empty:
        selected_order = st.selectbox("Inspect order items", orders['order_id'].astype(str).tolist())
        items = order_items(selected_order)
        st.dataframe(items, use_container_width=True, hide_index=True)
except Exception:
    st.error(
        "Unable to retrieve customer information. "
        "Please try again later."
    )
