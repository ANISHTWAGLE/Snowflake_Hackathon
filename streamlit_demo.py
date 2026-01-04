import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(page_title="Stock Management", layout="wide")

from snowflake.snowpark.context import get_active_session
session = get_active_session()

st.title("🏥 Advanced Stock Management System")

# ---------------- TABS ----------------
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Heatmap", "⚠️ Alerts", "📈 Forecasts", "📋 Reorders"
])

# ---------- TAB 1: Heatmap ----------
with tab1:
    st.header("Stock Health Heatmap")
    health_df = session.table("stock_health_metrics").to_pandas()
    
    heatmap_data = health_df.pivot_table(
        index='ITEM_NAME',
        columns='LOCATION',
        values='DAYS_UNTIL_STOCKOUT',
        aggfunc='first'
    )
    
    fig = px.imshow(
        heatmap_data,
        color_continuous_scale='RdYlGn',
        aspect="auto"
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------- TAB 2: Alerts ----------
with tab2:
    st.header("⚠️ Real-Time Alerts")
    
    alerts_df = session.sql("""
        SELECT *
        FROM stock_alerts
        ORDER BY alert_timestamp DESC
        LIMIT 50
    """).to_pandas()
    
    if len(alerts_df) > 0:
        critical_count = len(alerts_df[alerts_df['STOCK_STATUS'] == 'CRITICAL'])
        st.metric("Critical Alerts (Last 24h)", critical_count)
        st.dataframe(alerts_df, use_container_width=True)
    else:
        st.info("No alerts in the system")

# ---------- TAB 3: Forecasts ----------
with tab3:
    st.header("📈 14-Day Demand Forecast")
    
    col1, col2 = st.columns(2)
    with col1:
        f_location = st.selectbox(
            "Select Location",
            health_df['LOCATION'].unique()
        )
    with col2:
        f_items = health_df[health_df['LOCATION'] == f_location]['ITEM_NAME'].unique()
        f_item = st.selectbox("Select Item", f_items)
    
    if st.button("Generate Forecast"):
        forecast_df = session.call(
            'forecast_demand',
            f_location,
            f_item
        ).to_pandas()
        
        if len(forecast_df) > 0:
            st.line_chart(
                forecast_df.set_index("FORECAST_DATE")["PREDICTED_USAGE"]
            )
        else:
            st.warning("Not enough historical data for forecast")

# ---------- TAB 4: Reorders ----------
with tab4:
    st.header("📋 Reorder Recommendations")
    reorder_df = session.table("reorder_recommendations").to_pandas()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Items to Reorder", len(reorder_df))
    with col2:
        st.metric(
            "Total Units",
            f"{reorder_df['RECOMMENDED_REORDER_QTY'].sum():,.0f}"
        )
    
    st.dataframe(reorder_df, use_container_width=True)
    
    csv = reorder_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Purchase Order",
        csv,
        "reorder_list.csv",
        "text/csv"
    )
