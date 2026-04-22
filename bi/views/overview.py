from __future__ import annotations

import traceback

import pandas as pd
import streamlit as st

from components.charts import bar, line
from components.insight_card import render_insight_card
from components.insights import analyze_kpi_summary, analyze_order_status, analyze_orders_per_day
from components.kpi import render_kpi_cards
from components.layout import render_section_title
from utils.formatting import currency, integer, percent


def render(tables: dict[str, pd.DataFrame], filters: dict) -> None:
    try:
        _render(tables, filters)
    except Exception:
        st.error("Something went wrong loading this page. Please check the data and try refreshing.")
        st.code(traceback.format_exc())


def _render(tables: dict[str, pd.DataFrame], filters: dict) -> None:
    render_section_title("Executive Summary")
    st.caption(
        "These KPIs reflect your selected date range and filters. "
        "Use the sidebar to narrow down by state, category, or payment type."
    )

    kpi_df = tables.get("kpi_summary", pd.DataFrame())
    avg_payment_df = tables.get("average_payment_value", pd.DataFrame())

    if kpi_df.empty:
        st.warning("KPI data is not available")
        return

    kpi_row = kpi_df.iloc[0]
    avg_payment_value = None
    if not avg_payment_df.empty:
        payment_col = None
        if "average_payment_value" in avg_payment_df.columns:
            payment_col = "average_payment_value"
        elif "avg_payment_value" in avg_payment_df.columns:
            payment_col = "avg_payment_value"

        if payment_col:
            avg_payment_value = avg_payment_df.iloc[0][payment_col]

    render_kpi_cards(
        [
            ("Total Revenue", currency(kpi_row.get("total_revenue"))),
            ("Total Orders", integer(kpi_row.get("total_orders"))),
            ("Average Order Value", currency(kpi_row.get("avg_order_value"))),
            ("Average Payment Value", currency(avg_payment_value)),
            ("Late Delivery Rate", percent(kpi_row.get("late_delivery_rate"))),
        ]
    )

    # KPI Insight
    kpi_insight = analyze_kpi_summary(kpi_df)
    if kpi_insight:
        render_insight_card(kpi_insight)

    col1, col2 = st.columns(2)

    with col1:
        orders_per_day = tables.get("orders_per_day", pd.DataFrame()).copy()
        if not orders_per_day.empty:
            if "purchase_date" in orders_per_day.columns:
                orders_per_day = orders_per_day[
                    (orders_per_day["purchase_date"] >= filters["date_start"])
                    & (orders_per_day["purchase_date"] <= filters["date_end"])
                ]

            fig = line(orders_per_day, "purchase_date", "order_count", "Orders per Day")
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
                st.caption("Daily order volume helps spot trends, seasonality, and growth patterns.")
            else:
                st.info("Orders per day data is not available")
        else:
            st.info("Orders per day data is not available")

        # Orders per day insight
        opd_insight = analyze_orders_per_day(orders_per_day)
        if opd_insight:
            render_insight_card(opd_insight)

    with col2:
        status_df = tables.get("order_status_breakdown", pd.DataFrame())
        fig = bar(status_df, "order_status", "order_count", "Order Status Breakdown")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Most orders should be 'delivered'. A high share of 'canceled' or 'unavailable' may signal issues.")
        else:
            st.info("Order status data is not available")

        # Order status insight
        status_insight = analyze_order_status(status_df)
        if status_insight:
            render_insight_card(status_insight)

    insight_df = tables.get("decision_support", pd.DataFrame())
    if not insight_df.empty:
        render_section_title("Decision Support Snapshot")
        st.caption("Quick business highlights drawn from the current data snapshot.")

        rename_map = {
            "best_state_by_revenue": "Best State by Revenue",
            "top_category_by_revenue": "Top Category",
            "top_payment_type": "Most Popular Payment",
            "worst_state_by_late_rate": "Worst State for Late Delivery",
            "positive_review_ratio": "Happy Customers %",
            "negative_review_ratio": "Unhappy Customers %",
        }

        display_df = insight_df.copy()
        display_df.columns = [
            rename_map.get(col, col) for col in display_df.columns
        ]

        # Format percentage columns
        for col in ["Happy Customers %", "Unhappy Customers %"]:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{float(x) * 100:.1f}%" if pd.notna(x) else "N/A"
                )

        st.dataframe(display_df, use_container_width=True, hide_index=True)
