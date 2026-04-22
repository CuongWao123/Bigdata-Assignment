from __future__ import annotations

import traceback

import pandas as pd
import streamlit as st

from components.charts import bar
from components.insight_card import render_insight_card
from components.insights import (
    analyze_avg_order_value_by_state,
    analyze_orders_by_state,
    analyze_revenue_by_state,
    analyze_top_cities,
)
from components.layout import render_section_title


def _filter_state(df: pd.DataFrame, state: str) -> pd.DataFrame:
    if df.empty or state == "All" or "customer_state" not in df.columns:
        return df
    return df[df["customer_state"] == state]


def render(tables: dict[str, pd.DataFrame], filters: dict) -> None:
    try:
        _render(tables, filters)
    except Exception:
        st.error("Something went wrong loading this page. Please check the data and try refreshing.")
        st.code(traceback.format_exc())


def _render(tables: dict[str, pd.DataFrame], filters: dict) -> None:
    render_section_title("Geographic Insights")
    st.caption("See how sales and order value vary across Brazilian states and cities.")

    state = filters["state"]

    revenue_by_state = _filter_state(tables.get("revenue_by_state", pd.DataFrame()).copy(), state)
    orders_by_state = _filter_state(tables.get("orders_by_state", pd.DataFrame()).copy(), state)
    avg_order_by_state = _filter_state(tables.get("avg_order_value_by_state", pd.DataFrame()).copy(), state)
    top_cities = tables.get("top_cities_by_revenue", pd.DataFrame()).copy()

    col1, col2 = st.columns(2)
    with col1:
        fig = bar(revenue_by_state, "customer_state", "revenue", "Revenue by State")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Total sales revenue by state. Taller bars mean bigger markets.")
        else:
            st.info("Revenue by state data is not available")

        rev_insight = analyze_revenue_by_state(revenue_by_state)
        if rev_insight:
            render_insight_card(rev_insight)

    with col2:
        fig = bar(orders_by_state, "customer_state", "order_count", "Orders by State")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Total number of orders by state. Compare with revenue to spot high-volume vs high-value states.")
        else:
            st.info("Orders by state data is not available")

        ord_insight = analyze_orders_by_state(orders_by_state)
        if ord_insight:
            render_insight_card(ord_insight)

    col3, col4 = st.columns(2)
    with col3:
        fig = bar(
            avg_order_by_state,
            "customer_state",
            "avg_order_value",
            "Average Order Value by State",
        )
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("How much customers typically spend per order in each state. Higher values suggest premium buyers.")
        else:
            st.info("Average order value by state data is not available")

        aov_insight = analyze_avg_order_value_by_state(avg_order_by_state)
        if aov_insight:
            render_insight_card(aov_insight)

    with col4:
        fig = bar(top_cities.head(15), "customer_city", "revenue", "Top Cities by Revenue")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("The 15 cities generating the most revenue. Useful for targeted campaigns and logistics planning.")
        else:
            st.info("Top cities data is not available")

        city_insight = analyze_top_cities(top_cities)
        if city_insight:
            render_insight_card(city_insight)
