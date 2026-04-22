from __future__ import annotations

import traceback

import pandas as pd
import streamlit as st

from components.charts import bar, line, pie
from components.insight_card import render_insight_card
from components.insights import (
    analyze_delay_trend,
    analyze_late_delivery_by_state,
    analyze_on_time_vs_late,
)
from components.layout import render_section_title
from utils.formatting import percent


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
    render_section_title("Logistics & Fulfillment")
    st.caption(
        "Late delivery is counted when an order arrives after the estimated delivery date. "
        "Use these charts to spot problem states and track delivery trends over time."
    )

    state = filters["state"]
    date_start = filters["date_start"]
    date_end = filters["date_end"]

    on_time_vs_late = tables.get("on_time_vs_late_delivery", pd.DataFrame()).copy()
    late_by_state = _filter_state(tables.get("late_delivery_by_state", pd.DataFrame()).copy(), state)
    avg_delay_by_state = _filter_state(tables.get("average_delay_by_state", pd.DataFrame()).copy(), state)
    delay_trend = tables.get("delay_trend_over_time", pd.DataFrame()).copy()

    if not delay_trend.empty and "purchase_date" in delay_trend.columns:
        delay_trend = delay_trend[
            (delay_trend["purchase_date"] >= date_start) & (delay_trend["purchase_date"] <= date_end)
        ]

    # Summary metrics
    if not on_time_vs_late.empty and "delivery_status" in on_time_vs_late.columns and "count" in on_time_vs_late.columns:
        total = int(on_time_vs_late["count"].sum())
        on_time = int(on_time_vs_late.loc[on_time_vs_late["delivery_status"] == "on_time", "count"].sum())
        late = int(on_time_vs_late.loc[on_time_vs_late["delivery_status"] == "late", "count"].sum())
        if total > 0:
            col_kpi1, col_kpi2 = st.columns(2)
            with col_kpi1:
                st.metric("On-Time Deliveries", f"{(on_time / total) * 100:.1f}%")
            with col_kpi2:
                st.metric("Late Deliveries", f"{(late / total) * 100:.1f}%")
    else:
        st.info("Delivery summary data is not available")

    col1, col2 = st.columns(2)

    with col1:
        fig = pie(on_time_vs_late, "delivery_status", "count", "On-Time vs Late Delivery")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("The share of late deliveries. A healthy operation should keep this well below 10%.")
        else:
            st.info("On-time vs late data is not available")

        ot_insight = analyze_on_time_vs_late(on_time_vs_late)
        if ot_insight:
            render_insight_card(ot_insight)

    with col2:
        fig = bar(late_by_state, "customer_state", "late_delivery_rate", "Late Delivery by State")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("States with the highest share of late orders. Focus improvement efforts here first.")
        else:
            st.info("Late delivery by state data is not available")

        late_insight = analyze_late_delivery_by_state(late_by_state)
        if late_insight:
            render_insight_card(late_insight)

    col3, col4 = st.columns(2)

    with col3:
        fig = bar(avg_delay_by_state, "customer_state", "avg_delay_days", "Average Delay by State")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("How many days late orders typically are in each state.")
        else:
            st.info("Average delay by state data is not available")

    with col4:
        fig = line(delay_trend, "purchase_date", "avg_delay_days", "Delay Trend Over Time")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Track whether delivery performance is improving or worsening over the selected period.")
        else:
            st.info("Delay trend data is not available")

        trend_insight = analyze_delay_trend(delay_trend)
        if trend_insight:
            render_insight_card(trend_insight)
