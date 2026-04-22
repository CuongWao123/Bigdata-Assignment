from __future__ import annotations

import traceback

import pandas as pd
import streamlit as st

from components.charts import bar
from components.insight_card import render_insight_card
from components.insights import (
    analyze_avg_payment_by_type,
    analyze_installments,
    analyze_payment_orders,
)
from components.layout import render_section_title


def _filter_payment(df: pd.DataFrame, payment_type: str) -> pd.DataFrame:
    if df.empty or payment_type == "All" or "payment_type" not in df.columns:
        return df
    return df[df["payment_type"] == payment_type]


def render(tables: dict[str, pd.DataFrame], filters: dict) -> None:
    try:
        _render(tables, filters)
    except Exception:
        st.error("Something went wrong loading this page. Please check the data and try refreshing.")
        st.code(traceback.format_exc())


def _render(tables: dict[str, pd.DataFrame], filters: dict) -> None:
    render_section_title("Payment Analytics")
    st.caption("Understand how customers prefer to pay and how payment choices relate to order size.")

    payment_type = filters["payment_type"]

    orders_by_payment = _filter_payment(tables.get("orders_by_payment_type", pd.DataFrame()).copy(), payment_type)
    revenue_by_payment = _filter_payment(tables.get("revenue_by_payment_type", pd.DataFrame()).copy(), payment_type)
    installments_distribution = tables.get("installments_distribution", pd.DataFrame()).copy()
    avg_payment_by_type = _filter_payment(tables.get("avg_payment_by_type", pd.DataFrame()).copy(), payment_type)

    col1, col2 = st.columns(2)

    with col1:
        fig = bar(orders_by_payment, "payment_type", "order_count", "Orders by Payment Type")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Which payment methods are most popular. 'Credit card' usually leads in e-commerce.")
        else:
            st.info("Orders by payment type data is not available")

        pay_ord_insight = analyze_payment_orders(orders_by_payment)
        if pay_ord_insight:
            render_insight_card(pay_ord_insight)

    with col2:
        fig = bar(revenue_by_payment, "payment_type", "revenue", "Revenue by Payment Type")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Total revenue split by payment method. Compare with order counts to see which method drives bigger baskets.")
        else:
            st.info("Revenue by payment type data is not available")

    col3, col4 = st.columns(2)

    with col3:
        fig = bar(
            installments_distribution,
            "payment_installments",
            "count",
            "Installments Distribution",
        )
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("How many monthly payments customers choose. More installments may mean bigger-ticket purchases.")
        else:
            st.info("Installments distribution data is not available")

        inst_insight = analyze_installments(installments_distribution)
        if inst_insight:
            render_insight_card(inst_insight)

    with col4:
        fig = bar(
            avg_payment_by_type,
            "payment_type",
            "avg_payment_value",
            "Average Payment by Payment Type",
        )
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Average transaction size for each payment method. Higher averages may indicate preferred methods for expensive items.")
        else:
            st.info("Average payment by type data is not available")

        avg_pay_insight = analyze_avg_payment_by_type(avg_payment_by_type)
        if avg_pay_insight:
            render_insight_card(avg_pay_insight)
