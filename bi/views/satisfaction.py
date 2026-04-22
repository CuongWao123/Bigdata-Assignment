from __future__ import annotations

import traceback

import pandas as pd
import streamlit as st

from components.charts import bar, scatter
from components.insight_card import render_insight_card
from components.insights import (
    analyze_delay_vs_review,
    analyze_review_distribution,
    analyze_review_score_by_state,
)
from components.layout import render_section_title


def _filter_state(df: pd.DataFrame, state: str) -> pd.DataFrame:
    if df.empty or state == "All" or "customer_state" not in df.columns:
        return df
    return df[df["customer_state"] == state]


def _filter_category(df: pd.DataFrame, category: str) -> pd.DataFrame:
    if df.empty or category == "All" or "product_category_name" not in df.columns:
        return df
    return df[df["product_category_name"] == category]


def render(tables: dict[str, pd.DataFrame], filters: dict) -> None:
    try:
        _render(tables, filters)
    except Exception:
        st.error("Something went wrong loading this page. Please check the data and try refreshing.")
        st.code(traceback.format_exc())


def _render(tables: dict[str, pd.DataFrame], filters: dict) -> None:
    render_section_title("Customer Experience")
    st.caption("Reviews are scored 1 (very dissatisfied) to 5 (very satisfied). Use these charts to spot quality and delivery issues.")

    state = filters["state"]
    category = filters["category"]

    review_score_distribution = tables.get("review_score_distribution", pd.DataFrame()).copy()
    review_score_by_state = _filter_state(tables.get("review_score_by_state", pd.DataFrame()).copy(), state)
    review_score_by_category = _filter_category(
        tables.get("review_score_by_category", pd.DataFrame()).copy(),
        category,
    )
    delay_vs_review = tables.get("delay_vs_review_score", pd.DataFrame()).copy()

    col1, col2 = st.columns(2)

    with col1:
        fig = bar(
            review_score_distribution,
            "review_score",
            "count",
            "Review Score Distribution",
        )
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("How many customers gave each score. A healthy business shows most scores at 4 and 5.")
        else:
            st.info("Review score distribution is not available")

        dist_insight = analyze_review_distribution(review_score_distribution)
        if dist_insight:
            render_insight_card(dist_insight)

    with col2:
        fig = bar(
            review_score_by_state,
            "customer_state",
            "avg_review_score",
            "Review Score by State",
        )
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Average customer satisfaction by state. Lower scores may signal shipping or service issues in that region.")
        else:
            st.info("Review score by state is not available")

        state_rev_insight = analyze_review_score_by_state(review_score_by_state)
        if state_rev_insight:
            render_insight_card(state_rev_insight)

    col3, col4 = st.columns(2)

    with col3:
        fig = bar(
            review_score_by_category,
            "product_category_name",
            "avg_review_score",
            "Review Score by Category",
        )
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Which product categories get the best and worst reviews. Prioritize improving the lowest-rated ones.")
        else:
            st.info("Review score by category is not available")

    with col4:
        fig = scatter(
            delay_vs_review,
            "review_score",
            "avg_delay_days",
            "Delay vs Review Score",
        )
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Each dot is a review score. Points farther right with higher delays suggest late delivery hurts satisfaction.")

            delay_rev_insight = analyze_delay_vs_review(delay_vs_review)
            if delay_rev_insight:
                render_insight_card(delay_rev_insight)
        else:
            st.info("Delay vs review score is not available")
