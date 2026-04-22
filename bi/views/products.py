from __future__ import annotations

import traceback

import pandas as pd
import streamlit as st

from components.charts import bar, scatter
from components.insight_card import render_insight_card
from components.insights import (
    analyze_freight_vs_price,
    analyze_review_score_by_category,
    analyze_revenue_by_category,
)
from components.layout import render_section_title


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
    render_section_title("Product Performance")
    st.caption(
        "Understand which product categories drive the most revenue and how pricing relates to shipping costs."
    )

    category = filters["category"]

    revenue_by_category = _filter_category(tables.get("revenue_by_category", pd.DataFrame()).copy(), category)
    review_by_category = _filter_category(tables.get("review_score_by_category", pd.DataFrame()).copy(), category)
    freight_vs_price = _filter_category(
        tables.get("freight_vs_price_by_category", pd.DataFrame()).copy(),
        category,
    )

    col1, col2 = st.columns(2)

    with col1:
        fig = bar(revenue_by_category, "product_category_name", "revenue", "Revenue by Product Category")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Which product categories bring in the most money. Wider bars mean bigger revenue.")
        else:
            st.info("Revenue by category data is not available")

        rev_insight = analyze_revenue_by_category(revenue_by_category)
        if rev_insight:
            render_insight_card(rev_insight)

    with col2:
        fig = bar(
            revenue_by_category.head(10),
            "product_category_name",
            "revenue",
            "Top 10 Categories by Revenue",
        )
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("The 10 highest-earning categories. Focus marketing and inventory on these.")
        else:
            st.info("Top categories data is not available")

    col3, col4 = st.columns(2)

    with col3:
        fig = bar(
            review_by_category,
            "product_category_name",
            "avg_review_score",
            "Average Review Score by Category",
        )
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Customer satisfaction by category on a 1–5 scale. Higher bars mean happier buyers.")
        else:
            st.info("Review score by category data is not available")

        revcat_insight = analyze_review_score_by_category(review_by_category)
        if revcat_insight:
            render_insight_card(revcat_insight)

    with col4:
        if not freight_vs_price.empty and "avg_price" in freight_vs_price.columns:
            price_by_category = freight_vs_price[["product_category_name", "avg_price"]].copy()
            price_by_category = price_by_category.sort_values(by="avg_price", ascending=False)
            fig = bar(
                price_by_category,
                "product_category_name",
                "avg_price",
                "Average Price by Category",
            )
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)
                st.caption("Typical price level for each category. Helps set competitive pricing.")
            else:
                st.info("Price by category data is not available")
        else:
            st.info("Price by category data is not available")

    fig = scatter(
        freight_vs_price,
        "avg_price",
        "avg_freight",
        "Freight vs Price by Category",
        color="product_category_name",
    )
    if fig is not None:
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            "Each dot is a product category. "
            "This shows whether higher-priced items also have higher shipping costs. "
            "A diagonal trend means freight scales with price; outliers may reveal inefficient logistics."
        )

        freight_insight = analyze_freight_vs_price(freight_vs_price)
        if freight_insight:
            render_insight_card(freight_insight)
    else:
        st.info("Freight vs price data is not available")
