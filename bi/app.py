from __future__ import annotations

import pandas as pd
import streamlit as st

from config.settings import AGGREGATES_DIR, APP_ICON, APP_TITLE, LAYOUT, THEME_CSS
from data.loaders import get_last_update_timestamp, load_all_aggregates
from data.refresh import refresh_aggregates
from views import delivery, geography, overview, payments, products, satisfaction
from utils.validation import validate_aggregate_files


PAGE_RENDERERS = {
    "Executive Summary": overview.render,
    "Geographic Insights": geography.render,
    "Product Performance": products.render,
    "Payment Analytics": payments.render,
    "Logistics & Fulfillment": delivery.render,
    "Customer Experience": satisfaction.render,
}


def _build_filters(tables: dict[str, pd.DataFrame]) -> dict:
    date_start = pd.Timestamp("2016-01-01")
    date_end = pd.Timestamp.today().normalize()

    orders_per_day = tables.get("orders_per_day", pd.DataFrame())
    if not orders_per_day.empty and "purchase_date" in orders_per_day.columns:
        min_date = orders_per_day["purchase_date"].min()
        max_date = orders_per_day["purchase_date"].max()
        if pd.notna(min_date):
            date_start = min_date
        if pd.notna(max_date):
            date_end = max_date

    if date_start > date_end:
        date_start, date_end = date_end, date_start

    st.sidebar.info(
        f"**Date:** {date_start.date()} to {date_end.date()}\n\n"
    )

    return {
        "date_start": date_start,
        "date_end": date_end,
        "state": "All",
        "category": "All",
        "payment_type": "All",
    }


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, page_icon=APP_ICON, layout=LAYOUT)
    st.markdown(THEME_CSS, unsafe_allow_html=True)

    validation = validate_aggregate_files(AGGREGATES_DIR)
    if validation["status"] == "error":
        st.error(
            "Missing aggregate files: " + ", ".join(validation["missing_files"])
        )
    elif validation["status"] == "warning":
        st.warning(
            "Empty aggregate files: " + ", ".join(validation["empty_files"])
        )

    st.sidebar.markdown("### Data Status")
    st.sidebar.caption(f"Last updated: {get_last_update_timestamp()}")

    if st.sidebar.button("Refresh Aggregates", use_container_width=True):
        with st.spinner("Refreshing data from analysis pipeline..."):
            ok, message = refresh_aggregates()
        load_all_aggregates.clear()
        if ok:
            st.sidebar.success("Refresh completed")
            st.success("Data refreshed successfully")
        else:
            st.sidebar.error("Refresh failed")
            st.error(message)

    tables = load_all_aggregates()
    filters = _build_filters(tables)

    selected_page = st.sidebar.radio("Navigate", list(PAGE_RENDERERS.keys()))
    PAGE_RENDERERS[selected_page](tables, filters)


if __name__ == "__main__":
    main()
