from __future__ import annotations

import streamlit as st


def render_kpi_cards(kpis: list[tuple[str, str]]) -> None:
    if not kpis:
        st.info("No KPI data available")
        return

    max_columns = 3
    for start in range(0, len(kpis), max_columns):
        chunk = kpis[start : start + max_columns]
        cols = st.columns(len(chunk))
        for col, (label, value) in zip(cols, chunk):
            with col:
                st.markdown(
                    f"""
                    <div class=\"bi-kpi-card\">
                      <div class=\"bi-kpi-label\">{label}</div>
                      <div class=\"bi-kpi-value\">{value}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
