from __future__ import annotations

import streamlit as st


def render_insight_card(insight: dict) -> None:
    """
    Render a bilingual professional insight card.

    insight = {
        "analysis": {"en": "...", "vi": "..."},
        "forecast": {"en": "...", "vi": "..."},
        "decision": {"en": "...", "vi": "..."},
    }
    """
    if not insight:
        return

    analysis = insight.get("analysis", {})
    forecast = insight.get("forecast", {})
    decision = insight.get("decision", {})

    with st.container():
        st.markdown("---")

        # Analysis
        if analysis:
            st.markdown(
                "<div style='font-size:0.95rem; color:#0f4c81; font-weight:700; margin-bottom:4px;'>"
                "📊 ANALYSIS"
                "</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='font-size:0.92rem; color:#10253d; margin-bottom:12px;'>"
                f"{analysis.get('en', '')}"
                f"</div>",
                unsafe_allow_html=True,
            )
            st.caption(f"🌐{analysis.get('vi', '')}")

        # Forecast
        if forecast:
            st.markdown(
                "<div style='font-size:0.95rem; color:#2a9d8f; font-weight:700; margin-bottom:4px; margin-top:8px;'>"
                "🔮 FORECAST"
                "</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='font-size:0.92rem; color:#10253d; margin-bottom:12px;'>"
                f"{forecast.get('en', '')}"
                f"</div>",
                unsafe_allow_html=True,
            )
            st.caption(f"🌐{forecast.get('vi', '')}")

        # Decision
        if decision:
            st.markdown(
                "<div style='font-size:0.95rem; color:#e76f51; font-weight:700; margin-bottom:4px; margin-top:8px;'>"
                "✅ DECISION"
                "</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='font-size:0.92rem; color:#10253d; margin-bottom:8px;'>"
                f"{decision.get('en', '')}"
                f"</div>",
                unsafe_allow_html=True,
            )
            st.caption(f"🌐{decision.get('vi', '')}")


def render_kpi_insight(label: str, value: str, insight: dict) -> None:
    """
    Render a KPI card with an attached insight.
    """
    if not insight:
        return

    analysis = insight.get("analysis", {})
    with st.container():
        st.markdown(
            f"""
            <div style='background:linear-gradient(140deg, #ffffff 0%, #f6fbff 100%);
                        border:1px solid #d6e2ef; border-radius:14px; padding:0.9rem;
                        min-height:120px; box-shadow:0 4px 14px rgba(13,52,85,0.05); margin-bottom:8px;'>
              <div style='color:#3f5669; font-weight:600; font-size:0.93rem; line-height:1.3;'>{label}</div>
              <div style='font-family:"Space Grotesk", sans-serif; color:#0f4c81;
                          font-size:1.65rem; font-weight:700; margin-top:0.35rem;'>{value}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if analysis:
            st.caption(f"📊 {analysis.get('en', '')}")
            st.caption(f"🌐 {analysis.get('vi', '')}")
