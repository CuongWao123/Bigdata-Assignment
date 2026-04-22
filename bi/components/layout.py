from __future__ import annotations

import streamlit as st


def render_section_title(title: str) -> None:
    st.markdown(f"<div class='bi-section-title'>{title}</div>", unsafe_allow_html=True)
