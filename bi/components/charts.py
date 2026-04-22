from __future__ import annotations

import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure

PLOTLY_LAYOUT = {
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "#ffffff",
    "margin": {"l": 24, "r": 24, "t": 60, "b": 30},
    "legend": {"orientation": "h", "y": -0.2},
    "font": {"color": "#10253d", "family": "Source Sans 3, sans-serif"},
    "title": {"font": {"color": "#0f4c81", "size": 18}},
    "xaxis": {
        "title": {"font": {"color": "#10253d"}},
        "tickfont": {"color": "#10253d"},
        "gridcolor": "#e6edf5",
    },
    "yaxis": {
        "title": {"font": {"color": "#10253d"}},
        "tickfont": {"color": "#10253d"},
        "gridcolor": "#e6edf5",
    },
    "hoverlabel": {
        "bgcolor": "#ffffff",
        "font": {"color": "#10253d"},
        "bordercolor": "#d6e2ef",
    },
}

BLUE_SCALE = ["#0f4c81", "#164f86", "#2f6da3", "#4e79a7", "#8ab6d6"]


def _is_chart_ready(df: pd.DataFrame, columns: list[str]) -> bool:
    if df.empty:
        return False
    return all(col in df.columns for col in columns)


def bar(
    df: pd.DataFrame, x: str, y: str, title: str, color: str | None = None
) -> Figure | None:
    required = [x, y]
    if color:
        required.append(color)

    if not _is_chart_ready(df, required):
        return None

    fig = px.bar(df, x=x, y=y, color=color, title=title, color_discrete_sequence=BLUE_SCALE)
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig


def line(df: pd.DataFrame, x: str, y: str, title: str) -> Figure | None:
    if not _is_chart_ready(df, [x, y]):
        return None

    fig = px.line(df, x=x, y=y, title=title)
    fig.update_traces(line={"color": "#0f4c81", "width": 2.8})
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig


def pie(df: pd.DataFrame, names: str, values: str, title: str) -> Figure | None:
    if not _is_chart_ready(df, [names, values]):
        return None

    fig = px.pie(df, names=names, values=values, title=title, color_discrete_sequence=BLUE_SCALE)
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig


def scatter(
    df: pd.DataFrame, x: str, y: str, title: str, color: str | None = None
) -> Figure | None:
    required = [x, y]
    if color:
        required.append(color)

    if not _is_chart_ready(df, required):
        return None

    fig = px.scatter(df, x=x, y=y, color=color, title=title, color_discrete_sequence=BLUE_SCALE)
    fig.update_layout(**PLOTLY_LAYOUT)
    return fig
