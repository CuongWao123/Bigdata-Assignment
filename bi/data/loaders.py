from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from config.settings import AGGREGATES_DIR, CACHE_TTL_SECONDS, EXPECTED_AGGREGATE_FILES


NUMERIC_COLUMNS = [
    "revenue",
    "order_count",
    "avg_order_value",
    "avg_payment_value",
    "avg_freight",
    "avg_price",
    "avg_delay_days",
    "late_delivery_rate",
    "count",
    "review_score",
    "avg_review_score",
    "payment_installments",
    "total_revenue",
    "total_orders",
]

DATE_COLUMNS = [
    "purchase_date",
]


TABLE_DATE_COLUMNS = {
    "orders_per_day": ["purchase_date"],
    "delay_trend_over_time": ["purchase_date"],
}


TABLE_NUMERIC_COLUMNS = {
    "kpi_summary": [
        "total_revenue",
        "total_orders",
        "avg_order_value",
        "avg_review_score",
        "late_delivery_rate",
    ],
    "average_payment_value": ["average_payment_value", "avg_payment_value"],
    "orders_per_day": ["order_count"],
    "order_status_breakdown": ["order_count"],
    "orders_by_state": ["order_count"],
    "avg_order_value_by_state": ["avg_order_value"],
    "revenue_by_state": ["revenue"],
    "revenue_by_category": ["revenue"],
    "top_cities_by_revenue": ["revenue"],
    "top_products": ["revenue"],
    "top_sellers": ["revenue"],
    "freight_vs_price_by_category": ["avg_price", "avg_freight"],
    "orders_by_payment_type": ["order_count"],
    "revenue_by_payment_type": ["revenue"],
    "installments_distribution": ["payment_installments", "count"],
    "avg_payment_by_type": ["avg_payment_value"],
    "review_distribution": ["count"],
    "review_score_distribution": ["review_score", "count"],
    "review_score_by_state": ["avg_review_score"],
    "review_score_by_category": ["avg_review_score"],
    "late_delivery_by_state": ["total_rows", "late_rows", "late_delivery_rate", "unique_orders"],
    "on_time_vs_late_delivery": ["count"],
    "average_delay_by_state": ["avg_delay_days"],
    "delay_trend_over_time": ["avg_delay_days"],
    "delay_vs_review_score": ["review_score", "avg_delay_days"],
    "decision_support": ["positive_review_ratio", "negative_review_ratio"],
}


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _normalize_frame_by_table(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    normalized = _normalize_frame(df)

    for col in TABLE_DATE_COLUMNS.get(table_name, []):
        if col in normalized.columns:
            normalized[col] = pd.to_datetime(normalized[col], errors="coerce")

    for col in TABLE_NUMERIC_COLUMNS.get(table_name, []):
        if col in normalized.columns:
            normalized[col] = pd.to_numeric(normalized[col], errors="coerce")

    return normalized


@st.cache_data(ttl=CACHE_TTL_SECONDS)
def load_all_aggregates(aggregates_dir: str = str(AGGREGATES_DIR)) -> dict[str, pd.DataFrame]:
    base = Path(aggregates_dir)
    tables: dict[str, pd.DataFrame] = {}

    for filename in EXPECTED_AGGREGATE_FILES:
        path = base / filename
        key = path.stem
        if path.exists():
            df = pd.read_csv(path)
            tables[key] = _normalize_frame_by_table(df, key)
        else:
            tables[key] = pd.DataFrame()

    return tables


def get_missing_files(aggregates_dir: Path = AGGREGATES_DIR) -> list[str]:
    missing = []
    for filename in EXPECTED_AGGREGATE_FILES:
        if not (aggregates_dir / filename).exists():
            missing.append(filename)
    return missing


def get_last_update_timestamp(aggregates_dir: Path = AGGREGATES_DIR) -> str:
    latest_mtime = None

    for filename in EXPECTED_AGGREGATE_FILES:
        path = aggregates_dir / filename
        if not path.exists():
            continue
        current_mtime = path.stat().st_mtime
        latest_mtime = current_mtime if latest_mtime is None else max(latest_mtime, current_mtime)

    if latest_mtime is None:
        return "No aggregate files found"

    return pd.Timestamp(latest_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S")
