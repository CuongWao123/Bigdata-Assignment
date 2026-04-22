from pathlib import Path

import pandas as pd

from config.settings import AGGREGATES_DIR
from data.loaders import get_missing_files, load_all_aggregates


def test_expected_files_present():
    missing = get_missing_files(Path(AGGREGATES_DIR))
    assert not missing, f"Missing aggregate files: {missing}"


def test_load_all_aggregates_contains_kpi():
    tables = load_all_aggregates()
    assert "kpi_summary" in tables
    assert not tables["kpi_summary"].empty


def test_critical_tables_have_required_columns():
    tables = load_all_aggregates()

    required_columns = {
        "kpi_summary": {"total_revenue", "total_orders", "avg_order_value", "late_delivery_rate"},
        "orders_per_day": {"purchase_date", "order_count"},
        "delay_trend_over_time": {"purchase_date", "avg_delay_days"},
        "late_delivery_by_state": {"customer_state", "late_delivery_rate"},
    }

    for table_name, expected_cols in required_columns.items():
        df = tables.get(table_name)
        assert df is not None, f"Missing table key: {table_name}"
        assert not df.empty, f"Table is empty: {table_name}"
        missing = expected_cols - set(df.columns)
        assert not missing, f"Table {table_name} missing columns: {sorted(missing)}"


def test_purchase_date_columns_are_datetime():
    tables = load_all_aggregates()

    for table_name in ["orders_per_day", "delay_trend_over_time"]:
        df = tables.get(table_name, pd.DataFrame())
        assert not df.empty, f"Table is empty: {table_name}"
        assert "purchase_date" in df.columns, f"purchase_date missing in {table_name}"
        assert pd.api.types.is_datetime64_any_dtype(df["purchase_date"]), (
            f"purchase_date must be datetime in {table_name}"
        )
