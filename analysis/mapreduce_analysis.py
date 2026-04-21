from pathlib import Path
import pandas as pd


# =========================
# Path config
# =========================
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_CSV = BASE_DIR / "output" / "stream_output.csv"


def load_stream_data(csv_path: Path = OUTPUT_CSV) -> pd.DataFrame:
    """
    Read the CSV file produced by the Kafka consumer and return a DataFrame.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"File not found: {csv_path}")

    df = pd.read_csv(csv_path)
    return df


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean data
    """
    df = df.copy()

    # =========================
    # Convert datetime columns
    # =========================
    datetime_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "shipping_limit_date",
        "review_creation_date",
        "review_answer_timestamp",
    ]

    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # =========================
    # Convert numeric columns
    # =========================
    numeric_cols = [
        "order_item_id",
        "price",
        "freight_value",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "customer_zip_code_prefix",
        "payment_sequential",
        "payment_installments",
        "payment_value",
        "review_score",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # =========================
    # Fill common string nulls
    # =========================
    string_fill_map = {
        "product_category_name": "unknown",
        "customer_city": "unknown",
        "customer_state": "unknown",
        "payment_type": "unknown",
        "review_comment_title": "",
        "review_comment_message": "",
        "order_status": "unknown",
    }

    for col, fill_value in string_fill_map.items():
        if col in df.columns:
            df[col] = df[col].fillna(fill_value)

    return df

def add_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived fields to the DataFrame for analysis.
    """
    df = df.copy()

    # =========================
    # Revenue
    # =========================
    if "price" in df.columns and "freight_value" in df.columns:
        df["price"] = df["price"].fillna(0)
        df["freight_value"] = df["freight_value"].fillna(0)
        df["revenue"] = df["price"] + df["freight_value"]
    else:
        df["revenue"] = 0

    # =========================
    # Review sentiment
    # =========================
    def classify_sentiment(score):
        if pd.isna(score):
            return "unknown"
        if score >= 4:
            return "positive"
        if score == 3:
            return "neutral"
        if score >= 1:
            return "negative"
        return "unknown"

    if "review_score" in df.columns:
        df["review_sentiment"] = df["review_score"].apply(classify_sentiment)
    else:
        df["review_sentiment"] = "unknown"

    # =========================
    # Delivery delay days
    # delivered - estimated
    # > 0 means late
    # =========================
    if (
        "order_delivered_customer_date" in df.columns
        and "order_estimated_delivery_date" in df.columns
    ):
        delay = (
            df["order_delivered_customer_date"] - df["order_estimated_delivery_date"]
        ).dt.days

        df["delivery_delay_days"] = delay.fillna(0)
    else:
        df["delivery_delay_days"] = 0

    # =========================
    # Late delivery flag
    # =========================
    df["is_late_delivery"] = (df["delivery_delay_days"] > 0).astype(int)

    return df

def get_kpi_summary(df: pd.DataFrame) -> dict:
    total_revenue = float(df["revenue"].sum()) if "revenue" in df.columns else 0.0
    total_orders = int(df["order_id"].nunique()) if "order_id" in df.columns else 0

    avg_order_value = 0.0
    if total_orders > 0:
        avg_order_value = total_revenue / total_orders

    avg_review_score = 0.0
    if "review_score" in df.columns:
        avg_review_score = float(df["review_score"].dropna().mean())

    late_delivery_rate = 0.0
    if "is_late_delivery" in df.columns and len(df) > 0:
        late_delivery_rate = float(df["is_late_delivery"].mean())

    return {
        "total_revenue": round(total_revenue, 2),
        "total_orders": total_orders,
        "avg_order_value": round(avg_order_value, 2),
        "avg_review_score": round(avg_review_score, 2),
        "late_delivery_rate": round(late_delivery_rate, 4),
    }


def get_revenue_by_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Revenue by customer state.
    """
    if "customer_state" not in df.columns or "revenue" not in df.columns:
        return pd.DataFrame(columns=["customer_state", "revenue"])

    result = (
        df.groupby("customer_state", dropna=False)["revenue"]
        .sum()
        .reset_index()
        .sort_values(by="revenue", ascending=False)
    )

    return result


def get_revenue_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Revenue by product category.
    """
    if "product_category_name" not in df.columns or "revenue" not in df.columns:
        return pd.DataFrame(columns=["product_category_name", "revenue"])

    result = (
        df.groupby("product_category_name", dropna=False)["revenue"]
        .sum()
        .reset_index()
        .sort_values(by="revenue", ascending=False)
    )

    return result

def get_orders_by_payment_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count orders by payment type.
    Uses unique order_id per payment_type to reduce duplication effects.
    """
    if "payment_type" not in df.columns or "order_id" not in df.columns:
        return pd.DataFrame(columns=["payment_type", "order_count"])

    result = (
        df.groupby("payment_type", dropna=False)["order_id"]
        .nunique()
        .reset_index(name="order_count")
        .sort_values(by="order_count", ascending=False)
    )

    return result


def get_review_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count rows by review sentiment.
    """
    if "review_sentiment" not in df.columns:
        return pd.DataFrame(columns=["review_sentiment", "count"])

    result = (
        df.groupby("review_sentiment", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(by="count", ascending=False)
    )

    return result


def get_late_delivery_by_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute late delivery metrics by customer state.
    """
    required_cols = {"customer_state", "is_late_delivery", "order_id"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(
            columns=[
                "customer_state",
                "total_rows",
                "late_rows",
                "late_delivery_rate",
                "unique_orders",
            ]
        )

    grouped = (
        df.groupby("customer_state", dropna=False)
        .agg(
            total_rows=("order_id", "size"),
            late_rows=("is_late_delivery", "sum"),
            unique_orders=("order_id", "nunique"),
        )
        .reset_index()
    )

    grouped["late_delivery_rate"] = grouped["late_rows"] / grouped["total_rows"]
    grouped = grouped.sort_values(by="late_delivery_rate", ascending=False)

    return grouped

def get_top_sellers(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Top sellers by total revenue.
    """
    required_cols = {"seller_id", "revenue"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["seller_id", "revenue"])

    result = (
        df.groupby("seller_id", dropna=False)["revenue"]
        .sum()
        .reset_index()
        .sort_values(by="revenue", ascending=False)
        .head(top_n)
    )

    return result

def get_average_payment_value(df: pd.DataFrame) -> float:
    """
    Average payment value across rows with valid payment_value.
    """
    if "payment_value" not in df.columns:
        return 0.0

    series = pd.to_numeric(df["payment_value"], errors="coerce").dropna()
    if series.empty:
        return 0.0

    return round(float(series.mean()), 2)


def get_orders_per_day(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unique orders per purchase date.
    """
    required_cols = {"order_purchase_timestamp", "order_id"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["purchase_date", "order_count"])

    working_df = df.copy()
    working_df["purchase_date"] = pd.to_datetime(
        working_df["order_purchase_timestamp"], errors="coerce"
    ).dt.date

    result = (
        working_df.dropna(subset=["purchase_date"])
        .groupby("purchase_date")["order_id"]
        .nunique()
        .reset_index(name="order_count")
        .sort_values(by="purchase_date")
    )

    return result


def get_order_status_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unique orders by order status.
    """
    required_cols = {"order_status", "order_id"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["order_status", "order_count"])

    result = (
        df.groupby("order_status", dropna=False)["order_id"]
        .nunique()
        .reset_index(name="order_count")
        .sort_values(by="order_count", ascending=False)
    )

    return result


def get_orders_by_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unique orders by customer state.
    """
    required_cols = {"customer_state", "order_id"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["customer_state", "order_count"])

    result = (
        df.groupby("customer_state", dropna=False)["order_id"]
        .nunique()
        .reset_index(name="order_count")
        .sort_values(by="order_count", ascending=False)
    )

    return result


def get_avg_order_value_by_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average order revenue by state, based on order-level deduplicated revenue.
    """
    required_cols = {"order_id", "customer_state", "revenue"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["customer_state", "avg_order_value"])

    order_revenue_by_state = (
        df.groupby(["customer_state", "order_id"], dropna=False)["revenue"]
        .sum()
        .reset_index()
    )

    result = (
        order_revenue_by_state.groupby("customer_state", dropna=False)["revenue"]
        .mean()
        .reset_index(name="avg_order_value")
        .sort_values(by="avg_order_value", ascending=False)
    )

    return result


def get_top_cities_by_revenue(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Top cities by total revenue.
    """
    required_cols = {"customer_city", "revenue"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["customer_city", "revenue"])

    result = (
        df.groupby("customer_city", dropna=False)["revenue"]
        .sum()
        .reset_index()
        .sort_values(by="revenue", ascending=False)
        .head(top_n)
    )

    return result


def get_top_products(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Top products by total revenue.
    """
    required_cols = {"product_id", "revenue"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["product_id", "revenue"])

    result = (
        df.groupby("product_id", dropna=False)["revenue"]
        .sum()
        .reset_index()
        .sort_values(by="revenue", ascending=False)
        .head(top_n)
    )

    return result


def get_freight_vs_price_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average freight and average price by product category.
    """
    required_cols = {"product_category_name", "price", "freight_value"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["product_category_name", "avg_price", "avg_freight"])

    result = (
        df.groupby("product_category_name", dropna=False)
        .agg(
            avg_price=("price", "mean"),
            avg_freight=("freight_value", "mean"),
        )
        .reset_index()
        .sort_values(by="avg_price", ascending=False)
    )

    return result


def get_revenue_by_payment_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Revenue by payment type.
    """
    required_cols = {"payment_type", "revenue"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["payment_type", "revenue"])

    result = (
        df.groupby("payment_type", dropna=False)["revenue"]
        .sum()
        .reset_index()
        .sort_values(by="revenue", ascending=False)
    )

    return result


def get_installments_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count rows by payment installments.
    """
    if "payment_installments" not in df.columns:
        return pd.DataFrame(columns=["payment_installments", "count"])

    result = (
        df.groupby("payment_installments", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(by="payment_installments")
    )

    return result


def get_avg_payment_by_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average payment value by payment type.
    """
    required_cols = {"payment_type", "payment_value"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["payment_type", "avg_payment_value"])

    result = (
        df.groupby("payment_type", dropna=False)["payment_value"]
        .mean()
        .reset_index(name="avg_payment_value")
        .sort_values(by="avg_payment_value", ascending=False)
    )

    return result


def get_on_time_vs_late_delivery(df: pd.DataFrame) -> pd.DataFrame:
    """
    On-time vs late delivery counts.
    """
    if "is_late_delivery" not in df.columns:
        return pd.DataFrame(columns=["delivery_status", "count"])

    working_df = df.copy()
    working_df["delivery_status"] = working_df["is_late_delivery"].apply(
        lambda x: "late" if x == 1 else "on_time"
    )

    result = (
        working_df.groupby("delivery_status")
        .size()
        .reset_index(name="count")
        .sort_values(by="count", ascending=False)
    )

    return result


def get_average_delay_by_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average delivery delay days by state.
    """
    required_cols = {"customer_state", "delivery_delay_days"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["customer_state", "avg_delay_days"])

    result = (
        df.groupby("customer_state", dropna=False)["delivery_delay_days"]
        .mean()
        .reset_index(name="avg_delay_days")
        .sort_values(by="avg_delay_days", ascending=False)
    )

    return result


def get_delay_trend_over_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average delay by purchase date.
    """
    required_cols = {"order_purchase_timestamp", "delivery_delay_days"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["purchase_date", "avg_delay_days"])

    working_df = df.copy()
    working_df["purchase_date"] = pd.to_datetime(
        working_df["order_purchase_timestamp"], errors="coerce"
    ).dt.date

    result = (
        working_df.dropna(subset=["purchase_date"])
        .groupby("purchase_date")["delivery_delay_days"]
        .mean()
        .reset_index(name="avg_delay_days")
        .sort_values(by="purchase_date")
    )

    return result


def get_review_score_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Distribution of review_score.
    """
    if "review_score" not in df.columns:
        return pd.DataFrame(columns=["review_score", "count"])

    result = (
        df.groupby("review_score", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(by="review_score")
    )

    return result


def get_review_score_by_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average review score by customer state.
    """
    required_cols = {"customer_state", "review_score"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["customer_state", "avg_review_score"])

    result = (
        df.groupby("customer_state", dropna=False)["review_score"]
        .mean()
        .reset_index(name="avg_review_score")
        .sort_values(by="avg_review_score", ascending=False)
    )

    return result


def get_review_score_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average review score by product category.
    """
    required_cols = {"product_category_name", "review_score"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["product_category_name", "avg_review_score"])

    result = (
        df.groupby("product_category_name", dropna=False)["review_score"]
        .mean()
        .reset_index(name="avg_review_score")
        .sort_values(by="avg_review_score", ascending=False)
    )

    return result


def get_delay_vs_review_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Average delay grouped by review score.
    """
    required_cols = {"review_score", "delivery_delay_days"}
    if not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=["review_score", "avg_delay_days"])

    result = (
        df.groupby("review_score", dropna=False)["delivery_delay_days"]
        .mean()
        .reset_index(name="avg_delay_days")
        .sort_values(by="review_score")
    )

    return result

def get_order_level_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a deduplicated order-level dataframe.
    One row per order_id, intended for cleaner top-line KPIs.
    """
    if "order_id" not in df.columns:
        return pd.DataFrame()

    working_df = df.copy()

    # Sort so the most recent available row per order is kept deterministically
    sort_cols = []
    if "order_purchase_timestamp" in working_df.columns:
        sort_cols.append("order_purchase_timestamp")
    if "review_answer_timestamp" in working_df.columns:
        sort_cols.append("review_answer_timestamp")

    if sort_cols:
        working_df = working_df.sort_values(by=sort_cols)

    # Revenue at order level:
    # sum row-level revenue per order to avoid losing multi-item orders
    if "revenue" in working_df.columns:
        order_revenue = (
            working_df.groupby("order_id", as_index=False)["revenue"]
            .sum()
            .rename(columns={"revenue": "order_revenue"})
        )
    else:
        order_revenue = pd.DataFrame(columns=["order_id", "order_revenue"])

    # Keep one representative row per order for order-level metrics
    order_level = working_df.drop_duplicates(subset=["order_id"], keep="last").copy()

    # Attach summed order revenue
    order_level = order_level.merge(order_revenue, on="order_id", how="left")

    return order_level


def get_kpi_summary_order_level(df: pd.DataFrame) -> dict:
    """
    Cleaner KPI summary using one row per order_id.
    Better suited for report/presentation than raw joined-row KPIs.
    """
    order_df = get_order_level_df(df)

    if order_df.empty:
        return {
            "total_revenue": 0.0,
            "total_orders": 0,
            "avg_order_value": 0.0,
            "avg_review_score": 0.0,
            "late_delivery_rate": 0.0,
        }

    total_orders = int(order_df["order_id"].nunique()) if "order_id" in order_df.columns else 0

    total_revenue = 0.0
    if "order_revenue" in order_df.columns:
        total_revenue = float(order_df["order_revenue"].fillna(0).sum())

    avg_order_value = 0.0
    if total_orders > 0:
        avg_order_value = total_revenue / total_orders

    avg_review_score = 0.0
    if "review_score" in order_df.columns:
        avg_review_score = float(order_df["review_score"].dropna().mean())

    late_delivery_rate = 0.0
    if "is_late_delivery" in order_df.columns and len(order_df) > 0:
        late_delivery_rate = float(order_df["is_late_delivery"].mean())

    return {
        "total_revenue": round(total_revenue, 2),
        "total_orders": total_orders,
        "avg_order_value": round(avg_order_value, 2),
        "avg_review_score": round(avg_review_score, 2),
        "late_delivery_rate": round(late_delivery_rate, 4),
    }


def get_decision_support(df: pd.DataFrame) -> dict:
    """
    Rule-based business insights for the dashboard.
    """
    insights = {
        "best_state_by_revenue": None,
        "top_category_by_revenue": None,
        "top_payment_type": None,
        "worst_state_by_late_rate": None,
        "positive_review_ratio": 0.0,
        "negative_review_ratio": 0.0,
    }

    revenue_by_state = get_revenue_by_state(df)
    revenue_by_category = get_revenue_by_category(df)
    orders_by_payment_type = get_orders_by_payment_type(df)
    review_distribution = get_review_distribution(df)
    late_delivery_by_state = get_late_delivery_by_state(df)

    if not revenue_by_state.empty:
        insights["best_state_by_revenue"] = revenue_by_state.iloc[0]["customer_state"]

    if not revenue_by_category.empty:
        insights["top_category_by_revenue"] = revenue_by_category.iloc[0]["product_category_name"]

    if not orders_by_payment_type.empty:
        insights["top_payment_type"] = orders_by_payment_type.iloc[0]["payment_type"]

    if not late_delivery_by_state.empty:
        insights["worst_state_by_late_rate"] = late_delivery_by_state.iloc[0]["customer_state"]

    total_review_rows = int(review_distribution["count"].sum()) if not review_distribution.empty else 0
    if total_review_rows > 0:
        positive_count = int(
            review_distribution.loc[
                review_distribution["review_sentiment"] == "positive", "count"
            ].sum()
        )
        negative_count = int(
            review_distribution.loc[
                review_distribution["review_sentiment"] == "negative", "count"
            ].sum()
        )

        insights["positive_review_ratio"] = round(positive_count / total_review_rows, 4)
        insights["negative_review_ratio"] = round(negative_count / total_review_rows, 4)

    return insights


def get_all_dashboard_data(df: pd.DataFrame) -> dict:
    """
    Bundle all MapReduce outputs for the Streamlit dashboard.
    """
    return {
        "kpi_summary": get_kpi_summary(df),
        "kpi_summary_order_level": get_kpi_summary_order_level(df),
        "average_payment_value": get_average_payment_value(df),
        "orders_per_day": get_orders_per_day(df),
        "order_status_breakdown": get_order_status_breakdown(df),
        "revenue_by_state": get_revenue_by_state(df),
        "orders_by_state": get_orders_by_state(df),
        "avg_order_value_by_state": get_avg_order_value_by_state(df),
        "top_cities_by_revenue": get_top_cities_by_revenue(df),
        "revenue_by_category": get_revenue_by_category(df),
        "top_products": get_top_products(df),
        "top_sellers": get_top_sellers(df),
        "freight_vs_price_by_category": get_freight_vs_price_by_category(df),
        "orders_by_payment_type": get_orders_by_payment_type(df),
        "revenue_by_payment_type": get_revenue_by_payment_type(df),
        "installments_distribution": get_installments_distribution(df),
        "avg_payment_by_type": get_avg_payment_by_type(df),
        "review_distribution": get_review_distribution(df),
        "review_score_distribution": get_review_score_distribution(df),
        "review_score_by_state": get_review_score_by_state(df),
        "review_score_by_category": get_review_score_by_category(df),
        "late_delivery_by_state": get_late_delivery_by_state(df),
        "on_time_vs_late_delivery": get_on_time_vs_late_delivery(df),
        "average_delay_by_state": get_average_delay_by_state(df),
        "delay_trend_over_time": get_delay_trend_over_time(df),
        "delay_vs_review_score": get_delay_vs_review_score(df),
        "decision_support": get_decision_support(df),
    }
    
def prepare_dashboard_data(csv_path: Path = OUTPUT_CSV) -> dict:
    """
    Full pipeline:
    load -> preprocess -> derive -> aggregate
    """
    df = load_stream_data(csv_path)
    df = preprocess_data(df)
    df = add_derived_fields(df)
    dashboard_data = get_all_dashboard_data(df)

    return {
        "raw_df": df,
        "dashboard_data": dashboard_data,
    }


def export_dashboard_data(
    dashboard_data: dict,
    export_dir: Path = BASE_DIR / "output" / "aggregates",
) -> None:
    """
    Export aggregated outputs to CSV files for debugging or dashboard integration.
    """
    export_dir.mkdir(parents=True, exist_ok=True)

    # dict output
    kpi_summary = pd.DataFrame([dashboard_data["kpi_summary"]])
    kpi_summary.to_csv(export_dir / "kpi_summary.csv", index=False)

    avg_payment_value = pd.DataFrame(
        [{"average_payment_value": dashboard_data["average_payment_value"]}]
    )
    avg_payment_value.to_csv(export_dir / "average_payment_value.csv", index=False)

    decision_support = pd.DataFrame([dashboard_data["decision_support"]])
    decision_support.to_csv(export_dir / "decision_support.csv", index=False)

    # dataframe outputs
    dataframe_keys = [
        "orders_per_day",
        "order_status_breakdown",
        "revenue_by_state",
        "orders_by_state",
        "avg_order_value_by_state",
        "top_cities_by_revenue",
        "revenue_by_category",
        "top_products",
        "top_sellers",
        "freight_vs_price_by_category",
        "orders_by_payment_type",
        "revenue_by_payment_type",
        "installments_distribution",
        "avg_payment_by_type",
        "review_distribution",
        "review_score_distribution",
        "review_score_by_state",
        "review_score_by_category",
        "late_delivery_by_state",
        "on_time_vs_late_delivery",
        "average_delay_by_state",
        "delay_trend_over_time",
        "delay_vs_review_score",
    ]

    for key in dataframe_keys:
        value = dashboard_data.get(key)
        if isinstance(value, pd.DataFrame):
            value.to_csv(export_dir / f"{key}.csv", index=False)

if __name__ == "__main__":
    result = prepare_dashboard_data()
    df = result["raw_df"]
    dashboard_data = result["dashboard_data"]

    print("Loaded, preprocessed, and aggregated successfully.")
    print(f"Shape: {df.shape}")

    print("\n=== KPI SUMMARY (ORDER LEVEL) ===")
    print(dashboard_data["kpi_summary_order_level"])

    print("\n=== AVERAGE PAYMENT VALUE ===")
    print(dashboard_data["average_payment_value"])

    print("\n=== ORDERS PER DAY ===")
    print(dashboard_data["orders_per_day"].head(5))

    print("\n=== ORDER STATUS BREAKDOWN ===")
    print(dashboard_data["order_status_breakdown"].head(10))

    print("\n=== ORDERS BY STATE ===")
    print(dashboard_data["orders_by_state"].head(10))

    print("\n=== TOP CITIES BY REVENUE ===")
    print(dashboard_data["top_cities_by_revenue"].head(10))

    print("\n=== REVENUE BY PAYMENT TYPE ===")
    print(dashboard_data["revenue_by_payment_type"].head(10))

    print("\n=== ON-TIME VS LATE DELIVERY ===")
    print(dashboard_data["on_time_vs_late_delivery"].head(10))

    print("\n=== REVIEW SCORE DISTRIBUTION ===")
    print(dashboard_data["review_score_distribution"].head(10))

    print("\n=== DELAY VS REVIEW SCORE ===")
    print(dashboard_data["delay_vs_review_score"].head(10))

    export_dashboard_data(dashboard_data)
    print("\nAggregated CSV files exported to output/aggregates/")