import csv
import os
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


# =========================
# Conda / Windows PySpark config
# =========================
# Spark starts separate Python worker processes. On Windows, plain "python" may
# point to the Microsoft Store alias. sys.executable points to the currently
# running Conda environment's python.exe.
PYTHON_EXECUTABLE = sys.executable
os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE


# =========================
# Path config
# =========================
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_CSV = BASE_DIR / "output" / "stream_output.csv"
EXPORT_DIR = BASE_DIR / "output" / "aggregates"


# =========================
# Spark session
# =========================
def get_spark(app_name: str = "Olist PySpark MapReduce Aggregation") -> SparkSession:
    """
    Create a local SparkSession that works reliably with Conda on Windows.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.pyspark.python", PYTHON_EXECUTABLE)
        .config("spark.pyspark.driver.python", PYTHON_EXECUTABLE)
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# =========================
# Load + preprocess
# =========================
def load_stream_data(
    spark: SparkSession,
    csv_path: Path = OUTPUT_CSV,
) -> DataFrame:
    """
    Read CSV produced by the Kafka consumer.

    Notes:
    - inferSchema=false keeps all raw columns as strings.
    - Casting is done explicitly in preprocess_data().
    - multiLine=true helps with review_comment_message values containing line breaks.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"File not found: {csv_path}")

    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(str(csv_path))
    )


def preprocess_data(df: DataFrame) -> DataFrame:
    """
    Clean and cast data safely.
    """
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

    numeric_double_cols = [
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
        "payment_sequential",
        "payment_installments",
        "payment_value",
        "review_score",
    ]

    numeric_int_cols = [
        "customer_zip_code_prefix",
    ]

    for col_name in datetime_cols:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.coalesce(
                    F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss"),
                    F.to_timestamp(F.col(col_name)),
                ),
            )

    for col_name in numeric_double_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    for col_name in numeric_int_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))

    string_fill_map = {
        "product_category_name": "unknown",
        "customer_city": "unknown",
        "customer_state": "unknown",
        "payment_type": "unknown",
        "review_comment_title": "",
        "review_comment_message": "",
        "order_status": "unknown",
        "seller_id": "unknown",
        "product_id": "unknown",
    }

    fill_existing = {
        col_name: fill_value
        for col_name, fill_value in string_fill_map.items()
        if col_name in df.columns
    }

    if fill_existing:
        df = df.fillna(fill_existing)

    return df


def add_derived_fields(df: DataFrame) -> DataFrame:
    """
    Add derived BI fields.
    """
    if "price" in df.columns and "freight_value" in df.columns:
        df = df.withColumn("price", F.coalesce(F.col("price"), F.lit(0.0)))
        df = df.withColumn("freight_value", F.coalesce(F.col("freight_value"), F.lit(0.0)))
        df = df.withColumn("revenue", F.col("price") + F.col("freight_value"))
    else:
        df = df.withColumn("revenue", F.lit(0.0))

    if "review_score" in df.columns:
        df = df.withColumn(
            "review_sentiment",
            F.when(F.col("review_score").isNull(), F.lit("unknown"))
            .when(F.col("review_score") >= 4, F.lit("positive"))
            .when(F.col("review_score") == 3, F.lit("neutral"))
            .when(F.col("review_score") >= 1, F.lit("negative"))
            .otherwise(F.lit("unknown")),
        )
    else:
        df = df.withColumn("review_sentiment", F.lit("unknown"))

    if (
        "order_delivered_customer_date" in df.columns
        and "order_estimated_delivery_date" in df.columns
    ):
        df = df.withColumn(
            "delivery_delay_days",
            F.coalesce(
                F.datediff(
                    F.col("order_delivered_customer_date"),
                    F.col("order_estimated_delivery_date"),
                ),
                F.lit(0),
            ),
        )
    else:
        df = df.withColumn("delivery_delay_days", F.lit(0))

    df = df.withColumn(
        "is_late_delivery",
        F.when(F.col("delivery_delay_days") > 0, F.lit(1)).otherwise(F.lit(0)),
    )

    return df


# =========================
# Export helpers
# =========================
def _remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def write_spark_df_as_single_csv(df: DataFrame, output_file: Path) -> None:
    """
    Export small aggregated Spark DataFrame to a normal CSV file.

    Avoids Spark .write.csv() because Windows local Spark often requires
    HADOOP_HOME / winutils.exe for Hadoop file output.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if output_file.suffix.lower() != ".csv":
        output_file = output_file.with_suffix(".csv")

    _remove_path(output_file)

    rows = df.collect()
    columns = df.columns

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row[col] for col in columns])
            
def write_dict_as_csv(data: dict[str, Any], output_file: Path) -> None:
    """
    Write small Python dict metrics directly with Python's csv module.

    This intentionally does NOT use spark.createDataFrame() or toPandas().
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if output_file.suffix.lower() != ".csv":
        output_file = output_file.with_suffix(".csv")

    _remove_path(output_file)

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(data.keys()))
        writer.writeheader()
        writer.writerow(data)


# =========================
# Aggregations
# =========================
def get_kpi_summary(df: DataFrame) -> dict[str, Any]:
    row = df.agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        F.round(F.avg("is_late_delivery"), 4).alias("late_delivery_rate"),
    ).first()

    total_revenue = float(row["total_revenue"] or 0.0)
    total_orders = int(row["total_orders"] or 0)
    avg_order_value = round(total_revenue / total_orders, 2) if total_orders > 0 else 0.0

    return {
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "avg_order_value": avg_order_value,
        "avg_review_score": float(row["avg_review_score"] or 0.0),
        "late_delivery_rate": float(row["late_delivery_rate"] or 0.0),
    }


def get_revenue_by_state(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("customer_state")
        .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
    )


def get_revenue_by_category(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("product_category_name")
        .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
    )


def get_orders_by_payment_type(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("payment_type")
        .agg(F.countDistinct("order_id").alias("order_count"))
        .orderBy(F.desc("order_count"))
    )


def get_review_distribution(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("review_sentiment")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
    )


def get_late_delivery_by_state(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("customer_state")
        .agg(
            F.count("*").alias("total_rows"),
            F.sum("is_late_delivery").alias("late_rows"),
            F.countDistinct("order_id").alias("unique_orders"),
        )
        .withColumn("late_delivery_rate", F.round(F.col("late_rows") / F.col("total_rows"), 4))
        .orderBy(F.desc("late_delivery_rate"))
    )


def get_top_sellers(df: DataFrame, top_n: int = 10) -> DataFrame:
    return (
        df.groupBy("seller_id")
        .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
        .limit(top_n)
    )


def get_average_payment_value(df: DataFrame) -> float:
    row = df.agg(F.round(F.avg("payment_value"), 2).alias("average_payment_value")).first()
    return float(row["average_payment_value"] or 0.0)


def get_orders_per_day(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
        .where(F.col("purchase_date").isNotNull())
        .groupBy("purchase_date")
        .agg(F.countDistinct("order_id").alias("order_count"))
        .orderBy("purchase_date")
    )


def get_order_status_breakdown(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("order_status")
        .agg(F.countDistinct("order_id").alias("order_count"))
        .orderBy(F.desc("order_count"))
    )


def get_orders_by_state(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("customer_state")
        .agg(F.countDistinct("order_id").alias("order_count"))
        .orderBy(F.desc("order_count"))
    )


def get_avg_order_value_by_state(df: DataFrame) -> DataFrame:
    order_revenue_by_state = (
        df.groupBy("customer_state", "order_id")
        .agg(F.sum("revenue").alias("order_revenue"))
    )

    return (
        order_revenue_by_state.groupBy("customer_state")
        .agg(F.round(F.avg("order_revenue"), 2).alias("avg_order_value"))
        .orderBy(F.desc("avg_order_value"))
    )


def get_top_cities_by_revenue(df: DataFrame, top_n: int = 10) -> DataFrame:
    return (
        df.groupBy("customer_city")
        .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
        .limit(top_n)
    )


def get_top_products(df: DataFrame, top_n: int = 10) -> DataFrame:
    return (
        df.groupBy("product_id")
        .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
        .limit(top_n)
    )


def get_freight_vs_price_by_category(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("product_category_name")
        .agg(
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.avg("freight_value"), 2).alias("avg_freight"),
        )
        .orderBy(F.desc("avg_price"))
    )


def get_revenue_by_payment_type(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("payment_type")
        .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
    )


def get_installments_distribution(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("payment_installments")
        .agg(F.count("*").alias("count"))
        .orderBy("payment_installments")
    )


def get_avg_payment_by_type(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("payment_type")
        .agg(F.round(F.avg("payment_value"), 2).alias("avg_payment_value"))
        .orderBy(F.desc("avg_payment_value"))
    )


def get_on_time_vs_late_delivery(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "delivery_status",
            F.when(F.col("is_late_delivery") == 1, F.lit("late")).otherwise(F.lit("on_time")),
        )
        .groupBy("delivery_status")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
    )


def get_average_delay_by_state(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("customer_state")
        .agg(F.round(F.avg("delivery_delay_days"), 2).alias("avg_delay_days"))
        .orderBy(F.desc("avg_delay_days"))
    )


def get_delay_trend_over_time(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
        .where(F.col("purchase_date").isNotNull())
        .groupBy("purchase_date")
        .agg(F.round(F.avg("delivery_delay_days"), 2).alias("avg_delay_days"))
        .orderBy("purchase_date")
    )


def get_review_score_distribution(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("review_score")
        .agg(F.count("*").alias("count"))
        .orderBy("review_score")
    )


def get_review_score_by_state(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("customer_state")
        .agg(F.round(F.avg("review_score"), 2).alias("avg_review_score"))
        .orderBy(F.desc("avg_review_score"))
    )


def get_review_score_by_category(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("product_category_name")
        .agg(F.round(F.avg("review_score"), 2).alias("avg_review_score"))
        .orderBy(F.desc("avg_review_score"))
    )


def get_delay_vs_review_score(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("review_score")
        .agg(F.round(F.avg("delivery_delay_days"), 2).alias("avg_delay_days"))
        .orderBy("review_score")
    )


# =========================
# Order-level KPI
# =========================
def get_order_level_df(df: DataFrame) -> DataFrame:
    """
    One row per order_id:
    - Sum revenue per order.
    - Keep latest representative row per order.
    """
    order_revenue = (
        df.groupBy("order_id")
        .agg(F.sum("revenue").alias("order_revenue"))
    )

    window_spec = Window.partitionBy("order_id").orderBy(
        F.col("order_purchase_timestamp").desc_nulls_last(),
        F.col("review_answer_timestamp").desc_nulls_last(),
    )

    representative_rows = (
        df.withColumn("row_num", F.row_number().over(window_spec))
        .where(F.col("row_num") == 1)
        .drop("row_num")
    )

    return representative_rows.join(order_revenue, on="order_id", how="left")


def get_kpi_summary_order_level(df: DataFrame) -> dict[str, Any]:
    order_df = get_order_level_df(df)

    row = order_df.agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.sum("order_revenue"), 2).alias("total_revenue"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        F.round(F.avg("is_late_delivery"), 4).alias("late_delivery_rate"),
    ).first()

    total_revenue = float(row["total_revenue"] or 0.0)
    total_orders = int(row["total_orders"] or 0)
    avg_order_value = round(total_revenue / total_orders, 2) if total_orders > 0 else 0.0

    return {
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "avg_order_value": avg_order_value,
        "avg_review_score": float(row["avg_review_score"] or 0.0),
        "late_delivery_rate": float(row["late_delivery_rate"] or 0.0),
    }


# =========================
# Decision support
# =========================
def first_value(df: DataFrame, col_name: str):
    row = df.first()
    return row[col_name] if row else None


def get_decision_support(df: DataFrame) -> dict[str, Any]:
    revenue_by_state = get_revenue_by_state(df)
    revenue_by_category = get_revenue_by_category(df)
    orders_by_payment_type = get_orders_by_payment_type(df)
    late_delivery_by_state = get_late_delivery_by_state(df)
    review_distribution = get_review_distribution(df)

    total_reviews = review_distribution.agg(F.sum("count").alias("total")).first()["total"] or 0

    positive_count = (
        review_distribution
        .where(F.col("review_sentiment") == "positive")
        .agg(F.sum("count").alias("count"))
        .first()["count"]
        or 0
    )

    negative_count = (
        review_distribution
        .where(F.col("review_sentiment") == "negative")
        .agg(F.sum("count").alias("count"))
        .first()["count"]
        or 0
    )

    return {
        "best_state_by_revenue": first_value(revenue_by_state, "customer_state"),
        "top_category_by_revenue": first_value(revenue_by_category, "product_category_name"),
        "top_payment_type": first_value(orders_by_payment_type, "payment_type"),
        "worst_state_by_late_rate": first_value(late_delivery_by_state, "customer_state"),
        "positive_review_ratio": round(positive_count / total_reviews, 4) if total_reviews > 0 else 0.0,
        "negative_review_ratio": round(negative_count / total_reviews, 4) if total_reviews > 0 else 0.0,
    }


# =========================
# Bundle outputs
# =========================
def get_all_dashboard_data(df: DataFrame) -> dict[str, Any]:
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


def prepare_dashboard_data(
    spark: SparkSession,
    csv_path: Path = OUTPUT_CSV,
) -> dict[str, Any]:
    df = load_stream_data(spark, csv_path)
    df = preprocess_data(df)
    df = add_derived_fields(df)

    # Reused by many aggregations.
    df = df.cache()
    df.count()

    dashboard_data = get_all_dashboard_data(df)

    return {
        "raw_df": df,
        "dashboard_data": dashboard_data,
    }


def export_dashboard_data(
    dashboard_data: dict[str, Any],
    export_dir: Path = EXPORT_DIR,
) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)

    write_dict_as_csv(dashboard_data["kpi_summary"], export_dir / "kpi_summary.csv")
    write_dict_as_csv(dashboard_data["kpi_summary_order_level"], export_dir / "kpi_summary_order_level.csv")
    write_dict_as_csv(
        {"average_payment_value": dashboard_data["average_payment_value"]},
        export_dir / "average_payment_value.csv",
    )
    write_dict_as_csv(dashboard_data["decision_support"], export_dir / "decision_support.csv")

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
        if isinstance(value, DataFrame):
            write_spark_df_as_single_csv(value, export_dir / f"{key}.csv")


# =========================
# Main
# =========================
if __name__ == "__main__":
    print(f"Using Python executable: {PYTHON_EXECUTABLE}")

    spark = get_spark()

    try:
        result = prepare_dashboard_data(spark)
        df = result["raw_df"]
        dashboard_data = result["dashboard_data"]

        print("Loaded, preprocessed, and aggregated successfully.")
        print(f"Shape: ({df.count()}, {len(df.columns)})")

        print("\n=== KPI SUMMARY (ORDER LEVEL) ===")
        print(dashboard_data["kpi_summary_order_level"])

        print("\n=== AVERAGE PAYMENT VALUE ===")
        print(dashboard_data["average_payment_value"])

        print("\n=== ORDERS PER DAY ===")
        dashboard_data["orders_per_day"].show(5, truncate=False)

        print("\n=== ORDER STATUS BREAKDOWN ===")
        dashboard_data["order_status_breakdown"].show(10, truncate=False)

        print("\n=== ORDERS BY STATE ===")
        dashboard_data["orders_by_state"].show(10, truncate=False)

        print("\n=== TOP CITIES BY REVENUE ===")
        dashboard_data["top_cities_by_revenue"].show(10, truncate=False)

        print("\n=== REVENUE BY PAYMENT TYPE ===")
        dashboard_data["revenue_by_payment_type"].show(10, truncate=False)

        print("\n=== ON-TIME VS LATE DELIVERY ===")
        dashboard_data["on_time_vs_late_delivery"].show(10, truncate=False)

        print("\n=== REVIEW SCORE DISTRIBUTION ===")
        dashboard_data["review_score_distribution"].show(10, truncate=False)

        print("\n=== DELAY VS REVIEW SCORE ===")
        dashboard_data["delay_vs_review_score"].show(10, truncate=False)

        export_dashboard_data(dashboard_data)
        print("\nAggregated CSV files exported to output/aggregates/")

    finally:
        spark.stop()
