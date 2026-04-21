import os
import csv
import json
from pathlib import Path
from confluent_kafka import Consumer, KafkaException

# =========================
# Kafka config
# =========================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "olist_enriched_orders")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "olist-consumer-group-v1")


# =========================
# Output path
# =========================
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_CSV = OUTPUT_DIR / "stream_output.csv"


# =========================
# CSV schema
# =========================
CSV_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",

    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",

    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",

    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",

    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",

    "review_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp",
]


def create_consumer() -> Consumer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
    }
    return Consumer(config)


def ensure_output_csv() -> None:
    if not OUTPUT_CSV.exists():
        with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()

def normalize_value(value):
    if value is None:
        return ""

    try:
        if isinstance(value, float) and value != value:
            return ""
    except Exception:
        pass

    return value


def parse_message_to_row(message_value: bytes) -> dict:
    """
    Parses the Kafka message value (bytes) into a dictionary representing a CSV row.
    """
    decoded = message_value.decode("utf-8")
    payload = json.loads(decoded)

    row = {}
    for col in CSV_COLUMNS:
        row[col] = normalize_value(payload.get(col, ""))

    return row

def append_row_to_csv(row: dict) -> None:
    with open(OUTPUT_CSV, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow(row)


def consume_messages() -> None:
    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Subscribed to topic: {KAFKA_TOPIC}")
    print("Start consuming messages... Press Ctrl+C to stop.")

    message_count = 0

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            row = parse_message_to_row(msg.value())
            append_row_to_csv(row)

            message_count += 1

            if message_count % 1000 == 0:
                print(f"Processed {message_count} messages")

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
        print(f"Processed a total of {message_count} messages")

    finally:
        consumer.close()
        print("Kafka consumer closed.")

if __name__ == "__main__":
    ensure_output_csv()
    print(f"Output CSV ready at: {OUTPUT_CSV}")
    print(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")

    consume_messages()