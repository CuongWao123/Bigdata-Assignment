

from pathlib import Path
import json
import os
import socket
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"


def data_file(filename: str) -> Path:
    file_path = DATA_DIR / filename
    if not file_path.exists():
        raise FileNotFoundError(f"No such file or directory: '{file_path}'")
    return file_path


def extract_data():
    print("Extracting data...")
    orders_path = data_file("olist_orders_dataset.csv")
    orders_item_path = data_file("olist_order_items_dataset.csv")
    products_path = data_file("olist_products_dataset.csv")
    customers_path = data_file("olist_customers_dataset.csv")
    order_payments_path = data_file("olist_order_payments_dataset.csv")
    order_reviews_path = data_file("olist_order_reviews_dataset.csv")
    
    # read and join datasets
    orders = pd.read_csv(orders_path)
    order_items = pd.read_csv(orders_item_path)
    products = pd.read_csv(products_path)
    customers = pd.read_csv(customers_path)
    order_payments = pd.read_csv(order_payments_path)
    order_reviews = pd.read_csv(order_reviews_path)
    
    # Join datasets
    
    df = orders.merge(order_items, on='order_id', how='left') \
        .merge(products, on='product_id', how='left') \
        .merge(customers, on='customer_id', how='left') \
        .merge(order_payments, on='order_id', how='left') \
        .merge(order_reviews, on='order_id', how='left')
        
    return df    


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")


def ensure_topic(admin_client: AdminClient, topic_name: str, partitions: int = 1, replication_factor: int = 1):
    metadata = admin_client.list_topics(timeout=10)
    if topic_name in metadata.topics:
        print(f"Topic '{topic_name}' already exists.")
        return

    new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replication_factor)
    futures = admin_client.create_topics([new_topic])

    try:
        futures[topic_name].result()
        print(f"Created topic '{topic_name}'.")
    except Exception as exc:
        # Kafka may return an "already exists" error in races between clients.
        if "TOPIC_ALREADY_EXISTS" in str(exc):
            print(f"Topic '{topic_name}' already exists.")
            return
        raise


def publish_to_kafka(df: pd.DataFrame):
    conf = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "client.id": socket.gethostname(),
    }
    topic = os.getenv("KAFKA_TOPIC", "olist_enriched_orders")

    admin_client = AdminClient({"bootstrap.servers": conf["bootstrap.servers"]})
    ensure_topic(admin_client, topic)

    producer = Producer(conf)

    for _, row in df.iterrows():
        key = str(row.get("order_id", ""))
        message = json.dumps(row.to_dict(), default=str)
        producer.produce(topic=topic, key=key, value=message, callback=delivery_report)
        producer.poll(0)

    producer.flush()
    print(f"Published {len(df)} records to topic '{topic}'.")


def main():
    data = extract_data()
    publish_to_kafka(data)


if __name__ == "__main__":
    main()
    

