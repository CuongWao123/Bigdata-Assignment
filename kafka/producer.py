from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pathlib import Path
import socket
import pandas as pd
import os
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=BASE_DIR / '.env')

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'client.id': socket.gethostname()
}

producer = Producer(conf)
admin_client = AdminClient({'bootstrap.servers': conf['bootstrap.servers']})


raw_csv_file = os.getenv('SOURCE_1', 'data/olist_customers_dataset.csv').strip().strip('"').strip("'")
topic = "customers"


def ensure_topic(topic_name, partitions=1, replication_factor=1):
    metadata = admin_client.list_topics(timeout=10)
    if topic_name in metadata.topics:
        print(f"Topic '{topic_name}' already exists")
        return

    new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replication_factor)
    futures = admin_client.create_topics([new_topic])

    try:
        futures[topic_name].result()
        print(f"Created topic '{topic_name}'")
    except Exception as e:
        if "TOPIC_ALREADY_EXISTS" in str(e):
            print(f"Topic '{topic_name}' already exists")
        else:
            raise

csv_path = Path(raw_csv_file)
if not csv_path.is_absolute():
    csv_path = BASE_DIR / csv_path

if not csv_path.exists():
    raise FileNotFoundError(f"CSV not found: {csv_path}")

df = pd.read_csv(csv_path)

ensure_topic(topic)

for index, row in df.iterrows():
    message = row.to_json() 
    
    customer_id = str(row['customer_id']) 

    print(f"Producing message to {topic}: key={customer_id}, value={message}")

    # Send the message
    producer.produce(
        topic=topic,
        key=customer_id,
        value=message
    )

    producer.poll(0)

producer.flush()