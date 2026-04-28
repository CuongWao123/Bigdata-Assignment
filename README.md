# Bigdata Kafka Producer
**STUDENTS:**
| Student Name          | Student ID |
| --------------------- | ------- |
| Tạ Trung Tín          | 2213506 |
| Trần Đại Việt         | 2213951 |
| Nguyễn Văn Mạnh Cường | 2210437 |
| Nguyễn Văn Nhật Huy   | 2211254 |
| Trần Hồng Phúc        | 2212651 |

This project joins multiple Olist CSV datasets and publishes enriched order records to an Apache Kafka topic.

## Project Structure

```text
Bigdata/
|-- data/
|   |-- olist_customers_dataset.csv
|   |-- olist_orders_dataset.csv
|   |-- olist_order_items_dataset.csv
|   |-- olist_order_payments_dataset.csv
|   |-- olist_order_reviews_dataset.csv
|   |-- olist_products_dataset.csv
|   `-- ...
|-- kafka/
|   `-- producer.py
|-- docker-compose.yml
`-- Read.md
```

## What the Producer Does

1. Reads CSV files from `data/`.
2. Joins orders, items, products, customers, payments, and reviews.
3. Ensures Kafka topic exists (creates it if missing).
4. Publishes one JSON message per joined row.

Default topic: `olist_enriched_orders`

## Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- Python 3.10+
- Python packages:

```bash
pip install pandas confluent-kafka
```

## Start Kafka

From project root:

```bash
docker compose up -d
```

Check containers:

```bash
docker ps
```

## Run Producer

From `Bigdata/kafka`:

```bash
python producer.py
```

Or from project root:

```bash
python kafka/producer.py
```

## Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS` (default: `localhost:9092`)
- `KAFKA_TOPIC` (default: `olist_enriched_orders`)

PowerShell example:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
$env:KAFKA_TOPIC="olist_enriched_orders"
python .\kafka\producer.py
```

## Verify Topic and Messages

Open shell in broker container:

```bash
docker exec -it -w /opt/kafka/bin broker sh
```

List topics:

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Consume messages:

```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic olist_enriched_orders --from-beginning
```

## Troubleshooting

- If CSV file is not found: ensure files exist in `Bigdata/data/`.
- If broker connection fails: make sure Docker container `broker` is running.
- If no messages are consumed: verify topic name used by producer and consumer is the same.

## BI Dashboard (Streamlit)

After you generate aggregate files in `output/aggregates/`, run:

```bash
cd bi
uv run python -m streamlit run app.py
```

The BI app includes 6 dashboards:

1. Executive Overview
2. Sales by Geography
3. Product Performance
4. Payment Behavior
5. Fulfillment / Delivery
6. Customer Satisfaction

The sidebar includes a `Refresh Aggregates` button that re-runs `analysis/mapreduce_analysis.py`.
