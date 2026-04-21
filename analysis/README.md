# Kafka Consumer + MapReduce Pipeline

This module processes streaming e-commerce data using Kafka and performs MapReduce-style aggregation for dashboard visualization.

---

## 1. Overview

Pipeline:

Raw Data → Kafka Producer → Kafka Topic → Consumer → CSV → MapReduce → Aggregates → Streamlit

---

## 2. Requirements

- Python 3.10+
- Docker (for Kafka)
- Packages:
  - pandas
  - confluent-kafka

Install:

pip install pandas confluent-kafka

---

## 3. Project Structure
```
Bigdata-Assignment/
│
├── kafka/
│   ├── producer.py
│   └── consumer.py
│
├── analysis/
│   └── mapreduce_analysis.py
│
├── output/
│   ├── stream_output.csv
│   └── aggregates/
│
└── docker-compose.yml
```
---

## 4. How to Run

### Step 1 — Start Kafka
```
docker compose up -d
```
---

### Step 2 — Run Producer
```
python kafka/producer.py
```
This sends enriched data to Kafka topic:
`olist_enriched_orders`

---

### Step 3 — Run Consumer

python kafka/consumer.py

This will:
- read from Kafka
- write to: output/stream_output.csv

Stop with:
CTRL + C

---

### Step 4 — Run MapReduce

python analysis/mapreduce_analysis.py

This will:
- process CSV
- compute aggregations
- export results to:

output/aggregates/

---

## 5. Output Files

### Raw stream data
output/stream_output.csv

### Aggregated outputs (used by dashboard)
output/aggregates/

Important files:

- kpi_summary.csv
- revenue_by_state.csv
- revenue_by_category.csv
- orders_by_payment_type.csv
- review_score_distribution.csv
- on_time_vs_late_delivery.csv
- decision_support.csv

---

## 6. Key Metrics Explained

### KPI Summary

- total_revenue: total price + freight
- total_orders: unique order_id count
- avg_order_value: revenue / orders
- avg_review_score: average review score
- late_delivery_rate: % of late deliveries

NOTE:
We use **order-level aggregation** for KPIs to avoid duplication caused by joins.

---

### Revenue by State

Shows which regions generate the most revenue.

Example:
SP is the top-performing state.

---

### Payment Behavior

- credit_card is the dominant payment type
- useful for optimizing checkout flow

---

### Delivery Performance

- on_time vs late deliveries
- delay measured in days:
  - negative = early
  - positive = late

---

### Customer Satisfaction

- review_score distribution (1–5)
- higher scores = better customer experience

---

### Decision Support

Example insights:

- Best state: SP
- Top category: beleza_saude
- Top payment type: credit_card
- Worst late-delivery state: AL

---

## 7. Notes

- Consumer must run before MapReduce
- CSV grows as streaming continues
- Aggregation reads latest CSV snapshot

---

## 8. For Streamlit Integration

Use only:

output/aggregates/

Do NOT recompute metrics in Streamlit.

---

## 9. Common Issues

Consumer not reading:
- Kafka not running
- wrong topic
- offset already consumed → change group.id

CSV empty:
- producer not run

---

## 10. Summary

This module provides:

- Streaming ingestion (Kafka)
- Data persistence (CSV)
- MapReduce-style analytics
- Dashboard-ready outputs
