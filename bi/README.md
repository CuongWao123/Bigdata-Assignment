# BI Streamlit App

## Run

From the `bi` directory:

```bash
cd bi
uv run python -m streamlit run app.py
```

## Data Source

This app reads precomputed CSV files from:

- output/aggregates/

Use the **Refresh Aggregates** button in the sidebar to re-run:

- analysis/mapreduce_analysis.py

## Dashboards

1. Executive Overview
2. Sales by Geography
3. Product Performance
4. Payment Behavior
5. Fulfillment / Delivery
6. Customer Satisfaction
