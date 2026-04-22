from pathlib import Path

APP_TITLE = "Olist BI Dashboard"
APP_ICON = "bar_chart"
LAYOUT = "wide"

BASE_DIR = Path(__file__).resolve().parents[2]
AGGREGATES_DIR = BASE_DIR / "output" / "aggregates"
STREAM_OUTPUT_PATH = BASE_DIR / "output" / "stream_output.csv"
ANALYSIS_SCRIPT = BASE_DIR / "analysis" / "mapreduce_analysis.py"

CACHE_TTL_SECONDS = 120
REFRESH_TIMEOUT_SECONDS = 120

EXPECTED_AGGREGATE_FILES = [
    "average_delay_by_state.csv",
    "average_payment_value.csv",
    "avg_order_value_by_state.csv",
    "avg_payment_by_type.csv",
    "decision_support.csv",
    "delay_trend_over_time.csv",
    "delay_vs_review_score.csv",
    "freight_vs_price_by_category.csv",
    "installments_distribution.csv",
    "kpi_summary.csv",
    "late_delivery_by_state.csv",
    "on_time_vs_late_delivery.csv",
    "order_status_breakdown.csv",
    "orders_by_payment_type.csv",
    "orders_by_state.csv",
    "orders_per_day.csv",
    "revenue_by_category.csv",
    "revenue_by_payment_type.csv",
    "revenue_by_state.csv",
    "review_distribution.csv",
    "review_score_by_category.csv",
    "review_score_by_state.csv",
    "review_score_distribution.csv",
    "top_cities_by_revenue.csv",
    "top_products.csv",
    "top_sellers.csv",
]

THEME_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@400;600;700&family=Space+Grotesk:wght@500;700&display=swap');

:root {
  --bi-primary: #0f4c81;
  --bi-primary-soft: #e7f0fa;
  --bi-accent: #2a9d8f;
  --bi-bg: #f4f8fc;
  --bi-surface: #ffffff;
  --bi-text: #10253d;
  --bi-muted: #3f5669;
  --bi-border: #d6e2ef;
}

html, body, [class*="css"] {
  font-family: 'Source Sans 3', sans-serif;
}

h1, h2, h3, h4, h5, h6 {
  font-family: 'Space Grotesk', sans-serif;
  letter-spacing: 0.2px;
  color: var(--bi-text);
}

p, li, span, label, small, strong {
  color: var(--bi-text);
}

.stApp {
  background:
    radial-gradient(circle at 8% 12%, rgba(42,157,143,0.10), transparent 28%),
    radial-gradient(circle at 92% 88%, rgba(15,76,129,0.12), transparent 32%),
    linear-gradient(160deg, #eef4fb 0%, #f7fbff 55%, #eef5fb 100%);
}

.block-container {
  padding-top: 1.2rem;
}

.bi-panel {
  background: var(--bi-surface);
  border: 1px solid var(--bi-border);
  border-radius: 14px;
  padding: 0.9rem 1rem;
  box-shadow: 0 3px 10px rgba(13, 52, 85, 0.04);
}

.bi-caption {
  color: var(--bi-muted);
  font-size: 0.92rem;
}

.bi-kpi-card {
  background: linear-gradient(140deg, #ffffff 0%, #f6fbff 100%);
  border: 1px solid var(--bi-border);
  border-radius: 14px;
  padding: 0.9rem;
  min-height: 120px;
  box-shadow: 0 4px 14px rgba(13, 52, 85, 0.05);
}

.bi-kpi-label {
  color: var(--bi-muted);
  font-weight: 600;
  font-size: 0.93rem;
  line-height: 1.3;
}

.bi-kpi-value {
  font-family: 'Space Grotesk', sans-serif;
  color: var(--bi-primary);
  font-size: 1.65rem;
  font-weight: 700;
  margin-top: 0.35rem;
  overflow-wrap: anywhere;
}

.bi-section-title {
  font-family: 'Space Grotesk', sans-serif;
  font-size: 1.18rem;
  color: var(--bi-primary);
  margin: 0.35rem 0 0.6rem 0;
}

[data-testid="stSidebar"] {
  background: linear-gradient(180deg, #f5f9fd 0%, #eef5fc 100%);
  border-right: 1px solid var(--bi-border);
}

[data-testid="stDataFrame"] {
  background: #ffffff;
  border: 1px solid var(--bi-border);
  border-radius: 12px;
}

[data-testid="stDataFrame"] th {
  background: var(--bi-primary-soft);
  color: var(--bi-primary);
  font-weight: 600;
}

/* Hide Streamlit default chrome */
header[data-testid="stHeader"] {
  display: none !important;
}

footer {
  display: none !important;
}

[data-testid="stToolbar"] {
  display: none !important;
}

#MainMenu {
  visibility: hidden;
}

/* Hide the default deploy button */
.stDeployButton {
  display: none !important;
}

@media (max-width: 768px) {
  .bi-kpi-value {
    font-size: 1.3rem;
  }
}

.bi-insight-card {
  background: linear-gradient(140deg, #ffffff 0%, #f8fbff 100%);
  border-left: 4px solid var(--bi-primary);
  border-radius: 12px;
  padding: 1rem 1.2rem;
  margin: 0.8rem 0;
  box-shadow: 0 2px 8px rgba(13, 52, 85, 0.04);
}

.bi-insight-card.forecast {
  border-left-color: var(--bi-accent);
}

.bi-insight-card.decision {
  border-left-color: #e76f51;
}

.bi-insight-title {
  font-family: 'Space Grotesk', sans-serif;
  font-size: 0.95rem;
  font-weight: 700;
  margin-bottom: 0.4rem;
  display: flex;
  align-items: center;
  gap: 0.4rem;
}

.bi-insight-title.analysis {
  color: var(--bi-primary);
}

.bi-insight-title.forecast {
  color: var(--bi-accent);
}

.bi-insight-title.decision {
  color: #e76f51;
}

.bi-insight-body {
  font-size: 0.92rem;
  color: var(--bi-text);
  line-height: 1.5;
  margin-bottom: 0.3rem;
}

.bi-insight-vi {
  font-size: 0.85rem;
  color: var(--bi-muted);
  font-style: italic;
  border-top: 1px dashed var(--bi-border);
  padding-top: 0.3rem;
  margin-top: 0.3rem;
}
</style>
"""
