from __future__ import annotations

import numpy as np
import pandas as pd


def linear_trend_forecast(
    series: pd.Series, periods: int = 30
) -> tuple[float, list[float]]:
    """
    Fit a linear trend and forecast future values.
    Returns (slope, forecast_values).
    """
    if series.empty or len(series) < 2:
        return 0.0, []

    x = np.arange(len(series))
    y = series.values

    # Remove NaNs
    mask = ~np.isnan(y)
    if mask.sum() < 2:
        return 0.0, []

    slope, intercept = np.polyfit(x[mask], y[mask], 1)
    future_x = np.arange(len(series), len(series) + periods)
    forecast = slope * future_x + intercept
    return float(slope), forecast.tolist()


def moving_average(series: pd.Series, window: int = 7) -> pd.Series:
    """Simple moving average smoothing."""
    return series.rolling(window=window, min_periods=1).mean()


def growth_rate(series: pd.Series) -> float:
    """
    Compute compound monthly growth rate (CMGR) or simple growth.
    Returns growth rate as a decimal.
    """
    if series.empty or series.isna().all():
        return 0.0

    clean = series.dropna()
    if len(clean) < 2:
        return 0.0

    first = float(clean.iloc[0])
    last = float(clean.iloc[-1])

    if first == 0 or last == 0:
        return 0.0

    # Simple growth rate
    return (last - first) / abs(first)


def detect_trend(series: pd.Series) -> str:
    """
    Detect if a time series is trending up, down, or stable.
    """
    if series.empty or len(series) < 3:
        return "stable"

    slope, _ = linear_trend_forecast(series, periods=1)
    std = float(series.std()) if series.std() is not None else 0.0

    if std == 0:
        return "stable"

    # Normalized slope
    normalized = slope / std if std > 0 else 0

    if normalized > 0.05:
        return "increasing"
    elif normalized < -0.05:
        return "decreasing"
    return "stable"


def concentration_index(series: pd.Series) -> float:
    """
    Compute a simple concentration metric: top-N share of total.
    Returns a value 0-1 where 1 = all in one item, 0 = perfectly distributed.
    """
    if series.empty or series.sum() == 0:
        return 0.0

    total = float(series.sum())
    top_share = float(series.max()) / total
    return top_share
