from __future__ import annotations


def currency(value: float | int | None) -> str:
    if value is None:
        return "N/A"
    return f"${value:,.2f}"


def integer(value: float | int | None) -> str:
    if value is None:
        return "N/A"
    return f"{int(value):,}"


def number(value: float | int | None, decimals: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{value:,.{decimals}f}"


def percent(value: float | int | None, decimals: int = 2) -> str:
    if value is None:
        return "N/A"

    numeric = float(value)
    if abs(numeric) <= 1:
        numeric *= 100

    return f"{numeric:.{decimals}f}%"
