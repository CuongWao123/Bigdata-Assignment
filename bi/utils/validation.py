from __future__ import annotations

from pathlib import Path

from config.settings import EXPECTED_AGGREGATE_FILES


def validate_aggregate_files(aggregates_dir: Path) -> dict:
    missing_files = []
    empty_files = []

    for filename in EXPECTED_AGGREGATE_FILES:
        path = aggregates_dir / filename
        if not path.exists():
            missing_files.append(filename)
            continue
        if path.stat().st_size <= 2:
            empty_files.append(filename)

    status = "ok"
    if missing_files:
        status = "error"
    elif empty_files:
        status = "warning"

    return {
        "status": status,
        "missing_files": missing_files,
        "empty_files": empty_files,
    }
