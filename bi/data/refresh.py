from __future__ import annotations

import subprocess
from pathlib import Path

from config.settings import ANALYSIS_SCRIPT, BASE_DIR, REFRESH_TIMEOUT_SECONDS


def refresh_aggregates(timeout_seconds: int = REFRESH_TIMEOUT_SECONDS) -> tuple[bool, str]:
    script_path = Path(ANALYSIS_SCRIPT)
    if not script_path.exists():
        return False, f"Analysis script not found: {script_path}"

    try:
        result = subprocess.run(
            ["python", str(script_path)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, f"Refresh timed out after {timeout_seconds} seconds"
    except Exception as exc:  # pragma: no cover
        return False, f"Refresh failed: {exc}"

    if result.returncode != 0:
        stderr = result.stderr.strip() or "No error output"
        return False, stderr

    return True, result.stdout.strip() or "Refresh completed"
