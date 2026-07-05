"""Test import path configuration.

The project is intentionally script-friendly rather than packaged as an
installable wheel, so tests add the small module roots they exercise.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

for path in (
    ROOT / "data" / "generators",
    ROOT / "ingestion" / "stream",
    ROOT / "ingestion" / "batch",
    ROOT / "serving" / "dashboard",
):
    sys.path.insert(0, str(path))
