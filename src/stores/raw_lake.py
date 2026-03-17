"""
Immutable raw data lake.

Invariants:
- Files are never overwritten. Duplicate writes get .v2, .v3 suffixes.
- Every file is SHA-256 checksummed on write.
- Path convention: {root}/{source}/{YYYY}/{MM}/{DD}/{filename}
- data/raw/ is gitignored — never commit raw market data.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from pathlib import Path

# Default root is data/raw/ relative to project root
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_DEFAULT_ROOT = _PROJECT_ROOT / "data" / "raw"


def _get_root() -> Path:
    raw = os.environ.get("RAW_LAKE_ROOT")
    return Path(raw) if raw else _DEFAULT_ROOT


def _versioned_path(path: Path) -> Path:
    """If path exists, append .v2, .v3, etc. until we find a free slot."""
    if not path.exists():
        return path
    v = 2
    while True:
        candidate = path.with_suffix(f".v{v}{path.suffix}")
        if not candidate.exists():
            return candidate
        v += 1


def store(source: str, date_: date, filename: str, raw_bytes: bytes) -> Path:
    """
    Write raw bytes to the lake. Returns the path where data was stored.

    Args:
        source:    e.g. "nse/bhavcopy", "rbi/reference_rates"
        date_:     the data date (not the ingestion date)
        filename:  e.g. "BhavCopy_NSE_CM_0_0_0_20220502_F_0000.csv"
        raw_bytes: raw content (ZIP, CSV, HTML, etc.)
    """
    root = _get_root()
    dest_dir = root / source / str(date_.year) / f"{date_.month:02d}" / f"{date_.day:02d}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = _versioned_path(dest_dir / filename)
    dest.write_bytes(raw_bytes)

    # Write checksum sidecar
    sha256 = hashlib.sha256(raw_bytes).hexdigest()
    meta = {
        "source": source,
        "data_date": date_.isoformat(),
        "filename": filename,
        "stored_path": str(dest),
        "size_bytes": len(raw_bytes),
        "sha256": sha256,
    }
    dest.with_suffix(dest.suffix + ".meta.json").write_text(json.dumps(meta, indent=2))

    return dest


def exists(source: str, date_: date, filename: str) -> bool:
    """Check if a raw file for this source/date/filename already exists."""
    root = _get_root()
    dest_dir = root / source / str(date_.year) / f"{date_.month:02d}" / f"{date_.day:02d}"
    return (dest_dir / filename).exists()


def get_path(source: str, date_: date, filename: str) -> Path | None:
    """Return path to an existing raw file, or None if not present."""
    root = _get_root()
    p = root / source / str(date_.year) / f"{date_.month:02d}" / f"{date_.day:02d}" / filename
    return p if p.exists() else None


def verify(path: Path) -> bool:
    """Verify a stored file against its checksum sidecar. Returns True if intact."""
    meta_path = path.with_suffix(path.suffix + ".meta.json")
    if not meta_path.exists():
        return False
    meta = json.loads(meta_path.read_text())
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    return actual == meta["sha256"]
