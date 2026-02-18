"""CSV-based registry node loading for QNS."""

from __future__ import annotations

import csv
import importlib.resources
import io
from dataclasses import dataclass
from pathlib import Path

from ..exc import QNSConfigError

REQUIRED_COLUMNS = {"dataset", "cluster", "dbtype", "node", "host", "port", "port_env", "env"}


@dataclass(frozen=True)
class RegistryNode:
    """A single QNS registry node parsed from a CSV row."""

    dataset: str
    cluster: str
    dbtype: str
    node: str
    host: str
    port: int
    port_env: str
    env: str


def load_registry_nodes(
    market: str,
    env: str,
    data_dir: str | Path | None = None,
) -> list[RegistryNode]:
    """Load registry nodes from ``{market}_{env}.csv``.

    Parameters
    ----------
    market:
        Market identifier (e.g. ``"fx"``).
    env:
        Environment identifier (e.g. ``"prod"``).
    data_dir:
        Directory containing CSV files.  Defaults to the bundled
        ``qorm.qns.data`` package directory.

    Returns
    -------
    list[RegistryNode]

    Raises
    ------
    QNSConfigError
        If the file is missing, empty, or contains malformed rows.
    """
    filename = f"{market.lower()}_{env.lower()}.csv"

    if data_dir is not None:
        csv_path = Path(data_dir) / filename
        if not csv_path.exists():
            raise QNSConfigError(f"Registry CSV not found: {csv_path}")
        text = csv_path.read_text(encoding="utf-8")
    else:
        try:
            ref = importlib.resources.files("qorm.qns") / "data" / filename
            text = ref.read_text(encoding="utf-8")
        except (FileNotFoundError, TypeError, ModuleNotFoundError) as exc:
            raise QNSConfigError(
                f"Registry CSV not found in package data: {filename}"
            ) from exc

    return _parse_csv(text, filename)


def _parse_csv(text: str, filename: str) -> list[RegistryNode]:
    reader = csv.DictReader(io.StringIO(text))

    if reader.fieldnames is None:
        raise QNSConfigError(f"Registry CSV is empty: {filename}")

    actual = {f.strip().lower() for f in reader.fieldnames}
    missing = REQUIRED_COLUMNS - actual
    if missing:
        raise QNSConfigError(
            f"Registry CSV {filename} missing columns: {', '.join(sorted(missing))}"
        )

    # Build a mapping from normalized name -> original fieldname
    norm_to_orig = {f.strip().lower(): f for f in reader.fieldnames}

    nodes: list[RegistryNode] = []
    for i, row in enumerate(reader, start=2):  # line 1 is headers
        try:
            port_raw = row[norm_to_orig["port"]].strip()
            nodes.append(
                RegistryNode(
                    dataset=row[norm_to_orig["dataset"]].strip(),
                    cluster=row[norm_to_orig["cluster"]].strip(),
                    dbtype=row[norm_to_orig["dbtype"]].strip(),
                    node=row[norm_to_orig["node"]].strip(),
                    host=row[norm_to_orig["host"]].strip(),
                    port=int(port_raw),
                    port_env=row[norm_to_orig["port_env"]].strip(),
                    env=row[norm_to_orig["env"]].strip(),
                )
            )
        except (KeyError, ValueError, AttributeError) as exc:
            raise QNSConfigError(
                f"Malformed row {i} in {filename}: {exc}"
            ) from exc

    if not nodes:
        raise QNSConfigError(f"Registry CSV has no data rows: {filename}")

    return nodes
