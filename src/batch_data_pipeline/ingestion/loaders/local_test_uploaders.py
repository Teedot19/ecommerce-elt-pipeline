import csv
from pathlib import Path
from typing import List, Dict, Any

def _write_local_csv(rows: List[Dict[str, Any]], output_path: Path):
    if not rows:
        output_path.write_text("")
        return

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def upload_validated_to_local(entity: str, run_date: str, rows: List[Dict[str, Any]],bucket_name) -> str:
    """Simulate GCS validated upload by writing to /tmp."""
    path = Path(f"/tmp/{entity}_{run_date}_validated_test.csv")
    _write_local_csv(rows, path)
    return str(path)


def upload_quarantine_to_local(entity: str, run_date: str, rows: List[Dict[str, Any]],bucket_name) -> str:
    """Simulate GCS quarantine upload by writing to /tmp."""
    path = Path(f"/tmp/{entity}_{run_date}_quarantine_test.csv")
    _write_local_csv(rows, path)
    return str(path)
