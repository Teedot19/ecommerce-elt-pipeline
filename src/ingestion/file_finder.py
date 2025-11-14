from pathlib import Path
from typing import List

def list_csvs(folder: Path) -> List[Path]:
    return sorted(folder.glob("*.csv"))
