"""
Extract: Load all source CSV files from raw directory.
Reads from config.py for paths and file list.
"""

import pandas as pd
import os
from config import RAW_DIR, SOURCE_FILES


def load_all(raw_dir=RAW_DIR):
    """Load all source files and return dict: {table_name: DataFrame}"""
    tables = {}
    for fname in SOURCE_FILES:
        path = os.path.join(raw_dir, fname)
        table_name = fname.replace(".csv.gz", "")
        df = pd.read_csv(path, compression="gzip", low_memory=False)
        tables[table_name] = df
        print(f"[Extract] {table_name}: {len(df)} rows, {len(df.columns)} cols")
    return tables


if __name__ == "__main__":
    tables = load_all()
    print(f"\nAll {len(tables)} files loaded from: {RAW_DIR}")