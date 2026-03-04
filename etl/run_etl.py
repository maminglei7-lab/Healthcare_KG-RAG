"""
ETL Main Entry Point
Pipeline: Extract → Transform → Save → Quality Check

All paths read from config.py — switch MODE there to toggle demo/full.
"""

import os
import json
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import MODE, RAW_DIR, CLEANED_DIR, LINEAGE_DIR, LINEAGE_PATH, print_config
from extract import load_all
from transform import transform_all
from quality_check import run_quality_checks


def reset_lineage():
    """Clear lineage.json before each run to prevent duplicate accumulation."""
    os.makedirs(LINEAGE_DIR, exist_ok=True)
    with open(LINEAGE_PATH, 'w', encoding='utf-8') as f:
        json.dump({"lineage_records": []}, f)
    print(f"[Lineage] Reset: {LINEAGE_PATH}")


def save_cleaned(tables):
    """Save all cleaned DataFrames to CSV."""
    os.makedirs(CLEANED_DIR, exist_ok=True)
    for name, df in tables.items():
        out_path = os.path.join(CLEANED_DIR, f"{name}.csv")
        df.to_csv(out_path, index=False, encoding="utf-8")
        print(f"[Save] {name}.csv → {len(df)} rows")


if __name__ == "__main__":
    # ── Config ──
    print("=" * 50)
    print("  ETL Pipeline")
    print("=" * 50)
    print_config()

    # ── Step 0: Reset lineage ──
    print("\n" + "=" * 50)
    print("  Step 0: Reset Lineage")
    print("=" * 50)
    reset_lineage()

    # ── Step 1: Extract ──
    print("\n" + "=" * 50)
    print("  Step 1: Extract")
    print("=" * 50)
    raw = load_all(RAW_DIR)

    # ── Step 2: Transform ──
    print("\n" + "=" * 50)
    print("  Step 2: Transform")
    print("=" * 50)
    cleaned = transform_all(raw)

    # ── Step 3: Save ──
    print("\n" + "=" * 50)
    print("  Step 3: Save to cleaned/")
    print("=" * 50)
    save_cleaned(cleaned)

    # ── Step 4: Quality Check ──
    print("\n" + "=" * 50)
    print("  Step 4: Quality Check")
    print("=" * 50)
    all_passed = run_quality_checks(CLEANED_DIR)

    # ── Summary ──
    print("\n" + "=" * 50)
    print("  ETL Complete")
    print("=" * 50)
    print(f"  MODE:     {MODE.upper()}")
    print(f"  Cleaned:  {CLEANED_DIR}")
    print(f"  Lineage:  {LINEAGE_PATH}")
    print(f"  Quality:  {'ALL PASSED' if all_passed else 'HAS FAILURES'}")
    print("=" * 50)