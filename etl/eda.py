"""
EDA Script for MIMIC-IV Demo (7 tables)
Purpose: Explore all tables before defining cleaning rules
Output: Console report per table — shape, dtypes, nulls, unique counts, value samples
"""

import pandas as pd
import os

# ─────────────────────────────────────────────
# Configuration: Switch between demo and full data
# ─────────────────────────────────────────────
MODE = "demo"   # Switch to "full" for real data

PATHS = {
    "demo": r"D:\Desktop\DAMG 7374\healthcare_lineagetracking\data\raw",
    "full": r"D:\Desktop\project\data\raw_full",
}

RAW_DIR = PATHS[MODE]

FILES = [
    "patients.csv.gz",
    "admissions.csv.gz",
    "diagnoses_icd.csv.gz",
    "labevents.csv.gz",
    "d_icd_diagnoses.csv.gz",
    "d_labitems.csv.gz",
    "prescriptions.csv.gz",
]

def eda_table(name, df):
    print(f"\n{'='*70}")
    print(f"  TABLE: {name}")
    print(f"{'='*70}")

    # ── Basic Shape ──
    print(f"\n[Shape] {df.shape[0]} rows × {df.shape[1]} columns")

    # ── Null Analysis ──
    print(f"\n[Null Analysis]")
    print(f"  {'Column':<30} {'Dtype':<15} {'Nulls':>8} {'Null%':>8} {'Unique':>8}")
    print(f"  {'-'*30} {'-'*15} {'-'*8} {'-'*8} {'-'*8}")
    for col in df.columns:
        null_count = df[col].isna().sum()
        null_pct = f"{null_count / len(df) * 100:.1f}%"
        unique = df[col].nunique()
        dtype = str(df[col].dtype)
        flag = " ⚠️" if null_count > 0 else ""
        print(f"  {col:<30} {dtype:<15} {null_count:>8} {null_pct:>8} {unique:>8}{flag}")

    # ── Key Field Deep Dive (table-specific) ──
    print(f"\n[Key Field Details]")
    _deep_dive(name, df)

    # ── Sample Rows ──
    print(f"\n[Sample Data (first 3 rows)]")
    print(df.head(3).to_string(index=False, max_colwidth=40))


def _deep_dive(name, df):
    """Table-specific field analysis focused on graph schema needs"""

    if name == "patients":
        _show_value_counts(df, "gender")
        _show_numeric_stats(df, "anchor_age")
        _show_value_counts(df, "dod", top_n=5, show_empty=True)

    elif name == "admissions":
        _show_value_counts(df, "admission_type")
        _show_value_counts(df, "insurance")
        _show_value_counts(df, "language", show_empty=True)
        _show_value_counts(df, "marital_status", show_empty=True)
        _show_value_counts(df, "race")
        # Check timestamp nulls
        for col in ["admittime", "dischtime", "deathtime", "edregtime", "edouttime"]:
            null_n = df[col].isna().sum()
            empty_n = (df[col] == "").sum() if df[col].dtype == object else 0
            print(f"  {col}: null={null_n}, empty_string={empty_n}")

    elif name == "diagnoses_icd":
        _show_value_counts(df, "icd_version")
        print(f"  Unique icd_code count: {df['icd_code'].nunique()}")
        print(f"  Unique (subject_id, hadm_id) pairs: {df.groupby(['subject_id','hadm_id']).ngroups}")
        # Check for duplicates on primary key
        dup = df.duplicated(subset=["subject_id", "hadm_id", "seq_num"]).sum()
        print(f"  Duplicates on (subject_id, hadm_id, seq_num): {dup}")

    elif name == "labevents":
        _show_numeric_stats(df, "valuenum")
        _show_value_counts(df, "flag", show_empty=True)
        # ref_range analysis — critical for Layer 2
        print(f"\n  --- Reference Range Analysis (Layer 2 critical) ---")
        ref_lower_null = df["ref_range_lower"].isna().sum()
        ref_upper_null = df["ref_range_upper"].isna().sum()
        both_present = df["ref_range_lower"].notna() & df["ref_range_upper"].notna()
        print(f"  ref_range_lower null: {ref_lower_null} ({ref_lower_null/len(df)*100:.1f}%)")
        print(f"  ref_range_upper null: {ref_upper_null} ({ref_upper_null/len(df)*100:.1f}%)")
        print(f"  Both ranges present:  {both_present.sum()} ({both_present.sum()/len(df)*100:.1f}%)")
        # hadm_id null — affects relationship building
        hadm_null = df["hadm_id"].isna().sum()
        print(f"  hadm_id null: {hadm_null} ({hadm_null/len(df)*100:.1f}%) — these cannot link to Admission")
        # Sample of abnormal flags
        if "flag" in df.columns:
            abnormal = df[df["flag"] == "abnormal"]
            print(f"  Rows flagged 'abnormal': {len(abnormal)} ({len(abnormal)/len(df)*100:.1f}%)")

    elif name == "prescriptions":
        print(f"\n  --- Prescription Key Fields ---")
        _show_value_counts(df, "drug_type")
        print(f"  Unique drug names: {df['drug'].nunique()}")
        print(f"  Top 10 drugs:")
        for drug, cnt in df["drug"].value_counts().head(10).items():
            print(f"    {drug}: {cnt}")
        # Null analysis on key fields for Medication node
        for col in ["drug", "dose_val_rx", "dose_unit_rx", "route", "hadm_id"]:
            null_n = df[col].isna().sum()
            empty_n = (df[col].astype(str).str.strip() == "").sum()
            print(f"  {col}: null={null_n}, empty_string={empty_n}")
        _show_value_counts(df, "route", top_n=10)

    elif name == "d_icd_diagnoses":
        _show_value_counts(df, "icd_version")
        print(f"  Unique icd_code: {df['icd_code'].nunique()}")
        null_title = df["long_title"].isna().sum()
        print(f"  long_title null: {null_title}")
        # ICD hierarchy preview — needed for Layer 2
        print(f"\n  --- ICD Hierarchy Preview (Layer 2) ---")
        sample_10 = df[df["icd_version"] == 10].head(10)
        if len(sample_10) > 0:
            for _, row in sample_10.iterrows():
                code = str(row["icd_code"])
                parent = code[:3] if len(code) > 3 else code
                print(f"    {code} → parent: {parent} | {row['long_title'][:60]}")
        sample_9 = df[df["icd_version"] == 9].head(5)
        if len(sample_9) > 0:
            print(f"  ICD-9 samples:")
            for _, row in sample_9.iterrows():
                code = str(row["icd_code"])
                parent = code[:3] if len(code) > 3 else code
                print(f"    {code} → parent: {parent} | {row['long_title'][:60]}")

    elif name == "d_labitems":
        _show_value_counts(df, "category")
        _show_value_counts(df, "fluid")
        print(f"  Unique itemid: {df['itemid'].nunique()}")
        null_label = df["label"].isna().sum()
        print(f"  label null: {null_label}")


def _show_value_counts(df, col, top_n=5, show_empty=False):
    print(f"\n  {col} distribution (top {top_n}):")
    vc = df[col].value_counts(dropna=False).head(top_n)
    for val, cnt in vc.items():
        pct = cnt / len(df) * 100
        display_val = repr(val) if pd.isna(val) or val == "" or val == "?" else val
        print(f"    {display_val}: {cnt} ({pct:.1f}%)")
    if show_empty:
        empty_count = df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum()
        if empty_count > 0:
            print(f"    → Total empty/null: {empty_count}")


def _show_numeric_stats(df, col):
    if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
        stats = df[col].describe()
        print(f"\n  {col} stats:")
        print(f"    min={stats['min']:.2f}, 25%={stats['25%']:.2f}, "
              f"median={stats['50%']:.2f}, 75%={stats['75%']:.2f}, max={stats['max']:.2f}")
        print(f"    mean={stats['mean']:.2f}, std={stats['std']:.2f}")
    else:
        # Try converting
        numeric = pd.to_numeric(df[col], errors="coerce")
        null_after = numeric.isna().sum() - df[col].isna().sum()
        if null_after > 0:
            print(f"\n  {col}: {null_after} values failed numeric conversion")
        if numeric.notna().sum() > 0:
            stats = numeric.describe()
            print(f"  {col} numeric stats (convertible values only):")
            print(f"    min={stats['min']:.2f}, median={stats['50%']:.2f}, max={stats['max']:.2f}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 70)
    print(f"  MIMIC-IV EDA — MODE: {MODE.upper()} | Source: {RAW_DIR}")
    print("=" * 70)

    for fname in FILES:
        path = os.path.join(RAW_DIR, fname)
        table_name = fname.replace(".csv.gz", "")
        df = pd.read_csv(path, compression="gzip", low_memory=False)
        eda_table(table_name, df)

    print(f"\n{'='*70}")
    print("  EDA Complete")
    print(f"{'='*70}")