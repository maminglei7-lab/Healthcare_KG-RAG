"""
Transform: Clean and standardize all tables based on EDA-driven rules.

Cleaning philosophy (new):
  - Only fill nulls when the semantics are clear (e.g., flag=null means "normal")
  - Preserve null when it carries meaning (e.g., dod=null means "alive")
  - Filter out rows that cannot participate in graph relationships
  - Do NOT fabricate data (e.g., no median imputation for valuenum)
"""

import pandas as pd
from lineage_decorator import capture_lineage


# ─────────────────────────────────────────────
# PATIENTS
# ─────────────────────────────────────────────

def transform_patients(df):
    """
    Cleaning rules (based on EDA):
      - No nulls in core fields (subject_id, gender, anchor_age)
      - dod: 69% null → keep null (null = alive, meaningful)
      - Deduplicate on subject_id
    """
    print(f"[Transform] patients: {len(df)} rows")
    df = df.copy()

    before = len(df)
    df = df.drop_duplicates(subset=["subject_id"])
    if before != len(df):
        print(f"  Dedup: {before} → {len(df)}")

    print(f"[Transform] patients: done")
    return df


# ─────────────────────────────────────────────
# ADMISSIONS
# ─────────────────────────────────────────────

@capture_lineage(
    sources=["admissions.language"],
    target="admissions.language",
    transformation="replace_unknown",
    description="Replace '?' placeholder in language with 'UNKNOWN'"
)
def clean_admissions_language(df):
    df = df.copy()
    df["language"] = df["language"].replace("?", "UNKNOWN")
    return df

@capture_lineage(
    sources=["admissions.marital_status"],
    target="admissions.marital_status",
    transformation="fill_missing",
    description="Fill null marital_status with 'UNKNOWN'"
)
def clean_admissions_marital(df):
    df = df.copy()
    df["marital_status"] = df["marital_status"].fillna("UNKNOWN")
    return df

@capture_lineage(
    sources=["admissions.discharge_location"],
    target="admissions.discharge_location",
    transformation="fill_missing",
    description="Fill null discharge_location with 'UNKNOWN'"
)
def clean_admissions_discharge_location(df):
    df = df.copy()
    df["discharge_location"] = df["discharge_location"].fillna("UNKNOWN")
    return df

def transform_admissions(df):
    """
    Cleaning rules (based on EDA):
      - language: 20 rows "?" (7.3%) → "UNKNOWN"
      - marital_status: 12 null (4.4%) → "UNKNOWN"
      - discharge_location: 42 null (15.3%) → "UNKNOWN"
      - deathtime/edregtime/edouttime: keep null (semantically meaningful)
      - Deduplicate on hadm_id
    """
    print(f"[Transform] admissions: {len(df)} rows")
    df = clean_admissions_language(df)
    df = clean_admissions_marital(df)
    df = clean_admissions_discharge_location(df)

    before = len(df)
    df = df.drop_duplicates(subset=["hadm_id"])
    if before != len(df):
        print(f"  Dedup: {before} → {len(df)}")

    print(f"[Transform] admissions: done")
    return df


# ─────────────────────────────────────────────
# DIAGNOSES_ICD
# ─────────────────────────────────────────────

@capture_lineage(
    sources=["diagnoses_icd.icd_code", "diagnoses_icd.icd_version"],
    target="diagnoses_icd.diagnosis_id",
    transformation="concat_version_suffix",
    description="Generate diagnosis_id = icd_code + '_v' + icd_version to avoid ICD-9/10 code collisions"
)
def add_diagnosis_id(df):
    df = df.copy()
    df["icd_code"] = df["icd_code"].astype(str).str.strip()
    df["diagnosis_id"] = df["icd_code"] + "_v" + df["icd_version"].astype(str)
    return df

def transform_diagnoses(df):
    """
    Cleaning rules (based on EDA):
      - Zero nulls, zero duplicates — data quality excellent
      - Strip whitespace from icd_code (defensive)
      - Generate diagnosis_id for unique graph node identification
      - Deduplicate on (subject_id, hadm_id, seq_num)
    """
    print(f"[Transform] diagnoses_icd: {len(df)} rows")
    df = add_diagnosis_id(df)

    before = len(df)
    df = df.drop_duplicates(subset=["subject_id", "hadm_id", "seq_num"])
    if before != len(df):
        print(f"  Dedup: {before} → {len(df)}")

    print(f"[Transform] diagnoses_icd: done")
    return df


# ─────────────────────────────────────────────
# LABEVENTS
# ─────────────────────────────────────────────

@capture_lineage(
    sources=["labevents.flag"],
    target="labevents.flag",
    transformation="fill_missing",
    description="Fill null flag with 'normal' — per MIMIC-IV docs, null means result within normal range"
)
def clean_labevents_flag(df):
    df = df.copy()
    df["flag"] = df["flag"].fillna("normal")
    return df

def transform_labevents(df):
    """
    Cleaning rules (based on EDA):
      - hadm_id: 26.4% null → FILTER OUT (cannot link to Admission in graph)
      - valuenum: 11.6% null → keep null (do NOT impute — fabricated values harm RAG accuracy)
      - flag: 62.6% null → fill "normal" (MIMIC-IV semantics: null = normal)
      - ref_range_lower/upper: 17.4% null → keep null (some tests have no reference range)
      - Deduplicate on labevent_id
    """
    print(f"[Transform] labevents: {len(df)} rows")
    df = df.copy()

    # Filter out rows with no hadm_id — these cannot form Admission→LabTest relationships
    before_filter = len(df)
    df = df[df["hadm_id"].notna()].copy()
    df["hadm_id"] = df["hadm_id"].astype(int)
    print(f"  Filter hadm_id null: {before_filter} → {len(df)} ({before_filter - len(df)} removed)")

    df = clean_labevents_flag(df)

    before = len(df)
    df = df.drop_duplicates(subset=["labevent_id"])
    if before != len(df):
        print(f"  Dedup: {before} → {len(df)}")

    print(f"[Transform] labevents: done")
    return df


# ─────────────────────────────────────────────
# PRESCRIPTIONS (new)
# ─────────────────────────────────────────────

@capture_lineage(
    sources=["prescriptions.drug"],
    target="prescriptions.drug",
    transformation="replace_unknown",
    description="Standardize drug names: strip whitespace and normalize casing"
)
def clean_prescriptions_drug(df):
    df = df.copy()
    df["drug"] = df["drug"].astype(str).str.strip()
    return df

@capture_lineage(
    sources=["prescriptions.route"],
    target="prescriptions.route",
    transformation="fill_missing",
    description="Fill null route with 'UNKNOWN'"
)
def clean_prescriptions_route(df):
    df = df.copy()
    df["route"] = df["route"].fillna("UNKNOWN")
    return df

def transform_prescriptions(df):
    """
    Cleaning rules (based on EDA):
      - drug: 0 null, 631 unique → strip whitespace, normalize casing
      - route: 6 null (0.03%) → fill "UNKNOWN"
      - dose_val_rx/dose_unit_rx: 9 null (0.05%) → keep null (doesn't affect "what drug" queries)
      - form_rx: 99.9% null → will not be imported to graph
      - doses_per_24_hrs: 40.8% null → not a core attribute
      - hadm_id: 0 null → all rows can link to Admission
    """
    print(f"[Transform] prescriptions: {len(df)} rows")
    df = clean_prescriptions_drug(df)
    df = clean_prescriptions_route(df)
    print(f"[Transform] prescriptions: done")
    return df


# ─────────────────────────────────────────────
# Dictionary Tables
# ─────────────────────────────────────────────

def transform_d_icd_diagnoses(df):
    """Pass-through with defensive strip on icd_code."""
    df = df.copy()
    df["icd_code"] = df["icd_code"].astype(str).str.strip()
    print(f"[Transform] d_icd_diagnoses: {len(df)} rows, icd_code stripped")
    return df

def transform_d_labitems(df):
    """Fill 3 null labels with 'Unknown'."""
    df = df.copy()
    df["label"] = df["label"].fillna("Unknown")
    print(f"[Transform] d_labitems: {len(df)} rows, label nulls filled")
    return df


# ─────────────────────────────────────────────
# Unified Portal
# ─────────────────────────────────────────────

def transform_all(tables):
    """Run all transformations. Input/output: {table_name: DataFrame}"""
    return {
        "patients":        transform_patients(tables["patients"]),
        "admissions":      transform_admissions(tables["admissions"]),
        "diagnoses_icd":   transform_diagnoses(tables["diagnoses_icd"]),
        "labevents":       transform_labevents(tables["labevents"]),
        "prescriptions":   transform_prescriptions(tables["prescriptions"]),
        "d_icd_diagnoses": transform_d_icd_diagnoses(tables["d_icd_diagnoses"]),
        "d_labitems":      transform_d_labitems(tables["d_labitems"]),
    }