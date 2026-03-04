"""
Build Graph Input: Generate Neo4j-ready CSV files for nodes and relationships.

Graph Schema (new):
  Layer 1 — Clinical Entity Layer:
    Patient → Admission → Diagnosis / Medication / LabTest

  Layer 2 — Knowledge Enhancement Layer:
    Diagnosis -[BELONGS_TO_CATEGORY]→ ICD_Category (hierarchy)
    LabTest ref_range stored on HAS_LAB_RESULT relationship
"""

import pandas as pd
import os
from config import CLEANED_DIR, GRAPH_INPUT_DIR, NODES_DIR, RELS_DIR, TABLE_NAMES


def makedirs():
    os.makedirs(NODES_DIR, exist_ok=True)
    os.makedirs(RELS_DIR, exist_ok=True)


def save(df, path):
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"[Save] {os.path.relpath(path, GRAPH_INPUT_DIR)} → {len(df)} rows")


def read_cleaned(table_name):
    return pd.read_csv(os.path.join(CLEANED_DIR, f"{table_name}.csv"), low_memory=False)


# ═════════════════════════════════════════════
#  LAYER 1: CLINICAL ENTITY NODES
# ═════════════════════════════════════════════

def build_patients():
    df = read_cleaned("patients")
    out = pd.DataFrame({
        "patientId:ID":  "p_" + df["subject_id"].astype(str),
        "subjectId":     df["subject_id"].astype(int),
        "gender":        df["gender"],
        "anchorAge":     df["anchor_age"].astype(int),
        "anchorYear":    df["anchor_year"].astype(int),
        ":LABEL":        "Patient",
    })
    save(out, os.path.join(NODES_DIR, "patients.csv"))


def build_admissions():
    df = read_cleaned("admissions")
    out = pd.DataFrame({
        "admissionId:ID":    "a_" + df["hadm_id"].astype(str),
        "hadmId":            df["hadm_id"].astype(int),
        "admissionType":     df["admission_type"],
        "admitTime":         df["admittime"].fillna(""),
        "dischargeTime":     df["dischtime"].fillna(""),
        "insurance":         df["insurance"],
        "language":          df["language"],
        "maritalStatus":     df["marital_status"],
        "race":              df["race"],
        ":LABEL":            "Admission",
    })
    save(out, os.path.join(NODES_DIR, "admissions.csv"))


def build_diagnoses():
    diag = read_cleaned("diagnoses_icd")
    desc = read_cleaned("d_icd_diagnoses")

    # Get unique diagnoses and merge with descriptions
    unique = diag[["icd_code", "icd_version"]].drop_duplicates()
    merged = unique.merge(desc, on=["icd_code", "icd_version"], how="left")

    out = pd.DataFrame({
        "diagnosisId:ID":  "d_" + merged["icd_code"] + "_" + merged["icd_version"].astype(str),
        "icdCode":         merged["icd_code"],
        "icdVersion":      merged["icd_version"].astype(int),
        "icdTitle":        merged["long_title"].fillna("Unknown"),
        ":LABEL":          "Diagnosis",
    })
    save(out, os.path.join(NODES_DIR, "diagnoses.csv"))


def build_labtests():
    """LabTest nodes from d_labitems dictionary table."""
    df = read_cleaned("d_labitems")
    out = pd.DataFrame({
        "labTestId:ID":  "lt_" + df["itemid"].astype(str),
        "itemId":        df["itemid"].astype(int),
        "label":         df["label"],
        "fluid":         df["fluid"],
        "category":      df["category"],
        ":LABEL":        "LabTest",
    })
    save(out, os.path.join(NODES_DIR, "labtests.csv"))


def build_medications():
    """
    Medication nodes from prescriptions — one node per unique drug name.
    Dose/route/time are stored on the HAS_PRESCRIPTION relationship instead,
    since they vary per prescription instance.
    """
    df = read_cleaned("prescriptions")

    # One node per unique drug name
    unique_drugs = df[["drug"]].drop_duplicates().reset_index(drop=True)

    out = pd.DataFrame({
        "medicationId:ID":  "med_" + unique_drugs.index.astype(str),
        "drugName":         unique_drugs["drug"],
        ":LABEL":           "Medication",
    })

    # Build a lookup for later use in relationship building
    # Save drug → medicationId mapping alongside the node file
    mapping = pd.DataFrame({
        "drug":            unique_drugs["drug"],
        "medicationId":    "med_" + unique_drugs.index.astype(str),
    })
    mapping.to_csv(os.path.join(NODES_DIR, "_medication_lookup.csv"), index=False)

    save(out, os.path.join(NODES_DIR, "medications.csv"))


# ═════════════════════════════════════════════
#  LAYER 2: KNOWLEDGE ENHANCEMENT NODES
# ═════════════════════════════════════════════

def build_icd_categories():
    """
    ICD hierarchy: extract parent category from each diagnosis code.
      ICD-10: first 3 chars (e.g., E119 → E11, I5021 → I50)
      ICD-9:  first 3 chars (e.g., 41401 → 414, 4139 → 413)

    Only builds categories for codes that actually appear in our diagnoses.
    """
    diag = read_cleaned("diagnoses_icd")
    desc = read_cleaned("d_icd_diagnoses")

    # Get unique (icd_code, icd_version) from actual diagnoses
    unique_codes = diag[["icd_code", "icd_version"]].drop_duplicates()

    # Extract parent code: first 3 characters
    unique_codes = unique_codes.copy()
    unique_codes["parent_code"] = unique_codes["icd_code"].str[:3]

    # Get unique parent categories
    parents = unique_codes[["parent_code", "icd_version"]].drop_duplicates().reset_index(drop=True)

    # Try to find a representative title for each parent category
    # Match parent_code to d_icd_diagnoses entries whose code starts with that prefix
    parent_titles = []
    for _, row in parents.iterrows():
        p_code = row["parent_code"]
        p_ver = row["icd_version"]
        # Look for exact match first (some parent codes exist as entries themselves)
        exact = desc[(desc["icd_code"] == p_code) & (desc["icd_version"] == p_ver)]
        if len(exact) > 0:
            parent_titles.append(exact.iloc[0]["long_title"])
        else:
            # Use the first child's title as representative
            children = desc[(desc["icd_code"].str.startswith(p_code)) & (desc["icd_version"] == p_ver)]
            if len(children) > 0:
                parent_titles.append(children.iloc[0]["long_title"])
            else:
                parent_titles.append("Unknown")

    parents["categoryTitle"] = parent_titles

    # Build ICD_Category nodes
    out = pd.DataFrame({
        "categoryId:ID":    "icd_" + parents["parent_code"] + "_v" + parents["icd_version"].astype(str),
        "code":             parents["parent_code"],
        "icdVersion":       parents["icd_version"].astype(int),
        "title":            parents["categoryTitle"],
        ":LABEL":           "ICD_Category",
    })
    save(out, os.path.join(NODES_DIR, "icd_categories.csv"))


# ═════════════════════════════════════════════
#  LAYER 1: RELATIONSHIPS
# ═════════════════════════════════════════════

def build_had_admission():
    df = read_cleaned("admissions")
    out = pd.DataFrame({
        ":START_ID":  "p_" + df["subject_id"].astype(str),
        ":END_ID":    "a_" + df["hadm_id"].astype(str),
        ":TYPE":      "HAD_ADMISSION",
    })
    save(out, os.path.join(RELS_DIR, "had_admission.csv"))


def build_has_diagnosis():
    df = read_cleaned("diagnoses_icd")
    out = pd.DataFrame({
        ":START_ID":  "a_" + df["hadm_id"].astype(str),
        ":END_ID":    "d_" + df["icd_code"] + "_" + df["icd_version"].astype(str),
        ":TYPE":      "HAS_DIAGNOSIS",
        "seqNum":     df["seq_num"].astype(int),
    })
    save(out, os.path.join(RELS_DIR, "has_diagnosis.csv"))


def build_has_lab_result():
    """
    Admission → LabTest relationship.
    Carries per-instance data: value, flag, chartTime, ref_range.
    ref_range on relationship enables abnormal detection queries.
    """
    labs = read_cleaned("labevents")

    out = pd.DataFrame({
        ":START_ID":       "a_" + labs["hadm_id"].astype(int).astype(str),
        ":END_ID":         "lt_" + labs["itemid"].astype(str),
        ":TYPE":           "HAS_LAB_RESULT",
        "value":           labs["valuenum"],
        "flag":            labs["flag"],
        "chartTime":       labs["charttime"].fillna(""),
        "refRangeLower":   labs["ref_range_lower"],
        "refRangeUpper":   labs["ref_range_upper"],
    })
    save(out, os.path.join(RELS_DIR, "has_lab_result.csv"))


def build_has_prescription():
    """
    Admission → Medication relationship.
    Uses drug name to look up medicationId from the mapping file.
    Carries per-instance data: dose, route, starttime, stoptime.
    """
    df = read_cleaned("prescriptions")
    lookup = pd.read_csv(os.path.join(NODES_DIR, "_medication_lookup.csv"))

    # Merge to get medicationId
    merged = df.merge(lookup, on="drug", how="left")

    out = pd.DataFrame({
        ":START_ID":      "a_" + merged["hadm_id"].astype(int).astype(str),
        ":END_ID":        merged["medicationId"],
        ":TYPE":          "HAS_PRESCRIPTION",
        "drugType":       merged["drug_type"],
        "doseVal":        merged["dose_val_rx"].fillna(""),
        "doseUnit":       merged["dose_unit_rx"].fillna(""),
        "route":          merged["route"],
        "startTime":      merged["starttime"].fillna(""),
        "stopTime":       merged["stoptime"].fillna(""),
    })
    save(out, os.path.join(RELS_DIR, "has_prescription.csv"))


# ═════════════════════════════════════════════
#  LAYER 2: RELATIONSHIPS
# ═════════════════════════════════════════════

def build_belongs_to_category():
    """
    Diagnosis → ICD_Category relationship.
    Links each specific diagnosis to its parent category via first-3-char extraction.
    """
    diag = read_cleaned("diagnoses_icd")

    # Get unique diagnoses
    unique = diag[["icd_code", "icd_version"]].drop_duplicates()
    unique = unique.copy()
    unique["parent_code"] = unique["icd_code"].str[:3]

    out = pd.DataFrame({
        ":START_ID":  "d_" + unique["icd_code"] + "_" + unique["icd_version"].astype(str),
        ":END_ID":    "icd_" + unique["parent_code"] + "_v" + unique["icd_version"].astype(str),
        ":TYPE":      "BELONGS_TO_CATEGORY",
    })
    save(out, os.path.join(RELS_DIR, "belongs_to_category.csv"))


# ═════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════

if __name__ == "__main__":
    makedirs()

    print("=== Layer 1: Clinical Entity Nodes ===")
    build_patients()
    build_admissions()
    build_diagnoses()
    build_labtests()
    build_medications()

    print("\n=== Layer 2: Knowledge Enhancement Nodes ===")
    build_icd_categories()

    print("\n=== Layer 1: Relationships ===")
    build_had_admission()
    build_has_diagnosis()
    build_has_lab_result()
    build_has_prescription()

    print("\n=== Layer 2: Relationships ===")
    build_belongs_to_category()

    print("\n✓ graph_input build complete")
    print(f"  Nodes: {NODES_DIR}")
    print(f"  Rels:  {RELS_DIR}")