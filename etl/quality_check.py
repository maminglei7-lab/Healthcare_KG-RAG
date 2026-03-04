"""
Quality Check: Validate cleaned data against EDA-driven rules.
Runs after transform, before build_graph_input.
Rules mirror the cleaning logic — if transform did it right, all checks pass.
"""

import pandas as pd
import os
from config import CLEANED_DIR


class QualityReport:
    def __init__(self, table_name):
        self.table_name = table_name
        self.results = []

    def check(self, rule_name, passed, detail=""):
        status = "PASS" if passed else "FAIL"
        self.results.append({
            "table": self.table_name,
            "rule": rule_name,
            "status": status,
            "detail": detail,
        })

    def summary(self):
        total = len(self.results)
        failed = [r for r in self.results if r["status"] == "FAIL"]
        print(f"\n[Quality] {self.table_name}: {total - len(failed)}/{total} passed")
        for r in self.results:
            tag = r["status"]
            detail = f" — {r['detail']}" if r["detail"] else ""
            print(f"  {tag}  {r['rule']}{detail}")
        return len(failed) == 0


# ─────────────────────────────────────────────
# Quality Rules per Table
# ─────────────────────────────────────────────

def check_patients(df):
    r = QualityReport("patients")
    r.check("subject_id is unique",
            df["subject_id"].is_unique)
    r.check("subject_id has no nulls",
            df["subject_id"].notna().all())
    r.check("gender only contains M/F",
            df["gender"].isin(["M", "F"]).all(),
            f"values: {df['gender'].unique().tolist()}")
    r.check("anchor_age in range 0-120",
            df["anchor_age"].between(0, 120).all(),
            f"min={df['anchor_age'].min()}, max={df['anchor_age'].max()}")
    # dod should NOT be filled — null means alive
    r.check("dod is not universally filled (nulls preserved)",
            df["dod"].isna().any(),
            f"null count: {df['dod'].isna().sum()}")
    return r.summary()


def check_admissions(df):
    r = QualityReport("admissions")
    r.check("hadm_id is unique",
            df["hadm_id"].is_unique)
    r.check("subject_id has no nulls",
            df["subject_id"].notna().all())
    r.check("admittime has no nulls",
            df["admittime"].notna().all())
    r.check("dischtime has no nulls",
            df["dischtime"].notna().all())
    r.check("language has no '?' values",
            (df["language"] == "?").sum() == 0,
            f"'?' count: {(df['language'] == '?').sum()}")
    r.check("marital_status has no nulls (filled with UNKNOWN)",
            df["marital_status"].notna().all())
    r.check("discharge_location has no nulls (filled with UNKNOWN)",
            df["discharge_location"].notna().all())
    r.check("hospital_expire_flag only contains 0/1",
            df["hospital_expire_flag"].isin([0, 1]).all(),
            f"values: {df['hospital_expire_flag'].unique().tolist()}")
    # deathtime/edregtime/edouttime should still have nulls (preserved, not filled)
    r.check("deathtime nulls preserved (not filled)",
            df["deathtime"].isna().any(),
            f"null count: {df['deathtime'].isna().sum()}")
    return r.summary()


def check_diagnoses(df):
    r = QualityReport("diagnoses_icd")
    r.check("diagnosis_id column exists",
            "diagnosis_id" in df.columns)
    r.check("icd_version only contains 9 or 10",
            df["icd_version"].isin([9, 10]).all(),
            f"values: {df['icd_version'].unique().tolist()}")
    r.check("icd_code has no nulls",
            df["icd_code"].notna().all())
    r.check("icd_code has no leading/trailing spaces",
            (df["icd_code"] == df["icd_code"].str.strip()).all())
    r.check("diagnosis_id has correct format (_v9 or _v10)",
            df["diagnosis_id"].str.contains(r"_v(?:9|10)$", regex=True).all())
    r.check("no duplicates on (subject_id, hadm_id, seq_num)",
            not df.duplicated(subset=["subject_id", "hadm_id", "seq_num"]).any())
    return r.summary()


def check_labevents(df):
    r = QualityReport("labevents")
    r.check("labevent_id is unique",
            df["labevent_id"].is_unique)
    r.check("hadm_id has no nulls (null rows filtered out)",
            df["hadm_id"].notna().all())
    r.check("hadm_id is integer type (no floats)",
            df["hadm_id"].dtype in ["int64", "int32"])
    r.check("flag has no nulls (filled with 'normal')",
            df["flag"].notna().all())
    r.check("flag only contains 'normal' or 'abnormal'",
            df["flag"].isin(["normal", "abnormal"]).all(),
            f"values: {df['flag'].unique().tolist()}")
    r.check("itemid has no nulls",
            df["itemid"].notna().all())
    r.check("charttime has no nulls",
            df["charttime"].notna().all())
    # valuenum should still have nulls (preserved, not imputed)
    r.check("valuenum nulls preserved (not imputed)",
            df["valuenum"].isna().any(),
            f"null count: {df['valuenum'].isna().sum()}")
    # ref_range nulls are expected
    ref_both = (df["ref_range_lower"].notna() & df["ref_range_upper"].notna()).sum()
    r.check("ref_range coverage is reasonable (>70%)",
            ref_both / len(df) > 0.7,
            f"both present: {ref_both}/{len(df)} ({ref_both/len(df)*100:.1f}%)")
    return r.summary()


def check_prescriptions(df):
    r = QualityReport("prescriptions")
    r.check("hadm_id has no nulls",
            df["hadm_id"].notna().all())
    r.check("drug has no nulls",
            df["drug"].notna().all())
    r.check("drug has no leading/trailing spaces",
            (df["drug"] == df["drug"].str.strip()).all())
    r.check("route has no nulls (filled with UNKNOWN)",
            df["route"].notna().all())
    r.check("drug_type has no nulls",
            df["drug_type"].notna().all())
    r.check("drug_type only contains expected values",
            df["drug_type"].isin(["MAIN", "BASE", "ADDITIVE"]).all(),
            f"values: {df['drug_type'].unique().tolist()}")
    r.check("starttime has no nulls",
            df["starttime"].notna().all())
    return r.summary()


def check_d_icd_diagnoses(df):
    r = QualityReport("d_icd_diagnoses")
    r.check("icd_code has no nulls",
            df["icd_code"].notna().all())
    r.check("icd_code has no leading/trailing spaces",
            (df["icd_code"] == df["icd_code"].str.strip()).all())
    r.check("long_title has no nulls",
            df["long_title"].notna().all())
    r.check("icd_version only contains 9 or 10",
            df["icd_version"].isin([9, 10]).all())
    return r.summary()


def check_d_labitems(df):
    r = QualityReport("d_labitems")
    r.check("itemid is unique",
            df["itemid"].is_unique)
    r.check("label has no nulls (filled with Unknown)",
            df["label"].notna().all())
    r.check("category has no nulls",
            df["category"].notna().all())
    return r.summary()


# ─────────────────────────────────────────────
# Registry: table name → checker function
# ─────────────────────────────────────────────

CHECKERS = {
    "patients":        check_patients,
    "admissions":      check_admissions,
    "diagnoses_icd":   check_diagnoses,
    "labevents":       check_labevents,
    "prescriptions":   check_prescriptions,
    "d_icd_diagnoses": check_d_icd_diagnoses,
    "d_labitems":      check_d_labitems,
}


# ─────────────────────────────────────────────
# Main: Run all checks from cleaned CSV files
# ─────────────────────────────────────────────

def run_quality_checks(cleaned_dir=CLEANED_DIR):
    all_passed = True
    for name, checker in CHECKERS.items():
        path = os.path.join(cleaned_dir, f"{name}.csv")
        if not os.path.exists(path):
            print(f"\n[Quality] {name}: SKIPPED — file not found at {path}")
            continue
        df = pd.read_csv(path, low_memory=False)
        passed = checker(df)
        if not passed:
            all_passed = False

    print("\n" + "=" * 50)
    if all_passed:
        print("  All quality checks PASSED")
    else:
        print("  Some checks FAILED — review FAIL items above")
    print("=" * 50)
    return all_passed


if __name__ == "__main__":
    run_quality_checks()