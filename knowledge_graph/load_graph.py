"""
Load Graph: Import nodes and relationships from CSV into Neo4j.

Schema:
  Layer 1 — Clinical Entities:
    (Patient)-[HAD_ADMISSION]->(Admission)
    (Admission)-[HAS_DIAGNOSIS]->(Diagnosis)
    (Admission)-[HAS_LAB_RESULT]->(LabTest)
    (Admission)-[HAS_PRESCRIPTION]->(Medication)

  Layer 2 — Knowledge Enhancement:
    (Diagnosis)-[BELONGS_TO_CATEGORY]->(ICD_Category)
"""

import sys
import os

# Add etl directory to path so we can import config
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "etl"))

import pandas as pd
from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, NODES_DIR, RELS_DIR

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))


# ─────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────

def run(session, query, rows, batch=500):
    for i in range(0, len(rows), batch):
        session.run(query, rows=rows[i:i + batch])
    print(f"  → {len(rows)} rows imported")


def load_csv(dir_path, filename):
    df = pd.read_csv(f"{dir_path}/{filename}", low_memory=False)
    return df


# ─────────────────────────────────────────────
# 1. Clear & Setup
# ─────────────────────────────────────────────

def clear_db(session):
    session.run("MATCH (n) DETACH DELETE n")
    print("✓ Database cleared")


def create_constraints(session):
    constraints = [
        ("Patient",      "patientId"),
        ("Admission",    "admissionId"),
        ("Diagnosis",    "diagnosisId"),
        ("LabTest",      "labTestId"),
        ("Medication",   "medicationId"),
        ("ICD_Category", "categoryId"),
    ]
    for label, prop in constraints:
        session.run(
            f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE"
        )
    print(f"✓ {len(constraints)} constraints created")


# ─────────────────────────────────────────────
# 2. Layer 1 Nodes
# ─────────────────────────────────────────────

def load_patients(session):
    df = load_csv(NODES_DIR, "patients.csv")
    rows = df.rename(columns={"patientId:ID": "patientId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (p:Patient {patientId: r.patientId}) "
        "SET p.subjectId = r.subjectId, p.gender = r.gender, "
        "    p.anchorAge = r.anchorAge, p.anchorYear = r.anchorYear",
        rows)
    print(f"✓ Patients: {len(rows)}")


def load_admissions(session):
    df = load_csv(NODES_DIR, "admissions.csv")
    rows = df.rename(columns={"admissionId:ID": "admissionId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (a:Admission {admissionId: r.admissionId}) "
        "SET a.hadmId = r.hadmId, a.admissionType = r.admissionType, "
        "    a.admitTime = r.admitTime, a.dischargeTime = r.dischargeTime, "
        "    a.insurance = r.insurance, a.language = r.language, "
        "    a.maritalStatus = r.maritalStatus, a.race = r.race",
        rows)
    print(f"✓ Admissions: {len(rows)}")


def load_diagnoses(session):
    df = load_csv(NODES_DIR, "diagnoses.csv")
    rows = df.rename(columns={"diagnosisId:ID": "diagnosisId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (d:Diagnosis {diagnosisId: r.diagnosisId}) "
        "SET d.icdCode = r.icdCode, d.icdVersion = r.icdVersion, "
        "    d.icdTitle = r.icdTitle",
        rows)
    print(f"✓ Diagnoses: {len(rows)}")


def load_labtests(session):
    df = load_csv(NODES_DIR, "labtests.csv")
    rows = df.rename(columns={"labTestId:ID": "labTestId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (l:LabTest {labTestId: r.labTestId}) "
        "SET l.itemId = r.itemId, l.label = r.label, "
        "    l.fluid = r.fluid, l.category = r.category",
        rows)
    print(f"✓ LabTests: {len(rows)}")


def load_medications(session):
    df = load_csv(NODES_DIR, "medications.csv")
    rows = df.rename(columns={"medicationId:ID": "medicationId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (m:Medication {medicationId: r.medicationId}) "
        "SET m.drugName = r.drugName",
        rows)
    print(f"✓ Medications: {len(rows)}")


# ─────────────────────────────────────────────
# 3. Layer 2 Nodes
# ─────────────────────────────────────────────

def load_icd_categories(session):
    df = load_csv(NODES_DIR, "icd_categories.csv")
    rows = df.rename(columns={"categoryId:ID": "categoryId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (c:ICD_Category {categoryId: r.categoryId}) "
        "SET c.code = r.code, c.icdVersion = r.icdVersion, "
        "    c.title = r.title",
        rows)
    print(f"✓ ICD_Categories: {len(rows)}")


# ─────────────────────────────────────────────
# 4. Layer 1 Relationships
# ─────────────────────────────────────────────

def load_had_admission(session):
    df = load_csv(RELS_DIR, "had_admission.csv")
    rows = df.rename(columns={":START_ID": "start", ":END_ID": "end"}).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MATCH (p:Patient {patientId: r.start}), (a:Admission {admissionId: r.end}) "
        "MERGE (p)-[:HAD_ADMISSION]->(a)",
        rows)
    print(f"✓ HAD_ADMISSION: {len(rows)}")


def load_has_diagnosis(session):
    df = load_csv(RELS_DIR, "has_diagnosis.csv")
    rows = df.rename(columns={":START_ID": "start", ":END_ID": "end"}).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MATCH (a:Admission {admissionId: r.start}), (d:Diagnosis {diagnosisId: r.end}) "
        "MERGE (a)-[rel:HAS_DIAGNOSIS]->(d) "
        "SET rel.seqNum = r.seqNum",
        rows)
    print(f"✓ HAS_DIAGNOSIS: {len(rows)}")


def load_has_lab_result(session):
    df = load_csv(RELS_DIR, "has_lab_result.csv")
    # Handle NaN → None for Neo4j compatibility
    df = df.where(df.notna(), None)
    rows = df.rename(columns={":START_ID": "start", ":END_ID": "end"}).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MATCH (a:Admission {admissionId: r.start}), (l:LabTest {labTestId: r.end}) "
        "CREATE (a)-[rel:HAS_LAB_RESULT]->(l) "
        "SET rel.value = r.value, rel.flag = r.flag, "
        "    rel.chartTime = r.chartTime, "
        "    rel.refRangeLower = r.refRangeLower, "
        "    rel.refRangeUpper = r.refRangeUpper",
        rows)
    print(f"✓ HAS_LAB_RESULT: {len(rows)}")


def load_has_prescription(session):
    df = load_csv(RELS_DIR, "has_prescription.csv")
    rows = df.rename(columns={":START_ID": "start", ":END_ID": "end"}).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MATCH (a:Admission {admissionId: r.start}), (m:Medication {medicationId: r.end}) "
        "CREATE (a)-[rel:HAS_PRESCRIPTION]->(m) "
        "SET rel.drugType = r.drugType, rel.doseVal = r.doseVal, "
        "    rel.doseUnit = r.doseUnit, rel.route = r.route, "
        "    rel.startTime = r.startTime, rel.stopTime = r.stopTime",
        rows)
    print(f"✓ HAS_PRESCRIPTION: {len(rows)}")


# ─────────────────────────────────────────────
# 5. Layer 2 Relationships
# ─────────────────────────────────────────────

def load_belongs_to_category(session):
    df = load_csv(RELS_DIR, "belongs_to_category.csv")
    rows = df.rename(columns={":START_ID": "start", ":END_ID": "end"}).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MATCH (d:Diagnosis {diagnosisId: r.start}), (c:ICD_Category {categoryId: r.end}) "
        "MERGE (d)-[:BELONGS_TO_CATEGORY]->(c)",
        rows)
    print(f"✓ BELONGS_TO_CATEGORY: {len(rows)}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

if __name__ == "__main__":
    with driver.session() as session:
        print("=== Clear Database ===")
        clear_db(session)

        print("\n=== Create Constraints ===")
        create_constraints(session)

        print("\n=== Load Layer 1 Nodes ===")
        load_patients(session)
        load_admissions(session)
        load_diagnoses(session)
        load_labtests(session)
        load_medications(session)

        print("\n=== Load Layer 2 Nodes ===")
        load_icd_categories(session)

        print("\n=== Load Layer 1 Relationships ===")
        load_had_admission(session)
        load_has_diagnosis(session)
        load_has_lab_result(session)
        load_has_prescription(session)

        print("\n=== Load Layer 2 Relationships ===")
        load_belongs_to_category(session)

        # ── Summary ──
        result = session.run(
            "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count "
            "ORDER BY count DESC"
        )
        print("\n=== Graph Summary ===")
        total_nodes = 0
        for record in result:
            print(f"  {record['label']}: {record['count']}")
            total_nodes += record["count"]

        result = session.run(
            "MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count "
            "ORDER BY count DESC"
        )
        total_rels = 0
        for record in result:
            print(f"  {record['type']}: {record['count']}")
            total_rels += record["count"]

        print(f"\n  Total: {total_nodes} nodes, {total_rels} relationships")
        print("✓ Graph loaded successfully")

    driver.close()