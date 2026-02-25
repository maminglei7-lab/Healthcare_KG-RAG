import pandas as pd
from neo4j import GraphDatabase

# ─────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────
URI      = "neo4j://127.0.0.1:7687"
USERNAME = "neo4j"
PASSWORD = "Mml19980131!!!!"

GRAPH_INPUT = r"D:\Desktop\DAMG 7374\healthcare_lineagetracking\data\graph_input"
NODES_DIR   = GRAPH_INPUT + r"\nodes"
RELS_DIR    = GRAPH_INPUT + r"\relationships"

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

# ─────────────────────────────────────────
# Helper
# ─────────────────────────────────────────
def run(session, query, rows, batch=500):
    for i in range(0, len(rows), batch):
        session.run(query, rows=rows[i:i+batch])
        print(f"  imported rows {i} ~ {min(i+batch, len(rows))}")

# ─────────────────────────────────────────
# 1. Clear existing data
# ─────────────────────────────────────────
def clear_db(session):
    session.run("MATCH (n) DETACH DELETE n")
    print("✓ Database cleared")

# ─────────────────────────────────────────
# 2. Create constraints (unique IDs)
# ─────────────────────────────────────────
def create_constraints(session):
    constraints = [
        ("Patient",        "patientId"),
        ("Admission",      "admissionId"),
        ("Diagnosis",      "diagnosisId"),
        ("LabTest",        "labTestId"),
        ("Field",          "fieldId"),
        ("Transformation", "transformationId"),
    ]
    for label, prop in constraints:
        session.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE")
    print("✓ Constraints created")

# ─────────────────────────────────────────
# 3. Import Nodes
# ─────────────────────────────────────────
def load_patients(session):
    df = pd.read_csv(f"{NODES_DIR}/patients.csv")
    rows = df.rename(columns={"patientId:ID": "patientId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (p:Patient {patientId: r.patientId}) "
        "SET p.subjectId = r.subjectId, p.gender = r.gender, "
        "    p.anchorAge = r.anchorAge, p.anchorYear = r.anchorYear",
        rows)
    print(f"✓ Patients: {len(rows)}")

def load_admissions(session):
    df = pd.read_csv(f"{NODES_DIR}/admissions.csv")
    rows = df.rename(columns={"admissionId:ID": "admissionId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (a:Admission {admissionId: r.admissionId}) "
        "SET a.hadmId = r.hadmId, a.admissionType = r.admissionType, "
        "    a.admitTime = r.admitTime, a.dischargeTime = r.dischargeTime, "
        "    a.hospital = r.hospital",
        rows)
    print(f"✓ Admissions: {len(rows)}")

def load_diagnoses(session):
    df = pd.read_csv(f"{NODES_DIR}/diagnoses.csv")
    rows = df.rename(columns={"diagnosisId:ID": "diagnosisId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (d:Diagnosis {diagnosisId: r.diagnosisId}) "
        "SET d.icdCode = r.icdCode, d.icdVersion = r.icdVersion, "
        "    d.icdTitle = r.icdTitle, d.category = r.category",
        rows)
    print(f"✓ Diagnoses: {len(rows)}")

def load_labtests(session):
    df = pd.read_csv(f"{NODES_DIR}/labtests.csv")
    rows = df.rename(columns={"labTestId:ID": "labTestId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (l:LabTest {labTestId: r.labTestId}) "
        "SET l.itemId = r.itemId, l.label = r.label, "
        "    l.category = r.category, l.unit = r.unit",
        rows)
    print(f"✓ LabTests: {len(rows)}")

def load_fields(session):
    df = pd.read_csv(f"{NODES_DIR}/fields.csv")
    rows = df.rename(columns={"fieldId:ID": "fieldId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (f:Field {fieldId: r.fieldId}) "
        "SET f.fieldName = r.fieldName, f.tableName = r.tableName, "
        "    f.dataType = r.dataType, f.isSource = r.isSource, "
        "    f.description = r.description",
        rows)
    print(f"✓ Fields: {len(rows)}")

def load_transformations(session):
    df = pd.read_csv(f"{NODES_DIR}/transformations.csv")
    rows = df.rename(columns={"transformationId:ID": "transformationId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (t:Transformation {transformationId: r.transformationId}) "
        "SET t.transformType = r.transformType, t.logic = r.logic, "
        "    t.timestamp = r.timestamp, t.scriptRef = r.scriptRef",
        rows)
    print(f"✓ Transformations: {len(rows)}")

def load_tables(session):
    df = pd.read_csv(f"{NODES_DIR}/tables.csv")
    rows = df.rename(columns={"tableId:ID": "tableId"}).drop(columns=[":LABEL"]).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MERGE (t:Table {tableId: r.tableId}) "
        "SET t.tableName = r.tableName, t.description = r.description",
        rows)
    print(f"✓ Tables: {len(rows)}")

# ─────────────────────────────────────────
# 4. Import Relationships
# ─────────────────────────────────────────
def load_had_admission(session):
    df = pd.read_csv(f"{RELS_DIR}/had_admission.csv")
    rows = df.rename(columns={":START_ID": "start", ":END_ID": "end"}).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MATCH (p:Patient {patientId: r.start}), (a:Admission {admissionId: r.end}) "
        "MERGE (p)-[:HAD_ADMISSION]->(a)",
        rows)
    print(f"✓ HAD_ADMISSION: {len(rows)}")

def load_has_diagnosis(session):
    df = pd.read_csv(f"{RELS_DIR}/has_diagnosis.csv")
    rows = df.rename(columns={":START_ID": "start", ":END_ID": "end"}).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MATCH (a:Admission {admissionId: r.start}), (d:Diagnosis {diagnosisId: r.end}) "
        "MERGE (a)-[rel:HAS_DIAGNOSIS]->(d) "
        "SET rel.seqNum = r.seqNum",
        rows)
    print(f"✓ HAS_DIAGNOSIS: {len(rows)}")

def load_has_lab_result(session):
    df = pd.read_csv(f"{RELS_DIR}/has_lab_result.csv")
    rows = df.rename(columns={":START_ID": "start", ":END_ID": "end"}).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MATCH (a:Admission {admissionId: r.start}), (l:LabTest {labTestId: r.end}) "
        "MERGE (a)-[rel:HAS_LAB_RESULT]->(l) "
        "SET rel.value = r.value, rel.abnormalFlag = r.abnormalFlag, rel.chartTime = r.chartTime",
        rows)
    print(f"✓ HAS_LAB_RESULT: {len(rows)}")

def load_derived_from(session):
    df = pd.read_csv(f"{RELS_DIR}/derived_from.csv")
    rows = df.rename(columns={":START_ID": "start", ":END_ID": "end"}).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MATCH (f1:Field {fieldId: r.start}), (f2:Field {fieldId: r.end}) "
        "MERGE (f1)-[rel:DERIVED_FROM]->(f2) "
        "SET rel.derivationType = r.derivationType",
        rows)
    print(f"✓ DERIVED_FROM: {len(rows)}")

def load_belongs_to(session):
    df = pd.read_csv(f"{RELS_DIR}/belongs_to.csv")
    rows = df.rename(columns={":START_ID": "start", ":END_ID": "end"}).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MATCH (f:Field {fieldId: r.start}), (t:Table {tableId: r.end}) "
        "MERGE (f)-[:BELONGS_TO]->(t)",
        rows)
    print(f"✓ BELONGS_TO: {len(rows)}")

def load_transformed_by(session):
    df = pd.read_csv(f"{RELS_DIR}/transformed_by.csv")
    rows = df.rename(columns={":START_ID": "start", ":END_ID": "end"}).to_dict("records")
    run(session,
        "UNWIND $rows AS r "
        "MATCH (f:Field {fieldId: r.start}), (t:Transformation {transformationId: r.end}) "
        "MERGE (f)-[:TRANSFORMED_BY]->(t)",
        rows)
    print(f"✓ TRANSFORMED_BY: {len(rows)}")

# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────
if __name__ == "__main__":
    with driver.session() as session:
        print("=== Clearing database ===")
        clear_db(session)

        print("\n=== Creating constraints ===")
        create_constraints(session)

        print("\n=== Loading nodes ===")
        load_patients(session)
        load_admissions(session)
        load_diagnoses(session)
        load_labtests(session)
        load_fields(session)
        load_tables(session)
        load_transformations(session)

        print("\n=== Loading relationships ===")
        load_had_admission(session)
        load_has_diagnosis(session)
        load_has_lab_result(session)
        load_derived_from(session)
        load_belongs_to(session)
        load_transformed_by(session)

        print("\nAll done! Graph loaded successfully.")

    driver.close()