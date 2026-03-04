"""
Centralized configuration for the entire ETL + Graph pipeline.
All scripts import from here — change once, apply everywhere.
"""

import os

# ─────────────────────────────────────────────
# Mode Switch: "demo" or "full"
# ─────────────────────────────────────────────
MODE = "demo"

# ─────────────────────────────────────────────
# Base Directories
# ─────────────────────────────────────────────
PROJECT_ROOT = r"D:\Desktop\DAMG 7374\healthcare_lineagetracking"

BASE_PATHS = {
    "demo": {
        "raw":         os.path.join(PROJECT_ROOT, "data", "raw"),
        "cleaned":     os.path.join(PROJECT_ROOT, "data", "cleaned"),
        "graph_input": os.path.join(PROJECT_ROOT, "data", "graph_input"),
    },
    "full": {
        "raw":         r"D:\Desktop\project\data\raw_full",
        "cleaned":     r"D:\Desktop\project\data\cleaned_full",
        "graph_input": r"D:\Desktop\project\data\graph_input_full",
    },
}

# Resolved paths for current MODE
RAW_DIR         = BASE_PATHS[MODE]["raw"]
CLEANED_DIR     = BASE_PATHS[MODE]["cleaned"]
GRAPH_INPUT_DIR = BASE_PATHS[MODE]["graph_input"]
NODES_DIR       = os.path.join(GRAPH_INPUT_DIR, "nodes")
RELS_DIR        = os.path.join(GRAPH_INPUT_DIR, "relationships")
LINEAGE_DIR     = os.path.join(PROJECT_ROOT, "lineage")
LINEAGE_PATH    = os.path.join(LINEAGE_DIR, "lineage.json")

# ─────────────────────────────────────────────
# Source Files (shared across demo and full)
# ─────────────────────────────────────────────
SOURCE_FILES = [
    "patients.csv.gz",
    "admissions.csv.gz",
    "diagnoses_icd.csv.gz",
    "labevents.csv.gz",
    "d_icd_diagnoses.csv.gz",
    "d_labitems.csv.gz",
    "prescriptions.csv.gz",
]

# ─────────────────────────────────────────────
# Table Metadata
# ─────────────────────────────────────────────
TABLE_NAMES = [f.replace(".csv.gz", "") for f in SOURCE_FILES]

TABLE_DESCRIPTIONS = {
    "patients":        "Patient demographic information",
    "admissions":      "Hospital admission records",
    "diagnoses_icd":   "ICD diagnosis codes per admission",
    "labevents":       "Laboratory test results",
    "d_icd_diagnoses": "ICD diagnosis code dictionary",
    "d_labitems":      "Lab item dictionary",
    "prescriptions":   "Medication prescriptions",
}

# ─────────────────────────────────────────────
# Neo4j Connection
# ─────────────────────────────────────────────
NEO4J_URI      = "neo4j://127.0.0.1:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "Mml19980131!!!!"
NEO4J_DATABASE = "neo4j"

# ─────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────
def print_config():
    """Print current configuration for verification."""
    print(f"MODE:        {MODE.upper()}")
    print(f"RAW_DIR:     {RAW_DIR}")
    print(f"CLEANED_DIR: {CLEANED_DIR}")
    print(f"GRAPH_INPUT: {GRAPH_INPUT_DIR}")
    print(f"NEO4J_URI:   {NEO4J_URI}")