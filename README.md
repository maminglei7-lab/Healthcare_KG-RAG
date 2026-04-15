# MedGraphRAG-EHR
### Healthcare Knowledge Graph for Intelligent Medical Query via Graph RAG

> A three-stage Graph RAG pipeline built on MIMIC-IV EHR data and Neo4j, inspired by [MedGraphRAG (ACL 2025)](https://arxiv.org/abs/2408.04187). Enables clinical researchers and data analysts to query complex patient data through natural language — no Cypher required.

---

## Overview

Traditional EHR systems require SQL or graph query expertise to extract clinical insights. **MedGraphRAG-EHR** bridges this gap by combining a two-layer Healthcare Knowledge Graph with a three-stage Graph RAG pipeline, enabling natural language queries that return evidence-based, traceable answers with interactive reasoning path visualization.

**Key capabilities:**
- Natural language → Cypher → structured answer, end-to-end
- Multi-hop reasoning across Patient → Admission → Diagnosis → Medication → LabTest
- Compound query decomposition (Agentic AI planning)
- Interactive subgraph visualization with full reasoning path
- Full-chain traceability: query-side (Graph RAG) + data-side (ETL lineage)

---

## Architecture

### Two-Layer Knowledge Graph

```
Layer 1 — Clinical Entity Graph
  Patient → Admission → Diagnosis → ICD_Category
                     → LabTest   (ref ranges on relationship)
                     → Medication (dose/route on relationship)

Layer 2 — Knowledge Enhancement
  ICD_Category hierarchy  (standardized disease classification)
  Lab reference ranges    (clinical context for anomaly detection)
```

### Three-Stage Graph RAG Pipeline

```
Stage 1a  Query Decomposition    Detect compound questions, split into sub-queries
Stage 1b  Cypher Generation      LLM generates Neo4j Cypher from natural language
Stage 2   Graph Retrieval        Execute Cypher, extract subgraph
Stage 3   Answer Generation      Evidence-based response + pyvis reasoning visualization
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Graph Database | Neo4j |
| LLM | OpenAI GPT-3.5-turbo (dev) / GPT-4 (prod) |
| LLM Framework | LangChain |
| Frontend | Streamlit |
| Visualization | pyvis |
| ETL | Python / Pandas |
| Data | MIMIC-IV (demo: 100 patients / full: 364K patients) |

---

## Dataset Scale

| Mode | Nodes | Relationships |
|---|---|---|
| Demo | 4,785 | 103,649 |
| Full | 954,151 | 111,837,073 |

**Full dataset breakdown:**
- Patient: 364,627 · Admission: 546,028 · Diagnosis: 28,581
- Medication: 10,581 · LabTest: 1,650 · ICD_Category: 2,684
- HAS_LAB_RESULT: 84.6M · HAS_PRESCRIPTION: 20.3M · HAS_DIAGNOSIS: 6.36M

---

## Project Structure

```
MedGraphRAG-EHR/
├── etl/                        # ETL pipeline
│   ├── config.py               # Centralized config (MODE = "demo"/"full")
│   ├── eda.py                  # Exploratory data analysis (EDA-first methodology)
│   ├── extract.py              # Raw data extraction
│   ├── transform.py            # EDA-driven cleaning & transformation rules
│   ├── build_graph_input.py    # Generate Neo4j-ready CSV files
│   ├── run_etl.py              # End-to-end ETL runner
│   ├── lineage_decorator.py    # capture_lineage decorator for provenance tracking
│   └── quality_check.py        # 47 data quality assertions
├── llm_interface/              # Graph RAG pipeline
│   ├── config.py               # LLM_MODE switch (dev/prod)
│   ├── graph_rag.py            # Core 3-stage pipeline + compound decomposition
│   ├── app.py                  # Streamlit frontend + pyvis visualization
│   ├── validate_results.py     # 3-layer validation framework
│   ├── test_scenarios.py       # 6 query type test suite
│   ├── check_schema.py         # Neo4j schema inspection utility
│   └── test_connection.py      # Neo4j connection check
├── lineage/
│   └── lineage.json            # ETL transformation provenance records
├── data/
│   ├── raw/                    # MIMIC-IV demo source files
│   ├── raw_full/               # MIMIC-IV full source files
│   ├── cleaned/                # Demo post-ETL cleaned data
│   ├── cleaned_full/           # Full post-ETL cleaned data
│   ├── graph_input/            # Demo Neo4j-ready CSV files
│   └── graph_input_full/       # Full Neo4j-ready CSV files
└── .env                        # Neo4j + OpenAI credentials (not committed)
```

---

## Quick Start

### Prerequisites
- Python 3.9+
- Neo4j Desktop (local) or Neo4j AuraDB
- OpenAI API key
- MIMIC-IV dataset ([access required](https://physionet.org/content/mimic-iv/))

### Setup

```bash
git clone https://github.com/maminglei7-lab/MedGraphRAG-EHR.git
cd MedGraphRAG-EHR
pip install -r requirements.txt
```

Create `.env` in project root:
```
NEO4J_URI=bolt://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
OPENAI_API_KEY=your_openai_key
```

### Run ETL Pipeline

```bash
# Set MODE = "demo" or "full" in etl/config.py
python etl/extract.py
python etl/clean.py
python etl/build_graph_input.py
python etl/load_graph.py
```

### Launch Application

```bash
cd llm_interface
streamlit run app.py
```

### Run Validation

```bash
cd llm_interface
python validate_results.py
```

---

## Validation Framework

A three-layer validation framework ensures answer correctness beyond "the pipeline runs":

| Layer | Method | Status |
|---|---|---|
| ETL Quality Checks | 47 assertions on cleaned data | 47/47 ✅ |
| Ground-Truth Cypher | 6 human-approved GT queries | All approved ✅ |
| Automated Scoring | Exact Match + Recall/Precision + Semantic Judge (GPT-3.5) | See below |

**Automated validation results:**

| Query Type | Exact Match | Recall/Precision | Semantic Score |
|---|---|---|---|
| GT1 Single-hop fact | ✅ | 100% | 100 |
| GT2 Anomaly detection | ✅ | 100% | 95 |
| GT3 Multi-hop relational | ⏭* | 100%* | 70 |
| GT4 Backward tracing | ✅ | 100% | 70↑ |
| GT5 Cross-category aggregation | ✅ | 100% | 95 |
| GT6 Comprehensive | ⏭** | N/A** | 95 |

*GT3 uses Precision (LLM LIMIT 20 by design); GT3 semantic score reflects a known limitation: LIMIT truncation does not guarantee clinical relevance ordering.  
**GT6 is a compound query; Exact/Recall not applicable, semantic evaluation only.

---

## Key Design Decisions

**EDA-first methodology** — All cleaning rules derived from actual data inspection, not assumptions. Discovered 107,727 lab events with null `hadm_id`; 116 duplicate diagnosis rows; empty string vs null distinctions in lab units.

**Graph traversal order** — With 84.6M `HAS_LAB_RESULT` relationships, Cypher queries must start from the smallest node set. Rule enforced in LLM prompt to prevent Java heap OOM.

**Two layers vs paper's three** — The paper's three layers address unstructured text (document → literature → UMLS). Our structured EHR data eliminates the need for LLM-based entity extraction; ICD hierarchy + lab reference ranges serve the same knowledge-enrichment purpose in two layers.

**Full-chain traceability** — The paper traces Answer → Graph → Literature/UMLS. We extend this to include ETL provenance: Answer → Graph → Raw MIMIC-IV, tracked via a `capture_lineage` decorator on all transformation functions.

---

## Inspiration

This project is inspired by and adapts methodology from:

> **MedGraphRAG: A Medical Graph RAG System**  
> Ke, et al. ACL 2025 · [arXiv:2408.04187](https://arxiv.org/abs/2408.04187)

Key adaptations for structured EHR data:
- Two-layer KG replacing three-layer document graph
- LLM-driven Cypher planning replacing tag-based U-Retrieval
- ETL lineage traceability as second dimension of full-chain traceability
