import streamlit as st
from pyvis.network import Network
import streamlit.components.v1 as components

st.set_page_config(page_title="LLM Interface", layout="wide")

# -----------------------------
# 1) Cypher safety check
# -----------------------------
def is_safe_cypher(cypher: str) -> bool:
    bad = ["delete", "detach", "drop", "set", "remove", "call dbms", "apoc.load"]
    c = (cypher or "").lower()
    return not any(x in c for x in bad)

# -----------------------------
# 2) Mock graph data
# -----------------------------
MOCK_NODES = [
    {"id": "labevents.value", "label": "Field: labevents.value", "group": "Field"},
    {"id": "labevents.itemid", "label": "Field: labevents.itemid", "group": "Field"},
    {"id": "risk_score", "label": "Field: patient_risk_score", "group": "Field"},
    {"id": "diag.icd_code", "label": "Field: diagnoses_icd.icd_code", "group": "Field"},
    {"id": "adm.hadm_id", "label": "Field: admissions.hadm_id", "group": "Field"},
    {"id": "t_join", "label": "Transformation: join/admission", "group": "Transformation"},
    {"id": "t_rule", "label": "Transformation: risk rules", "group": "Transformation"},
    {"id": "t_filter", "label": "Transformation: diabetes filter", "group": "Transformation"},
    {"id": "diabetes_fields", "label": "Field group: diabetes_related_fields", "group": "Field"},
]

MOCK_EDGES = [
    ("labevents.value", "t_join", "TRANSFORMED_BY"),
    ("labevents.itemid", "t_join", "TRANSFORMED_BY"),
    ("adm.hadm_id", "t_join", "TRANSFORMED_BY"),
    ("diag.icd_code", "t_join", "TRANSFORMED_BY"),
    ("t_join", "t_rule", "DERIVED_TO"),
    ("t_rule", "risk_score", "DERIVED_TO"),
    ("diag.icd_code", "t_filter", "TRANSFORMED_BY"),
    ("t_filter", "diabetes_fields", "DERIVED_TO"),
]

# -----------------------------
# 3) Demo responses (hardcoded)
# -----------------------------
DEMO = {
    "audit": {
        "question": "Where does patient_risk_score come from?",
        "cypher": """MATCH (f:Field {name:"patient_risk_score"})<-[:DERIVED_TO]-(t:Transformation)<-[:DERIVED_TO|TRANSFORMED_BY*]-(up)
RETURN up,t,f""",
        "nodes": ["risk_score", "t_rule", "t_join", "labevents.value", "labevents.itemid", "adm.hadm_id", "diag.icd_code"],
        "edges": MOCK_EDGES,
        "explain": [
            "patient_risk_score is derived from upstream fields through transformations.",
            "Upstream signals include lab values, admission id, and diagnosis codes.",
            "The final step applies a risk-rule transformation to produce the score."
        ],
        "recommend": [
            "If you change any upstream field, rerun validation for risk_score.",
            "Log transformation version and timestamp for audit trails."
        ]
    },
    "impact": {
        "question": "What breaks if I change labevents?",
        "cypher": """MATCH (t)-[:DERIVED_TO|TRANSFORMED_BY*]->(down)
WHERE exists((:Field {table:"labevents"})-[:TRANSFORMED_BY]->(t))
RETURN DISTINCT down""",
        "nodes": ["labevents.value", "labevents.itemid", "t_join", "t_rule", "risk_score"],
        "edges": MOCK_EDGES,
        "explain": [
            "labevents fields feed into join/admission transformation.",
            "Downstream, patient_risk_score depends on that pipeline.",
            "So changes to labevents may affect risk scoring outputs."
        ],
        "recommend": [
            "Add schema checks (type/unit) for labevents before the pipeline.",
            "Run regression tests and compare score distributions."
        ]
    },
    "discover": {
        "question": "Show me diabetes-related fields",
        "cypher": """MATCH (f:Field) WHERE toLower(f.name) CONTAINS "diabetes"
RETURN f""",
        "nodes": ["diag.icd_code", "t_filter", "diabetes_fields"],
        "edges": MOCK_EDGES,
        "explain": [
            "This query finds fields related to diabetes signals.",
            "Diagnosis codes are filtered to form a diabetes-related field set."
        ],
        "recommend": [
            "Add synonyms (DM, T2D) to improve discovery recall.",
            "Store a tag/property for medical concepts to speed search."
        ]
    }
}

def render_graph(node_ids):
    net = Network(height="520px", width="100%", directed=True)
    net.barnes_hut()

    node_set = set(node_ids)
    for n in MOCK_NODES:
        if n["id"] in node_set:
            net.add_node(n["id"], label=n["label"], group=n["group"])

    for s, t, r in MOCK_EDGES:
        if s in node_set and t in node_set:
            net.add_edge(s, t, label=r)

    html = net.generate_html()
    components.html(html, height=560, scrolling=True)

# -----------------------------
# UI
# -----------------------------
st.title("LLM Interface")

col_l, col_m, col_r = st.columns([1.2, 2.2, 1.6])

with col_l:
    st.subheader("Ask (NL)")
    user_q = st.text_area("Your question", height=120, placeholder="Type a question…")
    b1 = st.button("Audit Trail")
    b2 = st.button("Impact Analysis")
    b3 = st.button("Data Discovery")
    ask = st.button("Ask")

    mode = None
    if b1: mode = "audit"
    if b2: mode = "impact"
    if b3: mode = "discover"

with col_m:
    st.subheader("Graph View")
    if "current" not in st.session_state:
        st.session_state.current = None

    if mode:
        st.session_state.current = DEMO[mode]

    # basic routing for free text (optional)
    if ask and user_q.strip():
        q = user_q.lower()
        if "risk" in q or "come from" in q or "from" in q:
            st.session_state.current = DEMO["audit"]
        elif "break" in q or "impact" in q or "change" in q:
            st.session_state.current = DEMO["impact"]
        elif "diabetes" in q or "discover" in q or "related" in q:
            st.session_state.current = DEMO["discover"]
        else:
            st.session_state.current = {
                "question": user_q.strip(),
                "cypher": "/* mock: no matching demo */",
                "nodes": [],
                "edges": [],
                "explain": ["No matching demo. Try: risk_score / change labevents / diabetes fields."],
                "recommend": []
            }

    cur = st.session_state.current
    if cur and cur["nodes"]:
        render_graph(cur["nodes"])
    elif cur:
        st.info("No graph to show for this question.")
    else:
        st.info("Click a demo button to start.")

with col_r:
    st.subheader("Generated Cypher + Explanation")
    if cur:
        st.markdown("**Question**")
        st.write(cur["question"])

        st.markdown("**Cypher (shown for demo)**")
        cypher = cur["cypher"]
        st.code(cypher, language="sql")

        if not is_safe_cypher(cypher):
            st.error("Unsafe Cypher detected (blocked).")

        st.markdown("**Explanation**")
        for p in cur["explain"]:
            st.write(f"- {p}")

        if cur["recommend"]:
            st.markdown("**Recommendations**")
            for r in cur["recommend"]:
                st.write(f"- {r}")