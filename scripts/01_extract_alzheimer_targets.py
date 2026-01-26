import requests
import pandas as pd
import sqlite3
import os

# ---- CONFIG ----
API_URL = "https://api.platform.opentargets.org/api/v4/graphql"
DISEASE_ID = "MONDO_0004975"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_CSV = os.path.join(BASE_DIR, "..", "data", "processed", "alzheimer_top_targets.csv")
OUTPUT_DB = os.path.join(BASE_DIR, "..", "tablas", "alzheimer_targets.db")

# ---- FUNCIONES ----
def run_query(query, variables):
    response = requests.post(
        API_URL,
        json={"query": query, "variables": variables},
        timeout=30
    )
    if response.status_code != 200:
        print("❌ GraphQL error:")
        print(response.text)
        response.raise_for_status()
    return response.json()

def extract_top_targets(limit=100):
    query = """
    query getAssociatedTargets($diseaseId: String!, $size: Int!) {
      disease(efoId: $diseaseId) {
        id
        name
        associatedTargets(page: { index: 0, size: $size }) {
          rows {
            score
            target {
              id
              approvedSymbol
              approvedName
            }
            datatypeScores {
              id
              score
            }
          }
        }
      }
    }
    """
    variables = {"diseaseId": DISEASE_ID, "size": limit}
    data = run_query(query, variables)

    if not data["data"]["disease"]:
        raise RuntimeError("❌ Disease not found")

    rows = data["data"]["disease"]["associatedTargets"]["rows"]
    processed = []

    for r in rows:
        score_map = {d["id"]: d["score"] for d in r["datatypeScores"]}

        processed.append({
            "ensembl_id": r["target"]["id"],
            "gene_symbol": r["target"]["approvedSymbol"],
            "gene_name": r["target"]["approvedName"],
            "score_overall": round(r["score"], 4),
            "score_genetic": round(score_map.get("genetic_association", 0), 4),
            "score_clinical": round(score_map.get("known_drug", 0), 4),
            "score_literature": round(score_map.get("literature", 0), 4),  # <-- CORRECCIÓN
            "raw_datatype_scores": str(score_map)
        })

    for r in rows[:5]:
        print(r["target"]["approvedSymbol"], {d["id"]: d["score"] for d in r["datatypeScores"]})


    return pd.DataFrame(processed)

def save_to_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"✅ CSV guardado en: {path}")

def save_to_sqlite(df, db_path, table_name="alzheimer_targets"):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"✅ SQLite DB creada en: {db_path}, tabla: {table_name}")

# ---- MAIN ----
if __name__ == "__main__":
    print("🚀 Extrayendo TOP targets asociados a Alzheimer...")
    df = extract_top_targets(limit=100)
    df = df.sort_values("score_overall", ascending=False)

    save_to_csv(df, OUTPUT_CSV)
    save_to_sqlite(df, OUTPUT_DB)

    print(f"\n✅ {len(df)} dianas extraídas correctamente")
    print(df.head(10))
