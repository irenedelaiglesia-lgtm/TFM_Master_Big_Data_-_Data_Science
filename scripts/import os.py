import os
import requests
import sqlite3
import pandas as pd

# =========================
# CONFIGURACIÓN
# =========================
OPENTARGETS_API_URL = "https://api.platform.opentargets.org/api/v4/graphql"
DISEASE_EFO_ID = "MONDO_0004975" 
TOP_N_TARGETS = 1000

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

DB_PATH = os.path.join(ROOT_DIR, "tablas", "alzheimer_targets.db")
OUTPUT_PATH = os.path.join(ROOT_DIR, "data", "processed", "alzheimer_top_targets.csv")

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# =========================
# QUERY GRAPHQL MEJORADA
# =========================
# Aquí incluimos 'knownDrugs' para obtener las fases clínicas reales
QUERY = """
query AlzheimerTargets($efoId: String!, $size: Int!) {
  disease(efoId: $efoId) {
    associatedTargets(page: { index: 0, size: $size }) {
      rows {
        target {
          id
          approvedSymbol
          approvedName
          # Nueva forma de pedir el UniProt ID correctamente
          proteinIds { id source }
          # Pedimos los fármacos conocidos para este target
          knownDrugs {
            uniqueDrugs
            rows {
              phase
            }
          }
        }
        score
        datatypeScores {
          id
          score
        }
      }
    }
  }
}
"""

def extract_alzheimer_targets(limit: int = 1000):
    variables = {"efoId": DISEASE_EFO_ID, "size": limit}
    response = requests.post(OPENTARGETS_API_URL, json={"query": QUERY, "variables": variables})
    response.raise_for_status()
    
    data = response.json()["data"]["disease"]["associatedTargets"]["rows"]
    records = []

    for row in data:
        target = row["target"]
        
        # 1. Extraer UniProt (buscando específicamente la fuente 'uniprot_swissprot')
        u_ids = [p['id'] for p in target.get('proteinIds', []) if 'uniprot' in p['source']]
        primary_uniprot = u_ids[0] if u_ids else "N/A"

        # 2. Extraer Fase Clínica Máxima de los fármacos
        drugs = target.get("knownDrugs", {})
        drug_rows = drugs.get("rows", [])
        max_phase = max([d['phase'] for d in drug_rows]) if drug_rows else 0

        record = {
            "target_id": target["id"],
            "uniprot_id": primary_uniprot,
            "gene_symbol": target["approvedSymbol"],
            "gene_name": target["approvedName"],
            "score_overall": row["score"],
            "max_clinical_phase": max_phase,  # <--- ¡AQUÍ ESTÁ LA FASE!
            "n_drugs": drugs.get("uniqueDrugs", 0)
        }

        # 3. Añadir scores por tipo de evidencia (Genética, RNA, etc.)
        for ds in row.get("datatypeScores", []):
            record[f"score_{ds['id']}"] = ds["score"]

        records.append(record)

    return pd.DataFrame(records)

if __name__ == "__main__":
    print(f"🚀 Extrayendo {TOP_N_TARGETS} targets y sus fases clínicas...")
    df_targets = extract_alzheimer_targets(TOP_N_TARGETS)
    
    # Guardar en SQLite y CSV
    conn = sqlite3.connect(DB_PATH)
    df_targets.to_sql("alzheimer_top_targets", conn, if_exists="replace", index=False)
    conn.close()
    
    df_targets.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Extracción completada. {len(df_targets)} registros guardados en {OUTPUT_PATH}")