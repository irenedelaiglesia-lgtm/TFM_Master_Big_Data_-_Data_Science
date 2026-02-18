import os
import requests
import sqlite3
import pandas as pd
import time

# =========================
# CONFIGURACIÓN
# =========================

OPENTARGETS_API_URL = "https://api.platform.opentargets.org/api/v4/graphql"
DISEASE_EFO_ID = "MONDO_0004975" 
TOP_N_TARGETS = 1000

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

DB_DIR = os.path.join(ROOT_DIR, "tablas")
DB_PATH = os.path.join(DB_DIR, "alzheimer_targets.db")
TABLE_NAME = "alzheimer_top_targets"

OUTPUT_DIR = os.path.join(ROOT_DIR, "data", "processed")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "alzheimer_top_targets.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DB_DIR, exist_ok=True)

# =========================
# QUERIES GRAPHQL (Corregidas)
# =========================

# Añadimos proteinIds a la consulta principal
QUERY_TARGETS = """
query AlzheimerTargets($efoId: String!, $size: Int!) {
  disease(efoId: $efoId) {
    associatedTargets(page: { index: 0, size: $size }) {
      rows {
        target {
          id
          approvedSymbol
          approvedName
          proteinIds { id source }
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

QUERY_DRUGS = """
query TargetDrugs($ensgId: String!) {
  target(ensemblId: $ensgId) {
    knownDrugs {
      uniqueDrugs
      rows {
        phase
      }
    }
  }
}
"""

# =========================
# FUNCIONES DE APOYO
# =========================

def get_drug_info(ensg_id):
    """Consulta la fase clínica y número de fármacos para un target"""
    try:
        response = requests.post(
            OPENTARGETS_API_URL, 
            json={"query": QUERY_DRUGS, "variables": {"ensgId": ensg_id}}
        )
        if response.status_code == 200:
            data = response.json().get("data", {}).get("target", {})
            drugs = data.get("knownDrugs", {})
            rows = drugs.get("rows", [])
            
            max_phase = max([r["phase"] for r in rows]) if rows else 0
            n_drugs = drugs.get("uniqueDrugs", 0)
            return max_phase, n_drugs
    except Exception:
        pass
    return 0, 0

# =========================
# EXTRACCIÓN DE DATOS
# =========================

def extract_alzheimer_targets(limit: int = 1000) -> pd.DataFrame:
    variables = {"efoId": DISEASE_EFO_ID, "size": limit}

    print("📡 Conectando con Open Targets API...")
    response = requests.post(
        OPENTARGETS_API_URL,
        json={"query": QUERY_TARGETS, "variables": variables}
    )
    response.raise_for_status()
    rows = response.json()["data"]["disease"]["associatedTargets"]["rows"]

    records = []
    print(f"🧬 Procesando {len(rows)} targets. Esto puede tardar un poco...")

    for i, row in enumerate(rows):
        target = row["target"]
        ensg_id = target["id"]
        
        # 1. Corregir extracción de UniProt (ahora sí viene en la query)
        u_ids = [p['id'] for p in target.get('proteinIds', []) if 'uniprot' in p['source']]
        primary_uniprot = u_ids[0] if u_ids else None

        # 2. Llamar a la sub-query de fármacos (Lo que faltaba)
        max_phase, n_drugs = get_drug_info(ensg_id)

        record = {
            "target_id": ensg_id,
            "uniprot_id": primary_uniprot,
            "gene_symbol": target["approvedSymbol"],
            "gene_name": target["approvedName"],
            "score_overall": row["score"],
            "max_clinical_phase": max_phase,
            "n_drugs": n_drugs
        }

        # 3. Añadir scores por tipo de evidencia
        for ds in row.get("datatypeScores", []):
            record[f"score_{ds['id']}"] = ds["score"]

        records.append(record)
        
        # Log de progreso cada 100
        if (i + 1) % 100 == 0:
            print(f"⏳ {i + 1} targets procesados...")
        
        # Pequeño delay para no saturar la API
        time.sleep(0.05)

    return pd.DataFrame(records)

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    df_targets = extract_alzheimer_targets(TOP_N_TARGETS)

    # Guardar en SQLite
    conn = sqlite3.connect(DB_PATH)
    df_targets.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
    conn.close()

    # Guardar en CSV
    df_targets.to_csv(OUTPUT_PATH, index=False)
    
    print(f"✅ Proceso finalizado.")
    print(f"📊 Total registros: {len(df_targets)}")
    print(f"💊 Targets con fase clínica > 0: {len(df_targets[df_targets['max_clinical_phase'] > 0])}")
    print(f"📁 Archivo: {OUTPUT_PATH}")

