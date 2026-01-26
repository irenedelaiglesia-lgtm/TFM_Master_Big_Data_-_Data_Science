import requests
import pandas as pd
import time

class AlzheimerDataEngine:
    """
    Enfoque: Relación entre soporte genético, evidencia clínica y desarrollo farmacológico en Alzheimer.
    """

    def __init__(self):
        self.api_url = "https://api.platform.opentargets.org/api/v4/graphql"
        self.disease_id = "MONDO_0004975"  # Alzheimer
        self.headers = {"Content-Type": "application/json"}

    def execute_query(self, query, variables=None):
        for attempt in range(3):
            try:
                response = requests.post(
                    self.api_url,
                    json={"query": query, "variables": variables},
                    headers=self.headers,
                    timeout=30
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                print(f"⚠️ Error intento {attempt + 1}: {e}")
                time.sleep(1)
        return None

    def get_top_targets(self, limit=50):
        """
        BLOQUE 1: Disease-centric target discovery
        """
        query = """
        query AssociatedTargets($diseaseId: String!, $limit: Int!) {
          disease(efoId: $diseaseId) {
            name
            associatedTargets(page: { index: 0, size: $limit }) {
              rows {
                target {
                  id
                  approvedSymbol
                  approvedName
                  targetClass {
                    label
                  }
                }
                score
                datasourceScores {
                  id
                  score
                }
              }
            }
          }
        }
        """

        variables = {
            "diseaseId": self.disease_id,
            "limit": limit
        }

        result = self.execute_query(query, variables)

        if not result or not result.get("data", {}).get("disease"):
            raise RuntimeError("❌ Disease not found or API error")

        rows = result["data"]["disease"]["associatedTargets"]["rows"]
        processed = []

        for r in rows:
            ds_scores = {ds["id"]: ds["score"] for ds in r["datasourceScores"]}

            processed.append({
                "target_id": r["target"]["id"],
                "symbol": r["target"]["approvedSymbol"],
                "name": r["target"]["approvedName"],
                "target_class": (
                    r["target"]["targetClass"][0]["label"]
                    if r["target"]["targetClass"] else "Unknown"
                ),
                "overall_score": round(r["score"], 3),
                "genetic_score": round(
                    max(
                        ds_scores.get("ot_genetics_portal", 0),
                        ds_scores.get("gene_burden", 0)
                    ),
                    3
                ),
                "clinical_score": round(ds_scores.get("known_drug", 0), 3),
                "literature_score": round(ds_scores.get("europepmc", 0), 3)
            })

        return pd.DataFrame(processed)

    def classify_target(self, row):
        """
        BLOQUE 2: Scientific interpretation
        """
        if row["genetic_score"] > 0.3:
            return "🟢 High Genetic Support (Emerging)"

        elif row["clinical_score"] > 0.4 and row["genetic_score"] < 0.1:
            return "🔴 Clinically Exploited (Low Genetics)"

        elif row["literature_score"] > 0.4:
            return "🟡 Literature-Driven Target"

        return "⚪ Under Investigation"

    def get_target_drugs(self, target_id):
        """
        BLOQUE 3: Drug development landscape
        """
        query = """
        query TargetDrugs($targetId: String!) {
          target(ensemblId: $targetId) {
            knownDrugs {
              rows {
                drug {
                  name
                  drugType
                  maximumClinicalTrialPhase
                }
                phase
                status
              }
            }
          }
        }
        """

        result = self.execute_query(query, {"targetId": target_id})

        if not result or not result.get("data", {}).get("target"):
            return []

        return result["data"]["target"]["knownDrugs"]["rows"]

    def run_pipeline(self):
        print("🚀 Alzheimer Target Discovery Pipeline")

        targets_df = self.get_top_targets(50)
        targets_df["category"] = targets_df.apply(self.classify_target, axis=1)

        print(f"📊 Targets retrieved: {len(targets_df)}")

        drug_records = []
        for _, row in targets_df.iterrows():
            drugs = self.get_target_drugs(row["target_id"])
            for d in drugs:
                drug_records.append({
                    "target_symbol": row["symbol"],
                    "target_category": row["category"],
                    "drug_name": d["drug"]["name"],
                    "drug_type": d["drug"]["drugType"],
                    "trial_phase": d["phase"],
                    "trial_status": d["status"] or "Unknown"
                })

        drugs_df = pd.DataFrame(drug_records)

        targets_df.to_csv("alzheimer_targets.csv", index=False)
        drugs_df.to_csv("alzheimer_drugs.csv", index=False)

        print("✅ Pipeline finalizado correctamente\n")
        print(targets_df["category"].value_counts())

        return targets_df, drugs_df


if __name__ == "__main__":
    engine = AlzheimerDataEngine()
    targets, drugs = engine.run_pipeline()
