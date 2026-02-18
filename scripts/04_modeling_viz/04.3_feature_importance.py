import pandas as pd
import joblib
import os
import matplotlib.pyplot as plt
import seaborn as sns

# Rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
MODEL_PATH = os.path.join(ROOT_DIR, "models", "alzheimer_tier_predictor.pkl")
REPORTS_DIR = os.path.join(ROOT_DIR, "reports")

# Cargar modelo
model = joblib.load(MODEL_PATH)

# Definir las features EXACTAMENTE en el mismo orden que el entrenamiento
# (Copiado de tu script 04_entrenamiento_final.py)
feature_names = [
    'score_genetic_association', 
    'score_literature', 
    'score_rna_expression', 
    'geneticConstraint',
    'interaction_gen_lit', 
    'score_animal_model', 
    'score_affected_pathway'
]

# Extraer importancias
importances = model.feature_importances_

# Crear DataFrame para visualizar
df_imp = pd.DataFrame({'Feature': feature_names, 'Importance': importances})
df_imp = df_imp.sort_values(by='Importance', ascending=False)

# Graficar
plt.figure(figsize=(10, 6))
sns.barplot(x='Importance', y='Feature', data=df_imp, palette='viridis')
plt.title('Importancia de Variables en el Modelo de Predicción')
plt.xlabel('Peso Relativo (Importancia)')
plt.tight_layout()

# Guardar
output_path = os.path.join(REPORTS_DIR, "feature_importance.png")
plt.savefig(output_path)
print(f"✅ Gráfico guardado en: {output_path}")
print("\nRANKING DE VARIABLES:")
print(df_imp)