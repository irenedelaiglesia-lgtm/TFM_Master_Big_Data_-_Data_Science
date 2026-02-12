import streamlit as st
import pandas as pd
import joblib
import os

# 1. Configuración y rutas
st.set_page_config(page_title="Alzheimer Target Predictor", layout="wide")
st.title("🧠 Predicción de Éxito en Dianas para Alzheimer")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

# 2. Carga de modelos y datos
@st.cache_resource
def load_data():
    model_path = os.path.join(ROOT_DIR, "models", "alzheimer_drug_predictor_optimized.pkl")
    scaler_path = os.path.join(ROOT_DIR, "models", "scaler_optimized.pkl")
    data_path = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_final.csv")
    
    model = joblib.load(model_path)
    scaler = joblib.load(scaler_path)
    df = pd.read_csv(data_path)
    return model, scaler, df

model, scaler, df_data = load_data()

# 3. Sidebar - Simulador
st.sidebar.header("🧪 Simulador de Diana Nueva")
v_gen = st.sidebar.slider("Evidencia Genética", 0.0, 1.0, 0.2)
v_lit = st.sidebar.slider("Evidencia en Literatura", 0.0, 1.0, 0.3)
v_rna = st.sidebar.slider("Expresión de RNA", 0.0, 1.0, 0.1)
v_ani = st.sidebar.slider("Modelos Animales", 0.0, 1.0, 0.1)
v_path = st.sidebar.slider("Rutas Metabólicas", 0.0, 1.0, 0.2)
v_cons = st.sidebar.slider("Restricción Genética (TSV)", 0.0, 1.0, 0.5)

if st.sidebar.button("Predecir Probabilidad"):
    # IMPORTANTE: Mismo orden que en el entrenamiento
    input_df = pd.DataFrame([[v_gen, v_lit, v_rna, v_ani, v_path, v_cons]], 
                            columns=['score_genetic_association', 'score_literature', 
                                     'score_rna_expression', 'score_animal_model', 
                                     'score_affected_pathway', 'geneticConstraint'])
    input_scaled = scaler.transform(input_df)
    prob = model.predict_proba(input_scaled)[0][1]
    st.sidebar.metric("Probabilidad de Éxito", f"{prob:.2%}")

# 4. Buscador de Genes
st.header("Explorador de Targets Actuales")
# Buscamos la columna de nombres de genes dinámicamente
possible_names = ['symbol', 'gene_symbol', 'target_symbol', 'targetId', 'target']
col_name = next((c for c in possible_names if c in df_data.columns), None)

if col_name:
    gene_choice = st.selectbox("Selecciona un gen para analizar:", df_data[col_name].unique())
    gene_info = df_data[df_data[col_name] == gene_choice].iloc[0]
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader(f"Análisis del Gen: {gene_choice}")
        st.write(f"**Puntuación Genética:** {round(gene_info.get('score_genetic_association', 0), 3)}")
        st.write(f"**Restricción (TSV):** {round(gene_info.get('geneticConstraint', 0), 3)}")
    with col2:
        # Gráfica de los scores reales que existan en el CSV
        metrics = [c for c in ['score_genetic_association', 'score_literature', 'score_affected_pathway', 'geneticConstraint'] if c in df_data.columns]
        st.bar_chart(gene_info[metrics])
else:
    st.error("No se encontró la columna de símbolos en el CSV.")
    st.write("Columnas disponibles:", df_data.columns.tolist())