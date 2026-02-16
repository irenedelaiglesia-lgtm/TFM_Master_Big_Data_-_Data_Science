import streamlit as st
import pandas as pd
import joblib
import os
import plotly.express as px

# 1. Configuración
st.set_page_config(page_title="AD-Target Intelligence", page_icon="🧠", layout="wide")

st.title("🧠 AD-Target Intelligence System")
st.subheader("Plataforma de Priorización Estratégica para Alzheimer")

# 2. Carga de activos (Asegúrate de que las rutas sean correctas)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

@st.cache_resource
def load_assets():
    model = joblib.load(os.path.join(ROOT_DIR, "models", "alzheimer_tier_predictor.pkl"))
    scaler = joblib.load(os.path.join(ROOT_DIR, "models", "scaler_tier.pkl"))
    df = pd.read_csv(os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_enriquecido_v2.csv"))
    joyas = pd.read_csv(os.path.join(ROOT_DIR, "reports", "dianas_ocultas.csv"))
    return model, scaler, df, joyas

model, scaler, df_data, df_joyas = load_assets()

# --- DICCIONARIO DE SIGNIFICADOS ---
TIER_DESCRIPTIONS = {
    1: "**Prioridad Alta (Validada):** Dianas con éxito clínico y química robusta. Bajo riesgo de fracaso traslacional.",
    2: "**Prioridad Media (Prometedora):** Dianas con evidencia química inicial o fases clínicas tempranas.",
    3: "**Prioridad de Investigación (Emergente):** Dianas sin fármacos actuales. Aquí es donde buscamos nuevas oportunidades biológicas."
}

# 3. Sidebar - Simulador con Tooltips (help)
st.sidebar.header("🕹️ Simulador de Potencial IA")
st.sidebar.write("Modifica los niveles de evidencia para ver la clasificación:")

s_gen = st.sidebar.slider("Asociación Genética", 0.0, 1.0, 0.5, help="Fuerza del vínculo causal entre el gen y el Alzheimer según estudios GWAS.")
s_lit = st.sidebar.slider("Respaldo Literatura", 0.0, 1.0, 0.5, help="Volumen de menciones en publicaciones científicas y PubMed.")
s_rna = st.sidebar.slider("Expresión RNA", 0.0, 1.0, 0.5, help="Nivel de actividad del gen en tejidos cerebrales afectados.")
s_ani = st.sidebar.slider("Modelos Animales", 0.0, 1.0, 0.5, help="Nivel de éxito observado en ensayos con ratones u otros modelos.")
s_path = st.sidebar.slider("Rutas Metabólicas", 0.0, 1.0, 0.5, help="Participación del gen en procesos biológicos clave de la enfermedad.")
s_cons = st.sidebar.slider("Restricción Genética", -2.0, 0.5, -0.5, help="Intolerancia a mutaciones. Valores más negativos indican mayor riesgo de toxicidad.")

if st.sidebar.button("Analizar Perfil"):
    input_data = pd.DataFrame([[s_gen, s_lit, s_rna, s_ani, s_path, s_cons]], 
                              columns=['score_genetic_association', 'score_literature', 'score_rna_expression', 
                                       'score_animal_model', 'score_affected_pathway', 'geneticConstraint'])
    input_scaled = scaler.transform(input_data)
    pred = model.predict(input_scaled)[0]
    probs = model.predict_proba(input_scaled)[0] # Obtenemos probabilidades de cada clase
    prob_t1 = probs[0] # Probabilidad de ser Tier 1 (Líder)

    st.sidebar.divider()
    
    # 1. Mostramos la clasificación por evidencias (lo que es hoy)
    col_map = {1: "🟢 PRIORIDAD ALTA", 2: "🟡 PRIORIDAD MEDIA", 3: "⚪ PRIORIDAD DE INVESTIGACIÓN"}
    st.sidebar.subheader(f"Estado Actual: {col_map[pred]}")
    
    # 2. Mostramos la recomendación de la IA (el futuro)
    if pred == 3 and prob_t1 > 0.40:
        st.sidebar.success(f"⭐ ¡OPORTUNIDAD DETECTADA! La IA detecta un {prob_t1:.1%} de similitud con dianas exitosas.")
    elif pred == 3 and prob_t1 < 0.15:
        st.sidebar.error(f"❌ BAJO POTENCIAL: La IA recomienda descartar esta diana por falta de concordancia biológica.")
    else:
        st.sidebar.info("Perfil alineado con su estado de desarrollo actual.")

# 4. Tabs
tab1, tab2, tab3 = st.tabs(["🔍 Explorador", "💎 Dianas Ocultas", "📈 Análisis Global"])

with tab1:
    gene_choice = st.selectbox("Selecciona un gen:", sorted(df_data['gene_symbol'].unique()))
    gene_info = df_data[df_data['gene_symbol'] == gene_choice].iloc[0]
    
    c1, c2, c3 = st.columns(3)
    # Lógica de nombre descriptivo para el explorador
    if gene_info['max_clinical_phase'] >= 3 or gene_info['max_pchembl'] >= 7.0: lbl = "Prioridad Alta"
    elif (gene_info['max_clinical_phase'] >= 1) or (gene_info['max_pchembl'] >= 4.5) or (gene_info['n_drugs'] > 0): lbl = "Prioridad Media"
    else: lbl = "Prioridad de Investigación"
    
    c1.metric("Clasificación", lbl)
    c2.metric("Fase Clínica", f"Fase {int(gene_info['max_clinical_phase'])}")
    c3.metric("Potencia Química", f"{round(gene_info['max_pchembl'], 2)} pChEMBL")

    features = ['score_genetic_association', 'score_literature', 'score_rna_expression', 'score_animal_model', 'score_affected_pathway']
    fig_radar = px.line_polar(r=gene_info[features].values, theta=features, line_close=True, range_r=[0,1])
    fig_radar.update_traces(fill='toself', line_color='#1f77b4')
    st.plotly_chart(fig_radar)

with tab2:
    st.header("💎 Dianas de Alto Potencial")
    st.write("Genes actualmente sin fármacos pero con un perfil biológico de **Prioridad Alta**.")
    # Renombramos columnas visualmente para la tabla
    df_joyas_view = df_joyas[['gene_symbol', 'prob_Tier1', 'geneticConstraint', 'score_genetic_association']].copy()
    df_joyas_view.columns = ['Gen', 'Potencial IA', 'Restricción Genética', 'Asociación Genética']
    
    st.dataframe(df_joyas_view.style.background_gradient(subset=['Potencial IA'], cmap='Greens'), use_container_width=True)

with tab3:
    st.header("📈 Visión General del Pipeline")
    # Gráfico de pie con nombres descriptivos
    df_data['Clasificación'] = df_data.apply(lambda r: "Prioridad Alta (Validada)" if r['max_clinical_phase']>=3 or r['max_pchembl']>=7 else ("Prioridad Media (Prometedora)" if r['max_clinical_phase']>=1 or r['max_pchembl']>=4.5 or r['n_drugs']>0 else "Investigación (Emergente)"), axis=1)
    fig_pie = px.pie(df_data, names='Clasificación', hole=0.4, color_discrete_sequence=['#2ecc71', '#f1c40f', '#95a5a6'])
    st.plotly_chart(fig_pie)

st.caption("TFM: Big Data & Data Science - Universidad Complutense de Madrid - Irene de la Iglesia [Año 2026]")