import streamlit as st
import pandas as pd
import joblib
import os
import plotly.express as px
import plotly.graph_objects as go

# 1. Configuración Visual
st.set_page_config(
    page_title="AD-Target Intelligence",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS
st.markdown("""
    <style>
    .main {background-color: #f8f9fa;}
    .stMetric {background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 2px 2px 10px rgba(0,0,0,0.05);}
    h1 {color: #2c3e50;}
    .stTooltipIcon {color: #2980b9;}
    </style>
    """, unsafe_allow_html=True)

# --- DICCIONARIO DE TRADUCCIONES ---
TRANSLATIONS = {
    "es": {
        "title": "🧠 AD-Target Intelligence System",
        "subtitle": "**Plataforma de Priorización Estratégica para Alzheimer basada en IA**",
        "sidebar_title": "🕹️ Simulador de Potencial",
        "sidebar_info": "Ajusta los parámetros biológicos para predecir el potencial clínico.",
        "lang_sel": "Idioma / Language",
        # Sliders Labels
        "lbl_gen": "Asociación Genética",
        "lbl_lit": "Respaldo Literatura",
        "lbl_rna": "Expresión RNA",
        "lbl_ani": "Modelos Animales",
        "lbl_path": "Rutas Metabólicas",
        "lbl_cons": "Restricción Genética (Toxicidad)",
        # Tooltips
        "help_gen": "Fuerza de la evidencia genética (Open Targets). 1.0 indica una asociación causal directa muy fuerte.",
        "help_lit": "Nivel de menciones en publicaciones científicas. Indica el consenso académico sobre la diana.",
        "help_rna": "Nivel de expresión del gen en tejidos cerebrales relevantes para el Alzheimer.",
        "help_ani": "Éxito reportado en pruebas con ratones u otros modelos animales.",
        "help_path": "Relevancia de la ruta biológica (Pathway) en la que participa el gen.",
        "help_cons": "gnomAD Score. Valores NEGATIVOS indican alta restricción (el gen no tolera mutaciones), lo que sugiere riesgo de efectos secundarios.",
        "btn_calc": "🔍 Analizar Viabilidad",
        # Resultados Simulador
        "res_prob": "Probabilidad de Éxito",
        "res_high": "🚀 ALTO POTENCIAL: Perfil biológico robusto.",
        "res_med": "⚠️ ZONA GRIS: Evidencia mixta.",
        "res_low": "❌ DESCARTADO: Perfil débil.",
        # Tabs
        "tab1": "📊 Dashboard General",
        "tab2": "💎 Joyas Ocultas (IA)",
        "tab3": "🧬 Explorador de Genes",
        # Tab 1
        "kpi_total": "Total Dianas Analizadas",
        "kpi_joyas": "Joyas Ocultas Identificadas",
        "kpi_delta": "Candidatos Nuevos",
        "kpi_clinic": "Dianas en Clínica (Hoy)",
        "map_title": "Mapa del Conocimiento Actual",
        "pie_title": "Estado de Desarrollo de las 1.000 Dianas",
        "pie_labels": {
            "F3": "Fase 3/4 (Consolidada)",
            "F1": "Fase 1/2 (En Desarrollo)",
            "PC": "Preclínica (Con Química)",
            "IB": "Investigación Básica (Sin Fármacos)"
        },
        # Tab 2
        "joyas_title": "💎 Predicciones de la IA: El 'Top 38'",
        "joyas_desc": "Genes sin fármacos actuales que el modelo identifica como de **Alto Potencial**.",
        "cols_joyas": ['Gen', 'Nombre', 'Probabilidad IA', 'Genética', 'Literatura'],
        "btn_csv": "📥 Descargar Reporte CSV",
        # Tab 3
        "sel_gen": "Busca un Gen:",
        "current_status": "📋 Estado Actual",
        "pred_analysis": "🤖 Análisis Predictivo (IA)",
        "gauge_title": "Probabilidad de Éxito",
        "rec_high": "✅ **Recomendación:** Este gen es un candidato sólido para investigación prioritaria.",
        "rec_med": "⚠️ **Recomendación:** Evidencia mixta. Profundizar en estudios antes de invertir.",
        "rec_low": "⛔ **Recomendación:** Bajo potencial traslacional según patrones históricos.",
        "radar_lbl": ["Genética", "Literatura", "RNA", "Animales", "Rutas"],
        "metric_phase": "Fase Clínica",
        "metric_drug": "Fármacos",
        "metric_pot": "Potencia (pChEMBL)"
    },
    "en": {
        "title": "🧠 AD-Target Intelligence System",
        "subtitle": "**AI-Driven Strategic Prioritization Platform for Alzheimer's**",
        "sidebar_title": "🕹️ Potential Simulator",
        "sidebar_info": "Adjust biological parameters to predict clinical potential.",
        "lang_sel": "Language / Idioma",
        # Sliders Labels
        "lbl_gen": "Genetic Association",
        "lbl_lit": "Literature Support",
        "lbl_rna": "RNA Expression",
        "lbl_ani": "Animal Models",
        "lbl_path": "Metabolic Pathways",
        "lbl_cons": "Genetic Constraint (Toxicity)",
        # Tooltips
        "help_gen": "Strength of genetic evidence (Open Targets). 1.0 indicates a very strong direct causal association.",
        "help_lit": "Level of mentions in scientific publications. Indicates academic consensus on the target.",
        "help_rna": "Level of gene expression in brain tissues relevant to Alzheimer's.",
        "help_ani": "Reported success in tests with mice or other animal models.",
        "help_path": "Relevance of the biological pathway in which the gene participates.",
        "help_cons": "gnomAD Score. NEGATIVE values indicate high constraint (gene does not tolerate mutations), suggesting risk of side effects.",
        "btn_calc": "🔍 Analyze Viability",
        # Resultados Simulador
        "res_prob": "Success Probability",
        "res_high": "🚀 HIGH POTENTIAL: Robust biological profile.",
        "res_med": "⚠️ GRAY ZONE: Mixed evidence.",
        "res_low": "❌ DISCARDED: Weak profile.",
        # Tabs
        "tab1": "📊 General Dashboard",
        "tab2": "💎 Hidden Gems (AI)",
        "tab3": "🧬 Gene Explorer",
        # Tab 1
        "kpi_total": "Total Targets Analyzed",
        "kpi_joyas": "Hidden Gems Identified",
        "kpi_delta": "New Candidates",
        "kpi_clinic": "Targets in Clinic (Today)",
        "map_title": "Current Knowledge Map",
        "pie_title": "Development Status of 1,000 Targets",
        "pie_labels": {
            "F3": "Phase 3/4 (Consolidated)",
            "F1": "Phase 1/2 (Developing)",
            "PC": "Preclinical (With Chemistry)",
            "IB": "Basic Research (No Drugs)"
        },
        # Tab 2
        "joyas_title": "💎 AI Predictions: The 'Top 38'",
        "joyas_desc": "Genes currently without drugs that the model identifies as **High Potential**.",
        "cols_joyas": ['Gene', 'Name', 'AI Probability', 'Genetics', 'Literature'],
        "btn_csv": "📥 Download CSV Report",
        # Tab 3
        "sel_gen": "Search for a Gene:",
        "current_status": "📋 Current Status",
        "pred_analysis": "🤖 Predictive Analysis (AI)",
        "gauge_title": "Success Probability",
        "rec_high": "✅ **Recommendation:** Solid candidate for priority research.",
        "rec_med": "⚠️ **Recommendation:** Mixed evidence. Further study required.",
        "rec_low": "⛔ **Recommendation:** Low translational potential based on historical patterns.",
        "radar_lbl": ["Genetics", "Literature", "RNA", "Animals", "Pathways"],
        "metric_phase": "Clinical Phase",
        "metric_drug": "Drugs",
        "metric_pot": "Potency (pChEMBL)"
    }
}

# 2. Carga de Activos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

@st.cache_resource
def load_assets():
    model_path = os.path.join(ROOT_DIR, "models", "alzheimer_tier_predictor.pkl")
    scaler_path = os.path.join(ROOT_DIR, "models", "scaler_tier.pkl")
    data_path = os.path.join(ROOT_DIR, "data", "processed", "dataset_maestro_enriquecido_v2.csv")
    joyas_path = os.path.join(ROOT_DIR, "reports", "dianas_ocultas.csv")
    
    model = joblib.load(model_path)
    scaler = joblib.load(scaler_path)
    df = pd.read_csv(data_path)
    joyas = pd.read_csv(joyas_path)
    return model, scaler, df, joyas

try:
    model, scaler, df_data, df_joyas = load_assets()
except Exception as e:
    st.error(f"Error: {e}")
    st.stop()

# --- SELECTOR DE IDIOMA ---
lang_option = st.sidebar.selectbox(TRANSLATIONS['es']['lang_sel'], ['Español', 'English'])
lang = 'es' if lang_option == 'Español' else 'en'
t = TRANSLATIONS[lang] # Atajo para acceder a los textos

# Renderizar Título
st.title(t['title'])
st.markdown(t['subtitle'])

# --- SIDEBAR: SIMULADOR IA ---
st.sidebar.header(t['sidebar_title'])
st.sidebar.info(t['sidebar_info'])

with st.sidebar.form("simulador_form"):
    s_gen = st.slider(t['lbl_gen'], 0.0, 1.0, 0.5, help=t['help_gen'])
    s_lit = st.slider(t['lbl_lit'], 0.0, 1.0, 0.5, help=t['help_lit'])
    s_rna = st.slider(t['lbl_rna'], 0.0, 1.0, 0.5, help=t['help_rna'])
    s_ani = st.slider(t['lbl_ani'], 0.0, 1.0, 0.0, help=t['help_ani'])
    s_path = st.slider(t['lbl_path'], 0.0, 1.0, 0.0, help=t['help_path'])
    s_cons = st.slider(t['lbl_cons'], -2.0, 1.0, 0.0, help=t['help_cons'])
    
    submit_val = st.form_submit_button(t['btn_calc'])

if submit_val:
    interaccion = s_gen * s_lit 
    input_df = pd.DataFrame([[s_gen, s_lit, s_rna, s_cons, interaccion, s_ani, s_path]], 
                            columns=['score_genetic_association', 'score_literature', 'score_rna_expression', 'geneticConstraint',
                                     'interaction_gen_lit', 'score_animal_model', 'score_affected_pathway'])
    
    input_scaled = scaler.transform(input_df)
    prob_exito = model.predict_proba(input_scaled)[0][1] 
    
    st.sidebar.divider()
    st.sidebar.metric(t['res_prob'], f"{prob_exito:.1%}")
    
    if prob_exito > 0.65:
        st.sidebar.success(t['res_high'])
    elif prob_exito > 0.40:
        st.sidebar.warning(t['res_med'])
    else:
        st.sidebar.error(t['res_low'])

# --- CUERPO PRINCIPAL (TABS) ---
tab1, tab2, tab3 = st.tabs([t['tab1'], t['tab2'], t['tab3']])

# TAB 1: VISIÓN GENERAL
with tab1:
    col1, col2, col3 = st.columns(3)
    col1.metric(t['kpi_total'], len(df_data))
    col2.metric(t['kpi_joyas'], len(df_joyas), delta=t['kpi_delta'])
    exito_real = len(df_data[df_data['max_clinical_phase'] > 0])
    col3.metric(t['kpi_clinic'], exito_real)

    st.markdown(f"### {t['map_title']}")
    
    def clasificar(r):
        if r['max_clinical_phase'] >= 3: return t['pie_labels']['F3']
        elif r['max_clinical_phase'] >= 1: return t['pie_labels']['F1']
        elif r['max_pchembl'] >= 5: return t['pie_labels']['PC']
        else: return t['pie_labels']['IB']
    
    df_data['Estado_View'] = df_data.apply(clasificar, axis=1)
    
    fig_pie = px.pie(df_data, names='Estado_View', hole=0.4, 
                     color_discrete_sequence=px.colors.sequential.Teal,
                     title=t['pie_title'])
    st.plotly_chart(fig_pie, use_container_width=True)

# TAB 2: JOYAS OCULTAS
with tab2:
    st.header(t['joyas_title'])
    st.markdown(t['joyas_desc'])
    
    df_show = df_joyas[['gene_symbol', 'gene_name', 'prob_exito_IA', 'score_genetic_association', 'score_literature']].copy()
    df_show.columns = t['cols_joyas']
    
    st.dataframe(
        df_show.style.background_gradient(subset=[t['cols_joyas'][2]], cmap='Greens'),
        use_container_width=True,
        height=600
    )
    
    csv = df_show.to_csv(index=False).encode('utf-8')
    st.download_button(t['btn_csv'], csv, "joyas_ocultas_alzheimer.csv", "text/csv")

# TAB 3: EXPLORADOR INDIVIDUAL
with tab3:
    col_sel, col_empty = st.columns([1, 2])
    with col_sel:
        gen_seleccionado = st.selectbox(t['sel_gen'], sorted(df_data['gene_symbol'].unique()))
    
    row = df_data[df_data['gene_symbol'] == gen_seleccionado].iloc[0]
    
    # Predicción real
    interaccion_gen_lit = row['score_genetic_association'] * row['score_literature']
    input_gen = pd.DataFrame([[row['score_genetic_association'], row['score_literature'], row['score_rna_expression'], 
                               row['geneticConstraint'], interaccion_gen_lit, row['score_animal_model'], row['score_affected_pathway']]], 
                            columns=['score_genetic_association', 'score_literature', 'score_rna_expression', 'geneticConstraint',
                                     'interaction_gen_lit', 'score_animal_model', 'score_affected_pathway'])
    
    scaled_gen = scaler.transform(input_gen)
    prob_gen = model.predict_proba(scaled_gen)[0][1] 
    
    st.divider()
    
    c1, c2 = st.columns([1, 1])
    
    with c1:
        st.subheader(t['current_status'])
        st.write(f"**Gen:** {row['gene_name']}")
        st.write(f"**{t['metric_phase']}:** {int(row['max_clinical_phase'])}")
        st.write(f"**{t['metric_drug']}:** {int(row['n_drugs'])}")
        st.metric(t['metric_pot'], round(row['max_pchembl'], 2))
        
        # Radar
        categories = t['radar_lbl']
        values = [row['score_genetic_association'], row['score_literature'], row['score_rna_expression'], row['score_animal_model'], row['score_affected_pathway']]
        fig = go.Figure(data=go.Scatterpolar(r=values, theta=categories, fill='toself', name=row['gene_symbol']))
        fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 1])), showlegend=False, margin=dict(t=20, b=20, l=40, r=40))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader(t['pred_analysis'])
        
        fig_gauge = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = prob_gen * 100,
            title = {'text': t['gauge_title']},
            gauge = {
                'axis': {'range': [0, 100]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 40], 'color': "#ffcccc"},
                    {'range': [40, 65], 'color': "#ffeebb"},
                    {'range': [65, 100], 'color': "#ccffcc"}
                ],
                'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 65}
            }
        ))
        fig_gauge.update_layout(height=300, margin=dict(t=40, b=20, l=20, r=20))
        st.plotly_chart(fig_gauge, use_container_width=True)
        
        if prob_gen > 0.65: st.success(t['rec_high'])
        elif prob_gen > 0.40: st.warning(t['rec_med'])
        else: st.error(t['rec_low'])

st.markdown("---")

st.caption("TFM Data Science, Big data & Business Analytics | Universidad Complutense de Madrid | Irene de la Iglesia | 2026")