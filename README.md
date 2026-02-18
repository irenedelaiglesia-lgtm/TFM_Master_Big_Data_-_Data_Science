# TFM - Data Science, Big data & Business Analytics
## *Desarrollo de un Sistema Big Data para la Priorización de Dianas en Alzheimer: Integración Genómica y Modelado Predictivo*
# 🧠 AD-Target Intelligence System
### Priorización de Dianas Terapéuticas para Alzheimer mediante Machine Learning

**Autor/a:** Irene de la Iglesia del Pino\
**Máster:** Data Science, Big Data y Business Analytics\
**Convocatoria:**  Febrero 2026

---

## 📋 Descripción del Proyecto
Este trabajo presenta un sistema de inteligencia artificial diseñado para identificar nuevas oportunidades terapéuticas (dianas) para la enfermedad de Alzheimer. Utilizando un enfoque *Biology-First*, el modelo entrena un algoritmo **Gradient Boosting** para detectar patrones de éxito en genes que actualmente carecen de fármacos asociados.

El sistema clasifica las dianas mediante un **Modelo Binario de Alto Potencial** (Investigación vs. Éxito) y ofrece una interfaz interactiva (Dashboard) para la exploración y simulación de escenarios biológicos.

---

## ⚙️ Estructura del Repositorio

El proyecto se organiza en la siguiente estructura de directorios:

```text
TFM - Data Science, Big data & Business Analytics/
│
├── 📄 README.md                <-- Manual de ejecución.
├── 📄 requirements.txt         <-- Dependencias.
│
├── 📂 data/
│   ├── 📂 raw/                 <-- Datos crudos 
│   └── 📂 processed/           <-- Dataset maestro (Salida de 03_analytics_gold).
│
├── 📂 models/                  <-- Modelos .pkl (Salida de 04_modeling_viz).
├── 📂 reports/                 <-- Reportes y gráficas.
│
└── 📂 scripts/
    ├── 📂 01_ingestion/        <-- Extracción de APIs (Open Targets).
    ├── 📂 02_transformation/   <-- Limpieza y Calidad del Dato.
    ├── 📂 03_analytics_gold/   <-- Generación del Dataset Maestro.
    └── 📂 04_modeling_viz/     <-- Entrenamiento, Validación y Dashboard.
```

## 🚀 Guía de Instalación 

Esta sección detalla los pasos necesarios para configurar el entorno técnico y ejecutar el sistema de predicción en una máquina local.

### 1. Requisitos del Sistema
El código es compatible con cualquier plataforma que soporte Python, pero se recomienda la siguiente configuración para una experiencia óptima:

- **Sistema Operativo**: Windows, macOS o Linux.

- **Lenguaje**: Python 3.8 o superior.

- **IDE Recomendado**: Visual Studio Code (VSCode) con la extensión de Python instalada.

- **Entorno**: Se recomienda utilizar un entorno virtual (venv o conda) dentro de VSCode para aislar las dependencias.

### 2. Instalación de Dependencias
El proyecto incluye un archivo `requirements.txt` con todas las librerías necesarias (pandas, scikit-learn, streamlit, plotly, etc.).

1. Abra la carpeta del proyecto en VSCode (o su terminal preferida).

2. Asegúrese de que su entorno virtual está activo.

3. Ejecute el siguiente comando para instalar todas las librerías necesarias:


    ```bash
    pip install -r requirements.txt

## 🔄 Reproducción Completa del Pipeline 

El proyecto está diseñado modularmente siguiendo una arquitectura Medallion (Bronze/Silver/Gold). Si desea regenerar el dataset maestro desde el origen, ejecute los scripts en el siguiente orden secuencial.

> **Nota:** Este proceso requiere conexión a internet (para la API de Open Targets) y que el archivo `targets_aditional_info.tsv` se encuentre en la carpeta `data/raw/`.

### 1. Ingesta de Datos (Capa Bronze)
Obtención de datos crudos desde la API y procesamiento de archivos complementarios.

* **1.1. Extracción API:** Descarga las dianas asociadas a Alzheimer y sus metadatos clínicos.
    ```bash
    python scripts/01_ingestion/01.1_extract_alzheimer_targets.py
    ```
* **1.2. Información Adicional:** Procesa datos de restricción genética y expresión (requiere TSV en `data/raw`).
    ```bash
    python scripts/01_ingestion/01.2_extract_aditional_info.py
    ```


### 2. Transformación y Limpieza (Capa Silver)
Limpieza de nulos, normalización de identificadores y filtrado de calidad para evitar ruido en el modelo predictivo.

* **2.1. Limpieza y Preparación:** Estandarización de textos (mayúsculas, borrado de espacios), casting de variables numéricas y creación de flags iniciales.
    ```bash
    python scripts/02_transformation/02.1_clean_and_prepare.py
    ```
* **2.2. Auditoría de Calidad:** Verificación de la integridad del dato y generación del informe de calidad automatizado en la carpeta `reports/`.
    ```bash
    python scripts/02_transformation/02.2_data_quality_check.py
    ```

### 3. Generación del Dataset Maestro (Capa Gold)
Esta etapa consolida las fuentes y enriquece los datos con información farmacológica externa.

* **3.1. Fusión de Fuentes (Merge):** Unifica los datos de la API (Open Targets) con el TSV de información adicional.
    ```bash
    python scripts/03_analytics_gold/03.1_merge_master_dataset.py
    ```
* **3.2. Enriquecimiento Químico (ChEMBL):** Consulta la API de ChEMBL para cada diana y añade las variables `max_pchembl` y `drug_count`.
    > **Nota:** Este script tarda varios minutos ya que realiza peticiones HTTP para cada gen.
    ```bash
    python scripts/03_analytics_gold/03.2_enrich_chembl_data.py
    ```
* **3.3. Auditoría Final:** Verifica la integridad del dataset final y genera un reporte de calidad en la carpeta `reports/`.
    ```bash
    python scripts/03_analytics_gold/03.3_validacion_y_calidad_final.py
    ```
 ### 4. Modelado Predictivo y Minería (Capa Platinum)
Ejecución del sistema de Inteligencia Artificial para la detección de potencial terapéutico y priorización de candidatos.

* **4.1. Entrenamiento del Modelo (Gradient Boosting):**
    Entrena un clasificador binario optimizado para distinguir entre dianas en fase de investigación y dianas con potencial clínico. Realiza balanceo de clases automático y optimización de hiperparámetros.
    * *Output:* `models/alzheimer_tier_predictor.pkl` y métricas de evaluación (Curva ROC, Matrices).
    ```bash
    python scripts/04_modeling_viz/04.1_entrenamiento_final.py
    ```

* **4.2. Inferencia y Descubrimiento (Minería):**
    Carga el modelo entrenado y lo aplica sobre las dianas no exploradas (Clase 0) para identificar "Joyas Ocultas" con alta probabilidad de éxito.
    * *Output:* `reports/dianas_ocultas.csv` (Listado final de candidatos) y `distribucion_joyas_ocultas.png`.
    ```bash
    python scripts/04_modeling_viz/04.2_validacion_realismo.py
    ```

* **4.3. Análisis de Importancia de Variables:**
    Genera un gráfico de barras que clasifica las variables biológicas según su peso en la decisión del modelo.
    * *Output:* `reports/feature_importance.png`.
    ```bash
    python scripts/04_modeling_viz/04.3_feature_importance.py
    ```

### 5. Visualización y Despliegue (Dashboard)
El proyecto incluye una interfaz web interactiva desarrollada en Streamlit. Esta es la herramienta principal para presentar los resultados.

* **5.1. Ejecución del Dashboard:**
    Lanza la aplicación web localmente utilizando Streamlit.
    * *Input:* Requiere que existan los modelos en `models/` y los datos en `data/processed/`.

    ```bash
    streamlit run scripts/04_modeling_viz/05_dashboard.py
    ```
    * *Funcionalidades del Dashboard:*

        1. **Simulador de Potencial:** Modifique sliders (Genética, Literatura, etc.) para ver cómo la IA evalúa un candidato teórico en tiempo real.

        2. **Joyas Ocultas**: Listado priorizado de los 38 genes descubiertos por el modelo.

        3. **Explorador de Genes:** Busque cualquier gen (ej. SORL1) para ver su perfil biológico y la predicción de éxito.


    > **Nota:** La aplicación se abrirá automáticamente en su navegador predeterminado (usualmente http://localhost:8501).



### ⚠️ Solución de Problemas Comunes
- **Error "Module not found"**: Asegúrese de haber activado su entorno virtual antes de ejecutar los comandos y de que la instalación de requirements.txt finalizó sin errores.

- **Error de Rutas (FileNotFound)**: Los scripts están configurados para ejecutarse desde la raíz del proyecto. Asegúrese de que su terminal está en la carpeta principal (TFM - Data Science, Big data & Business Analytics) y no dentro de scripts/.