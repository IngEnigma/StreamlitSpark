import streamlit as st
import pandas as pd
import requests

GITHUB_USER_DEFAULT = "IngEnigma"
GITHUB_REPO_DEFAULT = "Streamlit_Spark"
PRODUCER_URL = "https://kafka-postgres-producer.onrender.com/send-crimes"
JSONL_URL = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    url = f'https://api.github.com/repos/{user}/{repo}/dispatches'
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github.v3+json',
        'Content-type': 'application/json'
    }
    payload = {
        "event_type": job,
        "client_payload": {
            "codeurl": codeurl,
            "dataseturl": dataseturl
        }
    }

    response = requests.post(url, json=payload, headers=headers)
    st.write(f"📡 Request: {url}")
    st.write("📦 Payload:", payload)
    st.write("📬 Código de respuesta:", response.status_code)
    st.write("📬 Texto de respuesta:", response.text)

def process_crimes_to_kafka():
    try:
        with st.spinner('🚀 Enviando datos al producer de Kafka...'):
            response = requests.post(PRODUCER_URL)

            if response.ok:
                result = response.json()
                st.success("✅ Datos enviados correctamente a Kafka!")
                st.info(f"📊 Mensaje: {result['message']}")
            else:
                st.error(f"❌ Error {response.status_code}: {response.text}")
    except requests.RequestException as e:
        st.error(f"⚠️ Error de conexión: {str(e)}")

def get_data_from_postgres():
    try:
        conn = st.connection("neon", type="sql")
        query = "SELECT * FROM crimes;"
        df = conn.query(query, ttl=600)

        st.success("✅ Datos obtenidos correctamente desde PostgreSQL 🐘")
        st.dataframe(df)

        st.subheader("📊 Métricas de los datos")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total registros", len(df))
        col2.metric("Edad promedio víctimas", round(df['victim_age'].mean(), 1))
        col3.metric("Tipos de crimen", df['crm_cd_desc'].nunique())
    except Exception as e:
        st.error(f"⚠️ Error al conectar con la base de datos: {str(e)}")

def process_area_to_kafka():
    st.warning("⚠️ Función no implementada aún: process_area_to_kafka()")

def get_data_from_mongo():
    st.warning("⚠️ Función no implementada aún: get_data_from_mongo()")

st.title("🏢 BigData Dashboard - Sistema de Criminalidad")

tab1, tab2, tab3 = st.tabs(["Spark Jobs", "Kafka/PostgreSQL", "Kafka/MongoDB"])

with tab1:
    st.header("⚡ Submit Spark Job")

    with st.form("spark_form"):
        github_user = st.text_input("GitHub user", value=GITHUB_USER_DEFAULT)
        github_repo = st.text_input("GitHub repo", value=GITHUB_REPO_DEFAULT)
        spark_job = st.text_input("Spark job name", value="spark")
        github_token = st.text_input("GitHub token", value="", type="password")
        code_url = st.text_input("Code URL")
        dataset_url = st.text_input("Dataset URL")

        if st.form_submit_button("🚀 Ejecutar Spark Job"):
            post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)

with tab2:
    st.header("📊 Pipeline Kafka → PostgreSQL")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Cargar datos a Kafka"):
            process_crimes_to_kafka()

    with col2:
        if st.button("📥 Obtener datos de PostgreSQL"):
            get_data_from_postgres()

with tab3:
    st.header("📊 Pipeline Kafka → MongoDB")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔄 Cargar datos a Kafka"):
            process_area_to_kafka()

    with col2:
        if st.button("📥 Obtener datos de MongoDB"):
            get_data_from_mongo()
