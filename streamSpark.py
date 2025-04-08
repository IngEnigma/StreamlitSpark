import streamlit as st
import requests
import pandas as pd
import json

# Función para enviar job a GitHub Actions
def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    url = f'https://api.github.com/repos/{user}/{repo}/dispatches'
    payload = {
        "event_type": job,
        "client_payload": {
            "codeurl": codeurl,
            "dataseturl": dataseturl
        }
    }
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github.v3+json',
        'Content-type': 'application/json'
    }
    response = requests.post(url, json=payload, headers=headers)
    st.write(f"Request to: {url}")
    st.write("Payload:", payload)
    st.write("Response code:", response.status_code)
    st.write("Response text:", response.text)

# 🔁 Función para llamar al Producer API (versión actualizada)
def process_crimes_to_kafka():
    jsonl_url = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"
    producer_url = "https://kafka-postgres-consumer.onrender.com/send-crimes"  # URL de tu producer en Render
    
    try:
        with st.spinner('Enviando datos al producer de Kafka...'):
            response = requests.post(producer_url)
            
            if response.status_code == 200:
                result = response.json()
                st.success(f"✅ Datos enviados correctamente a Kafka!")
                st.info(f"📊 Mensaje: {result['message']}")
                st.info(f"📦 Registros enviados: {len(response.text.strip().splitlines())}")
            else:
                st.error(f"❌ Error {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        st.error(f"⚠️ Error de conexión: {str(e)}")
    except Exception as e:
        st.error(f"⚠️ Error inesperado: {str(e)}")

# 🧾 Función para obtener datos de PostgreSQL
def get_data_from_postgres():
    try:
        # Establecer la conexión utilizando la configuración en secrets.toml
        conn = st.connection("neon", type="sql")

        # Definir la consulta SQL que deseas ejecutar
        query = "SELECT * FROM crimes;"

        # Ejecutar la consulta y obtener los datos en un DataFrame de pandas
        df = conn.query(query, ttl=600)  # ttl=600 segundos para cachear los resultados

        # Mostrar los datos en la aplicación
        st.success("Datos obtenidos correctamente desde PostgreSQL 🐘")
        st.dataframe(df)

        # Mostrar métricas básicas
        st.subheader("📊 Métricas de los datos")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total registros", len(df))
        col2.metric("Edad promedio víctimas", round(df['victim_age'].mean(), 1))
        col3.metric("Tipos de crimen", df['crm_cd_desc'].nunique())

    except Exception as e:
        st.error(f"⚠️ Error al conectar con la base de datos: {str(e)}")

# ===============================
# UI Streamlit Mejorada
# ===============================

st.title("🏢 BigData Dashboard - Sistema de Criminalidad")

tab1, tab2, tab3 = st.tabs(["Spark Jobs", "Kafka/PostgreSQL", "MongoDB"])

with tab1:
    st.header("⚡ Submit Spark Job")
    github_user = st.text_input('GitHub user', value='IngEnigma', key='user')
    github_repo = st.text_input('GitHub repo', value='StreamlitSpark', key='repo')
    spark_job = st.text_input('Spark job name', value='spark', key='job')
    github_token = st.text_input('GitHub token', value='', type='password', key='token')
    code_url = st.text_input('Code URL', value='', key='code')
    dataset_url = st.text_input('Dataset URL', value='', key='dataset')

    if st.button("🚀 Ejecutar Spark Job", key='spark_btn'):
        post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)

with tab2:
    st.header("📊 Pipeline Kafka → PostgreSQL")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔄 Cargar datos a Kafka", key='kafka_btn'):
            process_crimes_to_kafka()

    with col2:
        if st.button("📥 Obtener datos de PostgreSQL", key='pg_btn'):
            get_data_from_postgres()

with tab3:
    st.header("🛢️ MongoDB Integration")
    st.info("Próximamente...")
    # Aquí puedes añadir la funcionalidad para MongoDB cuando esté lista
