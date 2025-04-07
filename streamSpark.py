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

# Función para mostrar resultados desde archivo JSONL
def get_spark_results(url_results):
    if not url_results.startswith("http"):
        st.error("La URL no es válida. Introduce una URL HTTP/HTTPS.")
        return

    response = requests.get(url_results)
    if response.status_code == 200:
        try:
            json_lines = response.text.strip().split("\n")
            data = [json.loads(line) for line in json_lines]
            df = pd.DataFrame(data[:100])
            st.dataframe(df)
        except json.JSONDecodeError as e:
            st.error(f"Error al decodificar JSON: {e}")
    else:
        st.error(f"Error {response.status_code}: No se pudieron obtener los datos.")

# 🔁 Nueva función para llamar al Producer API
def process_crimes_to_kafka():
    jsonl_url = "https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl"
    api_url = "http://localhost:5000/process-data"  # Cambia si estás usando Docker u otra IP

    try:
        response = requests.post(api_url, json={"url": jsonl_url})
        if response.status_code == 200:
            result = response.json()
            st.success(f"✅ {result['message']}")
            st.info(f"📦 Registros procesados: {result['records_processed']}")
            st.info(f"🧵 Tópico Kafka: {result['kafka_topic']}")
        else:
            st.error(f"❌ Error {response.status_code}: {response.text}")
    except Exception as e:
        st.error(f"⚠️ Error al conectar con el API: {str(e)}")

# 🧾 Función para hacer SELECT desde PostgreSQL
def get_data_from_postgres():
    try:
        url = "http://localhost:8000/crimes"  # Cambia si tu consumer ofrece otro endpoint
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            st.success("Datos obtenidos correctamente desde la DB 🐘")
            st.dataframe(df)
        else:
            st.error(f"❌ Error {response.status_code}: {response.text}")
    except Exception as e:
        st.error(f"⚠️ Error al conectar con el API: {str(e)}")

# ===============================
# UI Streamlit
# ===============================

st.title("BigData Dashboard")

st.header("Submit Spark Job")
github_user  = st.text_input('Github user', value='IngEnigma')
github_repo  = st.text_input('Github repo', value='StreamlitSpark')
spark_job    = st.text_input('Spark job', value='spark')
github_token = st.text_input('Github token', value='', type='password')
code_url     = st.text_input('Code URL', value='')
dataset_url  = st.text_input('Dataset URL', value='')

if st.button("POST Spark Submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)

st.header("Migrate Data To Postgresql")

if st.button("POST a Kafka API"):
    st.info("📤 Enviando archivo JSONL al productor Kafka...")
    process_crimes_to_kafka()

if st.button("GET data from DB"):
    get_data_from_postgres()

st.header("Migrate Data To MongoDb")
