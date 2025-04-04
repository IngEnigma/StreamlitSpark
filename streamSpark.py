import streamlit as st
import requests
import pandas as pd
import json

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    url = 'https://api.github.com/repos/' + user + '/' + repo + '/dispatches'
    payload = {
     "event_type": job,
     "client_payload": {
        "codeurl": codeurl,
        "dataseturl": dataseturl
     }
    }
    headers = {
      'Authorization': 'Bearer ' + token,
      'Accept': 'application/vnd.github.v3+json',
      'Content-type': 'application/json'
    }
    st.write(url)
    st.write(payload)
    st.write(headers)
    response = requests.post(url, json=payload, headers=headers)
    st.write(response)

def get_spark_results(url_results):
    if not url_results.startswith("http"):
        st.error("La URL no es válida. Introduce una URL HTTP/HTTPS.")
        return

    response = requests.get(url_results)

    if response.status_code == 200:
        try:
            json_lines = response.text.strip().split("\n")
            data = [json.loads(line) for line in json_lines] 
            data = data[:100]
            df = pd.DataFrame(data)
            st.dataframe(df)
        except json.JSONDecodeError as e:
            st.error(f"Error al decodificar JSON: {e}")
    else:
        st.error(f"Error {response.status_code}: No se pudieron obtener los datos.")

def process_crimes_to_kafka(api_url, jsonl_url):
    try:
        response = requests.post(api_url, json={"url": jsonl_url})
        if response.status_code == 200:
            result = response.json()
            st.success(f"Success: {result['message']}")
            st.info(f"Records processed: {result['records_processed']}")
            st.info(f"Kafka topic: {result['kafka_topic']}")
        else:
            st.error(f"Error: {response.status_code} - {response.text}")
    except Exception as e:
        st.error(f"Error connecting to API: {str(e)}")

st.title("BigData")

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

url_results = st.text_input('Results URL', value='')
if st.button("Get Spark Results"):
    get_spark_results(url_results)

st.header("Send Data to Kafka")

# Aquí puedes configurar la URL de tu API FastAPI
fastapi_url = st.text_input(
    'FastAPI URL', 
    value='http://localhost:8000/process-crimes',
    help="URL del endpoint FastAPI que procesa los datos"
)

jsonl_url = st.text_input(
    'JSONL URL', 
    value='https://raw.githubusercontent.com/IngEnigma/StreamlitSpark/refs/heads/master/results/male_crimes/data.jsonl',
    help="URL del archivo JSONL con los datos a enviar a Kafka"
)

if st.button("Process Crimes and Send to Kafka"):
    process_crimes_to_kafka(fastapi_url, jsonl_url)
