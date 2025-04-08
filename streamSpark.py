import streamlit as st
import requests
import pandas as pd
import json
from pymongo import MongoClient
from datetime import datetime
from bson import json_util

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

# Función para enviar datos a Kafka (PostgreSQL)
def process_crimes_to_kafka():
    producer_url = "https://kafka-postgres-producer.onrender.com/send-crimes"
    
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

# Función para enviar estadísticas de área a Kafka (MongoDB)
def process_area_stats_to_kafka():
    producer_url = "https://kafka-mongodb-producer.onrender.com/send-area-stats"
    
    try:
        with st.spinner('Enviando estadísticas por área a Kafka...'):
            response = requests.post(producer_url)
            
            if response.status_code == 200:
                result = response.json()
                st.success(f"✅ Datos enviados correctamente a Kafka!")
                st.info(f"📊 Mensaje: {result['message']}")
                st.info(f"📦 Registros enviados: {result['stats']['success']}")
            else:
                st.error(f"❌ Error {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        st.error(f"⚠️ Error de conexión: {str(e)}")
    except Exception as e:
        st.error(f"⚠️ Error inesperado: {str(e)}")

# Función para obtener datos de PostgreSQL
def get_data_from_postgres():
    try:
        conn = st.connection("neon", type="sql")
        query = "SELECT * FROM crimes;"
        df = conn.query(query, ttl=600)

        st.success("Datos obtenidos correctamente desde PostgreSQL 🐘")
        st.dataframe(df)

        st.subheader("📊 Métricas de los datos")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total registros", len(df))
        col2.metric("Edad promedio víctimas", round(df['victim_age'].mean(), 1))
        col3.metric("Tipos de crimen", df['crm_cd_desc'].nunique())

    except Exception as e:
        st.error(f"⚠️ Error al conectar con la base de datos: {str(e)}")

# Función para obtener datos de MongoDB
def get_data_from_mongodb(collection_name):
    try:
        # Configuración de MongoDB Atlas
        MONGO_URI = "mongodb+srv://IngEnigma:0ZArHx18XQIFWPHu@bigdata.iwghsuv.mongodb.net/?retryWrites=true&w=majority&appName=BigData"
        client = MongoClient(MONGO_URI)
        db = client["BigData"]
        collection = db[collection_name]
        
        # Obtener los últimos 100 registros
        data = list(collection.find().sort("_id", -1).limit(100))
        client.close()
        
        if not data:
            st.warning(f"No se encontraron datos en la colección {collection_name}")
            return None
            
        # Convertir a DataFrame
        df = pd.json_normalize([json.loads(json_util.dumps(d)) for d in data])
        
        # Limpiar nombres de columnas
        df.columns = [col.replace('.', '_') for col in df.columns]
        
        return df
        
    except Exception as e:
        st.error(f"⚠️ Error al conectar con MongoDB: {str(e)}")
        return None

# Función para mostrar estadísticas de MongoDB
def show_mongodb_stats():
    try:
        MONGO_URI = "mongodb+srv://IngEnigma:0ZArHx18XQIFWPHu@bigdata.iwghsuv.mongodb.net/?retryWrites=true&w=majority&appName=BigData"
        client = MongoClient(MONGO_URI)
        db = client["BigData"]
        
        # Estadísticas generales
        crimes_count = db["crimes"].count_documents({})
        areas_count = db["crime_areas"].count_documents({})
        
        # Última fecha de actualización
        last_crime = db["crimes"].find_one({}, sort=[("metadata_imported_at", -1)])
        last_area = db["crime_areas"].find_one({}, sort=[("metadata_imported_at", -1)])
        
        client.close()
        
        st.subheader("📊 Estadísticas de MongoDB")
        col1, col2 = st.columns(2)
        col1.metric("Total crímenes", crimes_count)
        col2.metric("Estadísticas por área", areas_count)
        
        if last_crime:
            st.info(f"Último crimen registrado: {last_crime.get('metadata', {}).get('imported_at', 'N/A')}")
        if last_area:
            st.info(f"Última estadística por área: {last_area.get('metadata', {}).get('imported_at', 'N/A')}")
            
    except Exception as e:
        st.error(f"Error al obtener estadísticas: {str(e)}")

# UI Streamlit
st.title("🏢 BigData Dashboard - Sistema de Criminalidad")

tab1, tab2, tab3 = st.tabs(["Spark Jobs", "Kafka/PostgreSQL", "MongoDB"])

with tab1:
    st.header("⚡ Submit Spark Job")
    github_user = st.text_input('GitHub user', value='IngEnigma', key='user1')
    github_repo = st.text_input('GitHub repo', value='StreamlitSpark', key='repo1')
    spark_job = st.text_input('Spark job name', value='spark', key='job1')
    github_token = st.text_input('GitHub token', value='', type='password', key='token1')
    code_url = st.text_input('Code URL', value='', key='code1')
    dataset_url = st.text_input('Dataset URL', value='', key='dataset1')

    if st.button("🚀 Ejecutar Spark Job", key='spark_btn1'):
        post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)

with tab2:
    st.header("📊 Pipeline Kafka → PostgreSQL")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Cargar datos a Kafka", key='kafka_btn2'):
            process_crimes_to_kafka()
    with col2:
        if st.button("📥 Obtener datos de PostgreSQL", key='pg_btn2'):
            get_data_from_postgres()

with tab3:
    st.header("🛢️ MongoDB Integration")
    
    st.subheader("Enviar datos a Kafka")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📤 Enviar datos de crímenes", key='mongo_crimes_btn'):
            process_crimes_to_kafka()
    with col2:
        if st.button("📊 Enviar stats por área", key='mongo_stats_btn'):
            process_area_stats_to_kafka()
    
    st.divider()
    
    st.subheader("Visualizar datos desde MongoDB")
    collection_option = st.selectbox(
        "Seleccionar colección",
        ["crimes", "crime_areas"],
        index=0,
        key='mongo_collection'
    )
    
    if st.button("🔍 Cargar datos", key='load_mongo_btn'):
        with st.spinner(f"Cargando datos de {collection_option}..."):
            df = get_data_from_mongodb(collection_option)
            if df is not None:
                st.success(f"Datos obtenidos de MongoDB Atlas 🍃")
                st.dataframe(df)
                
                # Mostrar estadísticas específicas según la colección
                if collection_option == "crimes":
                    st.subheader("Análisis de datos")
                    st.bar_chart(df["victim_age"].value_counts())
                elif collection_option == "crime_areas":
                    st.subheader("Crímenes por área")
                    st.bar_chart(df.set_index("area_number")["crime_count"])
    
    st.divider()
    show_mongodb_stats()
