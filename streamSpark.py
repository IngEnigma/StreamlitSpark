import streamlit as st
import requests
import pandas  as pd

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
    response = requests.get(url_results)

    if  (response.status_code ==  200):
        st.write(response.json())
    else:
        st.write(response)

st.title("BigData")

st.header("Submit Spark Job")

github_user  =  st.text_input('Github user', value='IngEnigma')
github_repo  =  st.text_input('Github repo', value='StreamlitSpark')
spark_job    =  st.text_input('Spark job', value='spark')
github_token =  st.text_input('Github token', value='')
code_url     =  st.text_input('Code URL', value='')
dataset_url  =  st.text_input('Dataset URL', value='')

if st.button("POST Spark Submit"):
   post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)

st.header("GET Spark Result")

url_results=  st.text_input('URL results', value='')

if st.button("GET spark results"):
    get_spark_results(url_results)
