from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from kafka import KafkaProducer
import time
import logging

default_args = {
    'owner': 'thangquang',
    'start_date': datetime(2024, 10, 13, 10, 00), # -> 10h00 13/10/2024
}

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    address_info = [
        location['street']['number'],
        location['street']['name'],
        location['city'],
        location['state'],
        location['country'],
    ]
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = " ".join([str(x) for x in address_info])
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    data = get_data()
    

    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000
    )
    start_time = time.time()
    while True:
        if time.time() > start_time + 60: 
            break
        try:
            data = get_data()
            data = format_data(data)
            producer.send(topic="user_created", value=json.dumps(data).encode("utf-8"))
        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue

    

with DAG(
    "user_automation",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    streaming_task = PythonOperator(
        task_id="stream_data_from_api",
        python_callable=stream_data
    )
