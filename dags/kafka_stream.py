from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum


default_args = {
    'owner' : 'ishanMaduranga',
    'start_date' : pendulum.now("UTC").subtract(days=1)
}

def stream_data():
    import json
    import requests

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    print(json.dumps(res, indent=3))

with DAG(dag_id='user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable = stream_data
    )

stream_data()