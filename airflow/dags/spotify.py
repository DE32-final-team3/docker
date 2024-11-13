from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests

client_id = '386885594a89422f96f138f865cdcabc'
client_secret = '44e24a688b414618a9fee49eefb4e275'

with DAG(
    'spotify_token_dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Fetch Spotify access token every hour',
    schedule_interval=timedelta(hours=1),  # 매시간 실행
    start_date=datetime(2024, 11, 13),
    catchup=False,
) as dag:


    def fetch_spotify_token():
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
        }

        response = requests.post('https://accounts.spotify.com/api/token', data=data)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            print("ACCESS TOKEN:", access_token)
        
            # 예시 (Airflow Variable 사용):
            from airflow.models import Variable
            Variable.set("spotify_token", access_token)
        
        else:
            print("FAILED", response.status_code, response.text)

    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end')

    get_token = PythonOperator(
        task_id='fetch_spotify_token',
        python_callable=fetch_spotify_token,
        )

    task_start >> get_token >> task_end
