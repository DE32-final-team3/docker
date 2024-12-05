from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests

# Spotify API credentials
# client_id = 'e33936d572b3493aa686de2728097788'
client_id = 'd86e7a296d0b4560ba4bd712b7c9fb1c'
# client_secret = 'd22957fb84df486386cd64bf595f1e31'
client_secret = 'efe71cea195c4272826d15604919b59b'

# DAG 정의
with DAG(
    'spotify_token_dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Fetch Spotify access token and log it',
    schedule_interval=timedelta(hours=1),  # 매시간 실행
    start_date=datetime(2024, 11, 13),
    catchup=False,
) as dag:

    # Task: Spotify 토큰 생성 및 로그 출력
    def fetch_token():
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
        }

        response = requests.post('https://accounts.spotify.com/api/token', data=data)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            print(f"ACCESS TOKEN: {access_token}")  # 로그에 출력
        else:
            print(f"FAILED: {response.status_code}, {response.text}")
            raise Exception("Failed to fetch Spotify token")

    fetch_token_task = PythonOperator(
        task_id='fetch_token',
        python_callable=fetch_token,
    )

    # DAG 실행 흐름 정의
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end')

    task_start >> fetch_token_task >> task_end
