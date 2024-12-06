from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from py.processing import process_movie_data, preprocess_genres
from py.calculate import compute_final_similarity

# 전역 변수 정의
USER_MOVIE_DICT = {
    'seokxkyu': [316029, 616670, 567646, 122906, 84111, 424694, 313369, 198277, 257211, 20342],
    'j25ng': [41531, 354072, 128, 296096, 257211, 20453, 20342, 70160, 671, 1858],
    's00zzang': [12153, 354912, 1278, 556509, 382336, 76341, 11104, 410225, 705996, 410220],
    'masters4297': [273248, 122, 475557, 58, 919207, 496243, 424694, 107235, 603692, 278],
    'thephunkmonk': [115, 985, 1018, 793, 29269, 31512, 293670, 429200, 798286, 473033],
}

# MONGO_URI = "mongodb://root:team3@mongodb:27017/?authSource=admin"
MONGO_URI = "mongodb://root:team3@172.17.0.1:27017/?authSource=admin"
DB_NAME = "similarity"
COLLECTION_NAME = "similarity_results"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="user_similarity",
    default_args=default_args,
    description="Compute user similarity matrix daily and save to MongoDB",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 4),
    catchup=False,
) as dag:
    
    def collect_and_process_data(**context):
        processed_data = process_movie_data(USER_MOVIE_DICT)
        context['ti'].xcom_push(key='processed_data', value=processed_data)

    def map_genres(**context):
        processed_data = context['ti'].xcom_pull(key='processed_data')
        user_genre_matrix = processed_data['user_genre_matrix'].copy()
        for user_id, genres in user_genre_matrix.items():
            user_genre_matrix[user_id] = [
                {"genre": genre, "mapped_name": preprocess_genres([genre])[0]}
                for genre in genres.keys()
            ]
        context['ti'].xcom_push(key='mapped_genres', value=user_genre_matrix)


    def calculate_similarity(**context):
        processed_data = context['ti'].xcom_pull(key='processed_data')
        similarity_matrix = compute_final_similarity(**processed_data)
        context['ti'].xcom_push(key='similarity_data', value=similarity_matrix)

    def save_to_mongo(**context):
        similarity_data = context['ti'].xcom_pull(key='similarity_data')
        execution_date = context['execution_date'].strftime('%Y%m%d')  # 날짜 형식: YYYY-MM-DD
        dynamic_collection_name = f"{execution_date}"  # 날짜 기반 컬렉션 이름 생성

        from pymongo import MongoClient
        try:
            client = MongoClient(MONGO_URI)
            db = client[DB_NAME] 
            collection = db[dynamic_collection_name]  
            collection.insert_many(similarity_data)
            print(f"Inserted {len(similarity_data)} documents into {DB_NAME}.{dynamic_collection_name}")
        finally:
            client.close()

    collect_task = PythonOperator(
        task_id="collect_and_process_data",
        python_callable=collect_and_process_data,
        provide_context=True,
    )
    
    genre_mapping_task = PythonOperator(
        task_id="map_genres",
        python_callable=map_genres,
        provide_context=True,
    )

    similarity_task = PythonOperator(
        task_id="calculate_similarity",
        python_callable=calculate_similarity,
        provide_context=True,
    )

    save_task = PythonOperator(
        task_id="save_to_mongo",
        python_callable=save_to_mongo,
        provide_context=True,
    )

    start_task = EmptyOperator(
        task_id="start"
    )

    end_task = EmptyOperator(
        task_id="end"
    )

    start_task >> collect_task >> genre_mapping_task >> similarity_task >> save_task >> end_task