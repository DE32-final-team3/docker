import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
from py.processing import process_movie_data, preprocess_genres
from py.calculate import compute_final_similarity

# 전역 설정
MONGO_URI = "mongodb://root:cine@3.37.94.149:27017/?authSource=admin"
DB_NAME = "cinetalk"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

logger = logging.getLogger("airflow.task")

def fetch_user_movie_dict():
    """MongoDB에서 사용자 데이터를 가져옵니다."""
    logger.info("Connecting to MongoDB to fetch user movie data...")
    with MongoClient(MONGO_URI) as client:
        db = client[DB_NAME]
        collection = db["user"]
        user_data = list(collection.find({}, {"_id": 1, "movie_list": 1}))
        
        if not user_data:
            logger.error("No data found in the 'user' collection.")
            raise ValueError("No data fetched. Collection might be empty or fields are missing.")
        
        user_movie_dict = {
            str(user["_id"]): user.get("movie_list", []) for user in user_data
        }
        logger.info(f"Fetched user_movie_dict: {user_movie_dict}")
        return user_movie_dict

def collect_and_process_data(**context):
    user_movie_dict = fetch_user_movie_dict()
    if not user_movie_dict:
        raise ValueError("Failed to fetch user movie data from MongoDB")
    
    processed_data = process_movie_data(user_movie_dict)
    context['ti'].xcom_push(key='processed_data', value=processed_data)

def map_genres(**context):
    processed_data = context['ti'].xcom_pull(key='processed_data')
    user_genre_matrix = processed_data['user_genre_matrix']
    mapped_genres = {
        user_id: [
            {"genre": genre, "mapped_name": preprocess_genres([genre])[0]}
            for genre in genres.keys()
        ]
        for user_id, genres in user_genre_matrix.items()
    }
    context['ti'].xcom_push(key='mapped_genres', value=mapped_genres)

def calculate_similarity(**context):
    processed_data = context['ti'].xcom_pull(key='processed_data')
    similarity_matrix = compute_final_similarity(**processed_data)
    if not similarity_matrix:
        raise ValueError("Failed to compute similarity matrix.")
    context['ti'].xcom_push(key='similarity_data', value=similarity_matrix)

def save_to_mongo(**context):
    similarity_data = context['ti'].xcom_pull(key='similarity_data')
    execution_date = context['execution_date'].strftime('%Y%m%d')
    collection_name = f"{execution_date}_similarity"

    if not similarity_data:
        raise ValueError("No similarity data to save.")

    logger.info(f"Saving similarity data to MongoDB: {collection_name}...")
    with MongoClient(MONGO_URI) as client:
        db = client[DB_NAME]
        collection = db[collection_name]
        collection.insert_many(similarity_data)
    logger.info(f"Successfully saved {len(similarity_data)} records.")

with DAG(
    dag_id="user_similarity",
    default_args=default_args,
    description="Compute user similarity matrix daily and save to MongoDB",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 4),
    catchup=False,
) as dag:
    start_task = EmptyOperator(task_id="start")

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

    end_task = EmptyOperator(task_id="end")

    start_task >> collect_task >> genre_mapping_task >> similarity_task >> save_task >> end_task