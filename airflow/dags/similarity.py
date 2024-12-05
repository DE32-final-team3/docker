from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from pymongo import MongoClient
import logging

# 데이터 처리 함수 임포트
from py.processing import (
    create_user_genre_matrix,
    create_user_director_sets,
    create_user_cast_sets,
    create_user_title_sets,
)
from py.calculate import fetch_movies, compute_final_similarity

# 전역 변수 정의
USER_IDS = ['seokxkyu', 'j25ng', 's00zzang', 'masters4297', 'thephunkmonk']
USER_MOVIE_DICT = {
    'seokxkyu': [316029, 616670, 567646, 122906, 84111, 424694, 313369, 198277, 257211, 20342],
    'j25ng': [41531, 354072, 128, 296096, 257211, 20453, 20342, 70160, 671, 1858],
    's00zzang': [12153, 354912, 1278, 556509, 382336, 76341, 11104, 410225, 705996, 410220],
    'masters4297': [273248, 122, 475557, 58, 919207, 496243, 424694, 107235, 603692, 278],
    'thephunkmonk': [115, 985, 1018, 793, 29269, 31512, 293670, 429200, 798286, 473033],
}

# MongoDB 설정
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "similarity"
COLLECTION_NAME = "similarity_results"

# 기본 DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id="user_similarity",
    default_args=default_args,
    description="Compute user similarity matrix daily and save to MongoDB",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 4),
    catchup=False,
) as dag:

    # def collect_data(**context):
    #     data = []
    #     for user_id, movie_ids in USER_MOVIE_DICT.items():
    #         movies = fetch_movies(movie_ids)  # FastAPI를 호출하여 영화 데이터 가져오기
    #         for movie in movies:
    #             data.append({
    #                 "user_id": user_id,
    #                 "movie_id": movie["movie_id"],
    #                 "title": movie["title"],
    #                 "genres": movie["genres"],
    #                 "director": movie["director"],
    #                 "cast": movie["cast"]
    #             })
    #     df = pd.DataFrame(data)
    #     context['ti'].xcom_push(key='movie_data', value=df.to_json())

    def collect_data(**context):
        data = []
        for user_id, movie_ids in USER_MOVIE_DICT.items():
            # 영화 데이터 가져오기 (FastAPI 호출)
            movies = fetch_movies(movie_ids)  # FastAPI를 호출하여 영화 데이터 가져오기
            print(f"Fetched movies for user_id {user_id}: {movies}")  # 디버깅용 출력
            
            for movie in movies:
                # 영화 데이터를 리스트에 추가
                data.append({
                    "user_id": user_id,
                    "movie_id": movie["movie_id"],
                    "title": movie["title"],
                    "genres": movie["genres"],
                    "director": movie["director"],
                    "cast": movie["cast"]
                })
        
        # DataFrame 생성
        df = pd.DataFrame(data)
        print(f"Collected DataFrame Columns: {df.columns.tolist()}")  # 디버깅용 출력
        print(f"First Few Rows of DataFrame:\n{df.head()}")  # 디버깅용 출력

        # DataFrame을 JSON으로 변환하여 XCom에 저장
        context['ti'].xcom_push(key='movie_data', value=df.to_json())
        print("Data successfully pushed to XCom with key 'movie_data'.")  # 확인 메시지


    # def process_similarity_matrices(**context):
    #     df = pd.read_json(context['ti'].xcom_pull(key='movie_data'))
        
    #     # 데이터 처리
    #     user_genre_matrix = create_user_genre_matrix(df)
    #     user_director_sets = create_user_director_sets(df)
    #     user_cast_sets = create_user_cast_sets(df)
    #     user_title_sets = create_user_title_sets(df)
        
    #     # 유사도 계산
    #     final_similarity = compute_final_similarity(
    #         user_ids=df['user_id'].unique(),
    #         user_genre_matrix=user_genre_matrix,
    #         user_director_sets=user_director_sets,
    #         user_cast_sets=user_cast_sets,
    #         user_title_sets=user_title_sets
    #     )
        
    #     # 결과 저장
    #     context['ti'].xcom_push(key='similarity_data', value=final_similarity.reset_index().to_dict(orient="records"))


    def process_similarity_matrices(**context):
        # XCom에서 데이터를 가져옵니다.
        try:
            raw_json = context['ti'].xcom_pull(key='movie_data')
            if not raw_json:
                raise ValueError("No data received from XCom.")

            logging.info("Raw JSON Data: %s", raw_json)

            # JSON 데이터를 DataFrame으로 변환합니다.
            try:
                from io import StringIO
                df = pd.read_json(StringIO(raw_json))
            except ValueError as e:
                raise ValueError(f"Error while reading JSON data: {e}")

            logging.info("DataFrame Columns: %s", df.columns.tolist())
            logging.info("First Few Rows:\n%s", df.head())

            # `genres` 컬럼 디버깅
            if 'genres' not in df.columns:
                raise KeyError("'genres' column not found in the DataFrame.")
            
            # 유저 장르 매트릭스 생성
            user_genre_matrix = create_user_genre_matrix(df)
            logging.info("User Genre Matrix Created Successfully.")
            return user_genre_matrix

        except Exception as e:
            logging.error("Error in process_similarity_matrices: %s", str(e))
            raise

    def save_results_to_mongo(**context):
        json_data = context['ti'].xcom_pull(key='similarity_data')
        try:
            client = MongoClient(MONGO_URI)
            db = client[DB_NAME]
            collection = db[COLLECTION_NAME]
            collection.insert_many(json_data)
            print(f"Successfully inserted {len(json_data)} documents into MongoDB.")
        except Exception as e:
            print(f"Error while inserting into MongoDB: {e}")
        finally:
            client.close()

    # Task 정의
    collect_task = PythonOperator(
        task_id="collect_data",
        python_callable=collect_data,
        provide_context=True,
    )

    process_task = PythonOperator(
        task_id="process_similarity",
        python_callable=process_similarity_matrices,
        provide_context=True,
    )

    save_task = PythonOperator(
        task_id="save_to_mongo",
        python_callable=save_results_to_mongo,
        provide_context=True,
    )

    # Task 의존성 설정
    collect_task >> process_task >> save_task

