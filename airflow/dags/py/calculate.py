import pandas as pd
import requests
from itertools import combinations
from sklearn.metrics.pairwise import cosine_similarity

# FastAPI 호출 함수
def fetch_movies(movie_ids, endpoint="http://localhost:8000/movies/list"):
    try:
        response = requests.get(endpoint, params={"movie_ids": movie_ids})
        if response.status_code == 200:
            return response.json().get("movies", [])
        else:
            raise Exception(f"Failed to fetch movies: {response.status_code}, {response.text}")
    except Exception as e:
        print(e)
        return []

# 유사도 계산 함수들
def calculate_cosine_similarity(user_genre_matrix):
    return cosine_similarity(user_genre_matrix.values)

def calculate_jaccard_similarity(user_sets, user_ids):
    matrix = pd.DataFrame(index=user_ids, columns=user_ids, dtype=float)
    for user1, user2 in combinations(user_ids, 2):
        set1 = user_sets.get(user1, set())
        set2 = user_sets.get(user2, set())
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        similarity = intersection / union if union > 0 else 0
        matrix.loc[user1, user2] = similarity
        matrix.loc[user2, user1] = similarity
    matrix.fillna(1, inplace=True)
    return matrix

def compute_final_similarity(user_ids, user_genre_matrix, user_director_sets, user_cast_sets, user_title_sets):
    # user_genre_matrix 복원 (dict -> DataFrame)
    user_genre_matrix = pd.DataFrame.from_dict(user_genre_matrix)

    # user_director_sets, user_cast_sets, user_title_sets 복원 (list -> set)
    user_director_sets = {k: set(v) for k, v in user_director_sets.items()}
    user_cast_sets = {k: set(v) for k, v in user_cast_sets.items()}
    user_title_sets = {k: set(v) for k, v in user_title_sets.items()}

    # 1. 코사인 유사도 계산 (장르)
    cosine_sim = calculate_cosine_similarity(user_genre_matrix)
    cosine_sim_df = pd.DataFrame(cosine_sim, index=user_genre_matrix.index, columns=user_genre_matrix.index)

    # 2. 자카드 유사도 계산 (감독, 배우, 제목)
    jaccard_director = calculate_jaccard_similarity(user_director_sets, user_ids)
    jaccard_cast = calculate_jaccard_similarity(user_cast_sets, user_ids)
    jaccard_title = calculate_jaccard_similarity(user_title_sets, user_ids)

    # 3. 인덱스와 열 이름 기준으로 행렬 합산
    combined_similarity = cosine_sim_df.add(jaccard_director, fill_value=0)
    combined_similarity = combined_similarity.add(jaccard_cast, fill_value=0)
    combined_similarity = combined_similarity.add(jaccard_title, fill_value=0)

    # 4. 결과 반환
    return combined_similarity.reset_index().to_dict(orient="records")
