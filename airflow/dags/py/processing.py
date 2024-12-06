import pandas as pd
from py.calculate import fetch_movies

GENRE_MAP = {
    28: "액션",
    12: "모험",
    16: "애니메이션",
    35: "코미디",
    80: "범죄",
    99: "다큐멘터리",
    18: "드라마",
    10751: "가족",
    14: "판타지",
    36: "역사",
    27: "공포",
    10402: "음악",
    9648: "미스터리",
    10749: "로맨스",
    878: "SF",
    10770: "TV 영화",
    53: "스릴러",
    10752: "전쟁",
    37: "서부",
}

def preprocess_genres(genre_ids: list[int]) -> list[str]:
    """장르 ID를 한글로 매핑"""
    return [GENRE_MAP.get(genre_id, "") for genre_id in genre_ids]

# 장르 매트릭스 생성
def create_user_genre_matrix(df):
    expanded_df = df.explode("genres")
    user_genre_counts = expanded_df.groupby(["user_id", "genres"]).size().reset_index(name="count")
    user_genre_matrix = user_genre_counts.pivot_table(index="user_id", columns="genres", values="count", fill_value=0)

    # 2번 이상 등장한 장르만 유지
    user_genre_matrix = user_genre_matrix.where(user_genre_matrix >= 2, other=0)
    return user_genre_matrix

# 사용자-감독 집합 생성
def create_user_director_sets(df):
    filtered_directors = (
        df.groupby(["user_id", "director"])
        .size()
        .reset_index(name="count")
        .query("count >= 2")  # 2번 이상 등장한 감독만 필터링
    )
    user_director_sets = (
        filtered_directors.groupby("user_id")["director"]
        .apply(set)
        .to_dict()
    )
    return user_director_sets

# 사용자-배우 집합 생성
def create_user_cast_sets(df):
    df_exploded_cast = df.explode("cast")  # 배우 열 확장
    filtered_casts = (
        df_exploded_cast.groupby(["user_id", "cast"])
        .size()
        .reset_index(name="count")
        .query("count >= 3")  # 3번 이상 등장한 배우만 필터링
    )
    user_cast_sets = (
        filtered_casts.groupby("user_id")["cast"]
        .apply(set)
        .to_dict()
    )
    return user_cast_sets

# 사용자-영화 제목 집합 생성
def create_user_title_sets(df):
    user_title_sets = (
        df.groupby("user_id")["title"]
        .apply(set)
        .to_dict()
    )
    return user_title_sets

def process_movie_data(user_movie_dict):
    data = []
    for user_id, movie_ids in user_movie_dict.items():
        movies = fetch_movies(movie_ids)
        for movie in movies:
            data.append({
                "user_id": user_id,
                "movie_id": movie["movie_id"],
                "title": movie["title"],
                "genres": movie["genres"],
                "director": movie["director"],
                "cast": movie["cast"]
            })
    df = pd.DataFrame(data)

    # 매트릭스 및 집합 생성
    user_genre_matrix = create_user_genre_matrix(df).to_dict()  # DataFrame을 dict로 변환
    user_director_sets = {k: list(v) for k, v in create_user_director_sets(df).items()}  # set -> list
    user_cast_sets = {k: list(v) for k, v in create_user_cast_sets(df).items()}  # set -> list
    user_title_sets = {k: list(v) for k, v in create_user_title_sets(df).items()}  # set -> list

    return {
        "user_ids": list(df['user_id'].unique()),  # numpy array -> list
        "user_genre_matrix": user_genre_matrix,
        "user_director_sets": user_director_sets,
        "user_cast_sets": user_cast_sets,
        "user_title_sets": user_title_sets,
    }