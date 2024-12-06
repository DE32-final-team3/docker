import pandas as pd

# 장르 매트릭스 생성
# def create_user_genre_matrix(df):
#     expanded_df = df.explode("genres")  # 장르를 펼쳐서 행 확장
#     user_genre_counts = expanded_df.groupby(["user_id", "genres"]).size().reset_index(name="count")
#     user_genre_matrix = user_genre_counts.pivot_table(index="user_id", columns="genres", values="count", fill_value=0)
#     return user_genre_matrix

def create_user_genre_matrix(df):
    # 장르를 펼쳐서 행을 확장
    expanded_df = df.explode("genres")
    print(f"Expanded DataFrame:\n{expanded_df.head()}")  # 디버깅용 출력

    # user_id와 genre로 그룹화하여 카운트 계산
    user_genre_counts = expanded_df.groupby(["user_id", "genres"]).size().reset_index(name="count")
    print(f"User Genre Counts:\n{user_genre_counts.head()}")  # 디버깅용 출력

    # 사용자-장르 매트릭스로 변환
    user_genre_matrix = user_genre_counts.pivot_table(
        index="user_id", columns="genres", values="count", fill_value=0
    )
    print(f"User Genre Matrix (Before Filtering):\n{user_genre_matrix}")  # 디버깅용 출력

    # 2번 이상 등장한 장르만 필터링
    user_genre_matrix = user_genre_matrix.applymap(lambda x: x if x >= 2 else 0.0)
    print(f"User Genre Matrix (After Filtering):\n{user_genre_matrix}")  # 디버깅용 출력

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

