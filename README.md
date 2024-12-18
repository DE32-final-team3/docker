# Apache Airflow in Docker Container

### 파일 설명
```
├── airflow
│   ├── airflow-compose.yml #airflow를 실행 하는 docker compose file
│   ├── dags
│   │   ├── py #DAG에서 사용되는 python모듈들
│   │   │   ├── __init__.py
│   │   │   ├── calculate.b.py
│   │   │   ├── calculate.py #유사도 계산 모듈
│   │   │   ├── processing.b.py
│   │   │   └── processing.py #데이터 전처리 모듈
│   │   └── similarity.py #airflow내에 실행 되는 DAG 파일
│   └── requirements.txt
├── prometheus-compose.yaml
└── spark
    ├── README.md
    └── spark-compose.yaml
```

### docker compose 실행
```
docker compose -f airflow-compose.yml up -d
```
