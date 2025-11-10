# 🧩 Aladin-Pipeline

**알라딘 베스트셀러 리뷰 분석 자동화 파이프라인**  
이 프로젝트는 주간 베스트셀러 데이터를 자동 수집·저장하고,  
리뷰 분석 및 시각화를 수행하는 **데이터 엔지니어링 기반의 ETL 파이프라인**입니다.  

---
## Architecture
- **VM 1 (Master)**: HDFS Namenode, ResourceManager, MongoDB, Grafana
- **VM 2-4 (Workers)**: Spark Workers, Datanodes
- **Client Node**: Job execution, Airflow scheduler

## 프로젝트 개요

| 구분 | 내용 |
|------|------|
| **주요 기술** | Python, Airflow, Hadoop, Spark, MongoDB, OpenSearch, Grafana |
| **데이터 소스** | 알라딘 주간 베스트셀러 & 네이버 리뷰 API |
| **데이터 저장소** | MongoDB (Raw & Aggregated Data) |
| **분석 대상** | 도서별 리뷰, 키워드, 주차별 트렌드 |
| **결과 시각화** | OpenSearch Dashboard + Grafana + Streamlit |