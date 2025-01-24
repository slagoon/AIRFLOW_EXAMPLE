from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
import requests
import os

# 데이터 크롤러 함수
def cate_crawler(start_date, end_date):
    data_dict = []
    page_num = 1
    while True:
        craw1_url = "https://www.work24.go.kr/cm/openApi/call/hr/callOpenApiSvcInfo310L01.do"
        craw1_params = {
            "authKey": "e4c7fa60-8400-4d6c-8fd9-80552b74b17f",
            "returnType": "JSON",
            "outType": 1,
            "pageNum": f"{page_num}",
            "pageSize": 100,
            "crseTracseSe": "C0104",
            "srchTraStDt": start_date,
            "srchTraEndDt": end_date
        }
        response = requests.get(craw1_url, params=craw1_params)
        craw2_dump = response.json()
        if not craw2_dump or not craw2_dump.get("srchList"):
            print(f"No more data found on page {page_num}. Stopping collection.")
            break
        data_dict.append(craw2_dump["srchList"])
        page_num += 1

    cate_df = pd.DataFrame(sum(data_dict, []))

    column_mapping = {
        "address": "주소",
        "contents": "컨텐츠",
        "courseMan": "수강비",
        "eiEmplCnt3": "고용보험3개월 취업인원 수",
        "eiEmplCnt3Gt10": "고용보험3개월 취업누적인원 10인이하 여부 (Y/N)",
        "eiEmplRate3": "고용보험3개월 취업률",
        "eiEmplRate6": "고용보험6개월 취업률",
        "grade": "등급",
        "instCd": "훈련기관 코드",
        "ncsCd": "NCS 코드",
        "realMan": "실제 훈련비",
        "regCourseMan": "수강신청 인원",
        "stdgScor": "만족도 점수",
        "subTitle": "훈련기관명",
        "subTitleLink": "부 제목 링크",
        "telNo": "전화번호",
        "title": "훈련과정명",
        "titleIcon": "제목 아이콘",
        "titleLink": "제목 링크",
        "traEndDate": "훈련종료일자",
        "traStartDate": "훈련시작일자",
        "trainTarget": "훈련대상",
        "trainTargetCd": "훈련구분",
        "trainstCstId": "훈련기관ID",
        "trngAreaCd": "지역코드(중분류)",
        "trprDegr": "훈련과정 순차",
        "trprId": "훈련과정ID",
        "yardMan": "정원"
    }

    list_df = cate_df.rename(columns=column_mapping)
    list_df2 = list_df.drop(columns=[
        '주소', '컨텐츠', '등급', '부 제목 링크', '전화번호', 
        '고용보험3개월 취업누적인원 10인이하 여부 (Y/N)', 
        '고용보험6개월 취업률', '고용보험3개월 취업인원 수', 
        '고용보험3개월 취업률', '제목 아이콘', '제목 링크'
    ])
    order_column = [
        '훈련과정명', '훈련기관명', '훈련기관ID', '훈련기관 코드', 
        '훈련과정ID', '훈련대상', '훈련구분', '훈련시작일자', 
        '훈련종료일자', '실제 훈련비', '수강비', '만족도 점수', 
        'NCS 코드', '정원', '수강신청 인원', '훈련과정 순차'
    ]
    list_df3 = list_df2[order_column]

    list_df3.to_csv('/tmp/cate_data.csv', index=False)  # CSV 파일로 저장

# GCS 업로드 함수
def upload_to_gcs(bucket_name, object_name, file_path, gcp_conn_id='gc'):
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    hook.upload(bucket_name=bucket_name, object_name=object_name, filename=file_path)
    print(f"Uploaded {file_path} to gs://{bucket_name}/{object_name}")

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
   'cate_data_pipeline_split_tasks',
    default_args=default_args,
    description='Pipeline to crawl data and upload to BigQuery as separate tasks',
    schedule='@daily',  # schedule_interval을 schedule로 변경
    catchup=False,
) as dag:

    # Step 1: 데이터 크롤링 및 CSV 저장
    crawl_data_task = PythonOperator(
        task_id='crawl_data_to_csv',
        python_callable=cate_crawler,
        op_kwargs={
            'start_date': '20210101',
            'end_date': '20251231',
        },
    )

    # Step 2: CSV 파일을 GCS로 업로드
    upload_to_gcs_task = PythonOperator(
        task_id='upload_csv_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            'bucket_name': 'slagoon',               # 실제 GCS 버킷 이름
            'object_name': 'cate_data/cate_data.csv',
            'file_path': '/tmp/cate_data.csv',
            'gcp_conn_id': 'gc',
        },
    )

    # Step 3: GCS에서 BigQuery로 데이터 업로드
    load_to_bigquery_task = BigQueryInsertJobOperator(
        task_id='upload_data_to_bigquery',
        configuration={
            "load": {
                "sourceUris": ["gs://slagoon/cate_data/cate_data.csv"],  # GCS 버킷 경로
                "destinationTable": {
                    "projectId": "rocket-448001",
                    "datasetId": "rocket6_project",
                    "tableId": "crawling1",
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_TRUNCATE",
                "fieldDelimiter": ",",
                "skipLeadingRows": 1,
                "autodetect": True,  # 스키마 자동 감지
            }
        },
        gcp_conn_id='gc',
    )

    # 태스크 순서 설정
    crawl_data_task >> upload_to_gcs_task >> load_to_bigquery_task
