from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.models import Variable

from io import StringIO
from datetime import datetime
from datetime import timedelta

import pandas as pd
import requests
import logging
import pendulum
import time

## 로컬 타임존 생성
local_tz = pendulum.timezone("Asia/Seoul")

SNOWFLAKE_CONN_ID = 'hunsoo_snowflake_conn'
SNOWFLAKE_STAGE = 'hunsoo_external_stage'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'HUNSOO'
SNOWFLAKE_SCHEMA = 'RAW_DATA'
SNOWFLAKE_TABLE = 'stock_prices_daily'
S3_FILE_NAME = 'get_stock_prices_daily.csv'


def create_url():
    BASE_URL = "https://openapi.koreainvestment.com:9443"
    PATH = '/uapi/domestic-stock/v1/quotations/inquire-daily-price'
    return f"{BASE_URL}/{PATH}"


def extract_task():
    logging.info("Starting data extraction process")
    APP_KEY = Variable.get("APP_KEY")
    APP_SECRET = Variable.get("APP_SECRET")
    ACCESS_TOKEN = Variable.get("ACCESS_TOKEN")

    max_attempts = 3  # 최대 시도 횟수
    attempt = 0  # 현재 시도 횟수
    URL = create_url()
    
    # 헤더 설정
    headers = {
        "Content-Type": "application/json", 
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey": APP_KEY,
        "appSecret": APP_SECRET,
        "tr_id": "FHKST01010400"}

    params = {
        "fid_cond_mrkt_div_code": "J",  # 주식
        "fid_input_iscd": '005380',     # 종목 코드(임시) stock_no
        "fid_period_div_code": "D",     # 최근 30거래일
        "fid_org_adj_prc": "0",         # 수정주가반영
    }

    logging.info("Requesting data from URL")
    
    while attempt < max_attempts:
        logging.info(f"{attempt+1}/{max_attempts} 시도")
        try:
            response = requests.get(URL, headers=headers, params=params)
            if response.status_code == 200 and response.json()["rt_cd"] == "0":
                logging.info("Data extraction successful")
                return response.json()
            else:
                print("요청 실패, 상태 코드:", response.status_code)
        except requests.exceptions.RequestException as e:
            print("HTTP 요청 중 오류 발생:", e)

        attempt += 1
        time.sleep(1)  # 1초 대기

    # 호출
    if attempt == max_attempts:
        logging.error("Data extraction failed")
        raise
            


def transform_task(**context):
    logging.info("Starting data transformation process")

    json_data = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    df = pd.DataFrame(json_data['output'])
    logging.info("Data extraction completed")

    df['stck_bsop_date'] = df['stck_bsop_date'].apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:]}")

    int_columns = ['stck_oprc', 'stck_hgpr', 'stck_lwpr', 'stck_clpr', 'acml_vol', 'prdy_vrss', 'frgn_ntby_qty']
    try:
        df[int_columns] = df[int_columns].astype(int)
        logging.info("Conversion to INT type completed for columns: %s", int_columns)
    except Exception as e:
        logging.error("Error in converting to INT type: %s", e)

    float_columns = ['prdy_vrss_vol_rate', 'prdy_ctrt', 'hts_frgn_ehrt', 'acml_prtt_rate']
    try:
        df[float_columns] = df[float_columns].astype(float)
        logging.info("Conversion to FLOAT type completed for columns: %s", float_columns)
    except Exception as e:
        logging.error("Error in converting to FLOAT type: %s", e)

    return df


def upload_df_to_s3(**context):
    bucket_name = context["params"]["bucket_name"]
    s3_key = context["params"]["s3_key"]

    df = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    
    try:
        hook = S3Hook(aws_conn_id='dev_course_s3_id')
        
        # 데이터프레임을 문자열 버퍼로 변환
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # S3에 업로드
        hook.load_string(string_data=csv_buffer.getvalue(), bucket_name=bucket_name, key=s3_key, replace=True)
        logging.info(f"File {s3_key} successfully uploaded to S3 bucket {bucket_name}")

        # 버퍼 메모리 해제
        del csv_buffer

    except Exception as e:
        logging.error(f"Error occurred while uploading file to S3: {e}")
        raise


dag = DAG(
    dag_id="get_stock_prices_daily",
    tags=['dev-course'],
    start_date=datetime(2024, 1, 9, tzinfo=local_tz),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'hunsoo',
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }
)

extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract_task,
    dag = dag
)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform_task,
    provide_context = True,
    dag = dag
)


upload_S3 = PythonOperator(
    task_id='upload_df_to_s3',
    python_callable=upload_df_to_s3,
    provide_context=True,
    params={
        'bucket_name': 'programmers-bucket',
        's3_key': 'hunsoo/get_stock_prices_daily.csv'
    },
    dag=dag
)

copy_into_table = S3ToSnowflakeOperator(
    task_id='copy_into_table',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    s3_keys=[S3_FILE_NAME],
    table=SNOWFLAKE_TABLE,
    schema=SNOWFLAKE_SCHEMA,
    stage=SNOWFLAKE_STAGE,
    file_format="(type = 'CSV', skip_header=1)",
    dag=dag,
)

extract >> transform >> upload_S3 >> copy_into_table