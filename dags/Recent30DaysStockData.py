from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.models import Variable

from io import StringIO
from datetime import datetime
from datetime import timedelta

import pandas as pd
import requests
import logging

SNOWFLAKE_CONN_ID = 'hunsoo_snowflake_conn'
SNOWFLAKE_STAGE = 'hunsoo_external_stage'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_DATABASE = 'HUNSOO'
SNOWFLAKE_SCHEMA = 'RAW_DATA'
SNOWFLAKE_TABLE = 'stock_prices_daily'
S3_FILE_PATH = 's3://programmers-bucket/hunsoo/get_stock_prices_daily.csv'


def create_url():
    BASE_URL = "https://openapi.koreainvestment.com:9443"
    PATH = '/uapi/domestic-stock/v1/quotations/inquire-daily-price'
    return f"{BASE_URL}/{PATH}"


def extract_task(ACCESS_TOKEN, API_KEY, API_SECRET):
    try:
        URL = create_url()
        
        # 헤더 설정
        headers = {
            "Content-Type": "application/json", 
            "authorization": f"Bearer {ACCESS_TOKEN}",
            "appKey": API_KEY,
            "appSecret": API_SECRET,
            "tr_id": "FHKST01010400"}

        params = {
            "fid_cond_mrkt_div_code": "J",  # 주식
            "fid_input_iscd": '005380',     # 종목 코드(임시) stock_no
            "fid_period_div_code": "D",     # 최근 30거래일
            "fid_org_adj_prc": "0",         # 수정주가반영
        }

        logging.info("Requesting data from URL")
        
        # 호출
        response = requests.get(URL, headers=headers, params=params)

        if response.status_code == 200 and response.json()["rt_cd"] == "0":  # 0: 성공
            json_data = response.json()
            df_data = pd.DataFrame(json_data['output'])
            logging.info("Data extraction successful")
            return (df_data)
        else:
            logging.error(f"Failed to retrieve data: {response.status_code}, {response.json()}")
            return None
            
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


def transform_task(**context):
    logging.info("Starting data transformation process")

    df = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
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


def upload_df_to_s3(bucket_name, s3_key, **context):
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
    start_date=datetime(2024, 1, 10),
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
    op_kwargs = {
        'ACCESS_TOKEN': Variable.get("ACCESS_TOKEN"),
        'API_KEY': Variable.get("API_KEY"),
        'API_SECRET': Variable.get("API_SECRET")
    },
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
    op_kwargs={
        'bucket_name': 'programmers-bucket',
        's3_key': 'hunsoo/get_stock_prices_daily.csv'
    },
    dag=dag
)

copy_into_table = S3ToSnowflakeOperator(
    task_id='copy_into_table',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    s3_keys=[S3_FILE_PATH],
    table=SNOWFLAKE_TABLE,
    schema=SNOWFLAKE_SCHEMA,
    stage=SNOWFLAKE_STAGE,
    file_format="(type = 'CSV', skip_header=1)",
    dag=dag,
)

extract >> transform >> upload_S3 >> copy_into_table