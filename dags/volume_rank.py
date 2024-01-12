from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator

from datetime import datetime
import requests
from io import StringIO
import pandas as pd
from datetime import timedelta


default_args = {
    'owner': 'yein',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('volume_rank_to_snowflake',
          default_args=default_args,
          schedule='* * * * *',
          catchup=False)


def get_volume_rank_data(**context):
    KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
    VOLUME_RANK_URL = "/uapi/domestic-stock/v1/quotations/volume-rank"
    APP_KEY = Variable.get("APP_KEY")
    APP_SECRET = Variable.get("APP_SECRET")
    ACCESS_TOKEN = Variable.get("ACCESS_TOKEN")

    headers = {
        'content-type': 'application/json; charset=utf-8',
        'authorization': f"Bearer {ACCESS_TOKEN}",
        'appkey': APP_KEY,
        'appsecret': APP_SECRET,
        'tr_id': 'FHPST01710000',
        'tr_cont': 'N',
        'custtype': 'P'
    }

    params = {
        'FID_COND_MRKT_DIV_CODE': 'J',
        'FID_COND_SCR_DIV_CODE': '20171',
        'FID_INPUT_ISCD': '0000',
        'FID_DIV_CLS_CODE': '0',
        'FID_BLNG_CLS_CODE': '0',
        'FID_TRGT_CLS_CODE': '111111111',
        'FID_TRGT_EXLS_CLS_CODE': '000000',
        'FID_INPUT_PRICE_1': '',
        'FID_INPUT_PRICE_2': '',
        'FID_VOL_CNT': '',
        'FID_INPUT_DATE_1': ''
    }

    response = requests.get(KIS_BASE_URL + VOLUME_RANK_URL, headers=headers, params=params)

    if response.status_code != 200:
        raise Exception(f"API Request Failed: {response.status_code}")

    data = response.json()
    output = data.get('output', [])

    if not output:
        raise Exception("No data available for CSV conversion")

    df = pd.DataFrame(output)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    csv_data = csv_buffer.getvalue()

    execution_date = context['execution_date']
    formatted_date_time = execution_date.strftime('%Y-%m-%d-%H-%M-%S')

    s3_hook = S3Hook(aws_conn_id='aws_s3_conn_id')
    s3_hook.load_string(
        string_data=csv_data,
        key=f'yein/volume-rank/{formatted_date_time}.csv',
        bucket_name='programmers-bucket',
        replace=True
    )

    return f'{formatted_date_time}.csv'


get_volume_rank_data_task = PythonOperator(
    task_id='get_volume_rank_data',
    python_callable=get_volume_rank_data,
    provide_context=True,
    dag=dag,
)

bulk_update_snowflake = S3ToSnowflakeOperator(
    task_id='bulk_update_snowflake',
    s3_keys=["{{ task_instance.xcom_pull(task_ids='get_volume_rank_data') }}"],
    stage='volume_rank',
    table='volume_rank',
    file_format='SKIP_HEADER_AND_ADD_CREATED_AT',
    snowflake_conn_id='snowflake_conn_id',
    dag=dag
)


get_volume_rank_data_task >> bulk_update_snowflake
