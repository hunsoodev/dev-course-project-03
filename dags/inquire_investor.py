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

dag = DAG('inquire_investor_to_snowflake',
          default_args=default_args,
          schedule='* * * * *',
          catchup=False)


def get_inquire_investor_data(**context):
    KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
    INQUIRE_INVESTOR_URL = "/uapi/domestic-stock/v1/quotations/inquire-investor"
    APP_KEY = Variable.get("APP_KEY")
    APP_SECRET = Variable.get("APP_SECRET")
    ACCESS_TOKEN = Variable.get("ACCESS_TOKEN")

    headers = {
        'content-type': 'application/json; charset=utf-8',
        'authorization': f"Bearer {ACCESS_TOKEN}",
        'appkey': APP_KEY,
        'appsecret': APP_SECRET,
        'tr_id': 'FHKST01010900',
        'tr_cont': 'N',
        'custtype': 'P'
    }

    params = {
        'FID_COND_MRKT_DIV_CODE': 'J',
        'FID_INPUT_ISCD': '005930'
    }

    response = requests.get(KIS_BASE_URL + INQUIRE_INVESTOR_URL, headers=headers, params=params)

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
        key=f'yein/inquire-investor/{formatted_date_time}.csv',
        bucket_name='programmers-bucket',
        replace=True
    )

    return f'{formatted_date_time}.csv'


get_inquire_investor_data = PythonOperator(
    task_id='get_inquire_investor_data',
    python_callable=get_inquire_investor_data,
    provide_context=True,
    dag=dag,
)

bulk_update_snowflake = S3ToSnowflakeOperator(
    task_id='bulk_update_snowflake',
    s3_keys=["{{ task_instance.xcom_pull(task_ids='get_inquire_investor_data') }}"],
    stage='inquire_investor',
    table='inquire_investor',
    file_format='CSV_SKIP_HEADER',
    snowflake_conn_id='snowflake_conn_id',
    dag=dag
)


get_inquire_investor_data >> bulk_update_snowflake
