from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import pandas as pd
import logging
import os
import FinanceDataReader as fdr
import time
import boto3
import snowflake.connector
import pendulum

# API 관련 정보
API_APP_KEY = Variable.get("API_APP_KEY")
API_APP_SECRET = Variable.get("API_APP_SECRET")
API_ACCESS_TOKEN = Variable.get("API_ACCESS_TOKEN")
KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
VOLUME_RANK_URL = "/uapi/domestic-stock/v1/quotations/inquire-price"
col_list = ['iscd_stat_cls_code', 'marg_rate', 'rprs_mrkt_kor_name', 'bstp_kor_isnm', 'temp_stop_yn', 'oprc_rang_cont_yn', 'clpr_rang_cont_yn', 'crdt_able_yn', 'grmn_rate_cls_code', 'elw_pblc_yn', 'stck_prpr', 'prdy_vrss', 'prdy_vrss_sign', 'prdy_ctrt', 'acml_tr_pbmn', 'acml_vol', 'prdy_vrss_vol_rate', 'stck_oprc', 'stck_hgpr', 'stck_lwpr', 'stck_mxpr', 'stck_llam', 'stck_sdpr', 'wghn_avrg_stck_prc', 'hts_frgn_ehrt', 'frgn_ntby_qty', 'pgtr_ntby_qty', 'pvt_scnd_dmrs_prc', 'pvt_frst_dmrs_prc', 'pvt_pont_val', 'pvt_frst_dmsp_prc', 'pvt_scnd_dmsp_prc', 'dmrs_val', 'dmsp_val', 'cpfn', 'rstc_wdth_prc', 'stck_fcam', 'stck_sspr', 'aspr_unit', 'hts_deal_qty_unit_val', 'lstn_stcn', 'hts_avls', 'per', 'pbr', 'stac_month', 'vol_tnrt', 'eps', 'bps', 'd250_hgpr', 'd250_hgpr_date', 'd250_hgpr_vrss_prpr_rate', 'd250_lwpr', 'd250_lwpr_date', 'd250_lwpr_vrss_prpr_rate', 'stck_dryy_hgpr', 'dryy_hgpr_vrss_prpr_rate', 'dryy_hgpr_date', 'stck_dryy_lwpr', 'dryy_lwpr_vrss_prpr_rate', 'dryy_lwpr_date', 'w52_hgpr', 'w52_hgpr_vrss_prpr_ctrt', 'w52_hgpr_date', 'w52_lwpr', 'w52_lwpr_vrss_prpr_ctrt', 'w52_lwpr_date', 'whol_loan_rmnd_rate', 'ssts_yn', 'stck_shrn_iscd', 'fcam_cnnm', 'cpfn_cnnm', 'frgn_hldn_qty', 'vi_cls_code', 'ovtm_vi_cls_code', 'last_ssts_cntg_qty', 'invt_caful_yn', 'mrkt_warn_cls_code', 'short_over_yn', 'sltr_yn', 'iscd', 'created_at']

# AWS S3 관련 정보
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
s3_bucket = 'programmers-bucket'

# Snowflake 관련 정보
snowflake_account = Variable.get("Snowflake_ACCOUNT")
snowflake_user = Variable.get("Snowflake_USER")
snowflake_password = Variable.get("Snowflake_PASSWORD")
snowfalke_warehouse = 'COMPUTE_WH'
snowflake_database = 'jaeho'
snowflake_schema = 'raw_data'
snowflake_table = 'inquire_price'
snowflake_stage_table = 'stage_inquire_price'

# timezone 설정.
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 1, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id='test_inquire_price',
    default_args=default_args,
    description='주식현재가 시세 관련 dag',
    schedule='0 9-15 * * 1-5',
    catchup=True,
)

# API에서 데이터를 읽어와서 csv 파일로 저장하는 함수.
def fetch_data_and_load_as_csv(**kwargs):
    execution_date = kwargs['execution_date']
    csv_file_path = f'inquire_price_data_{execution_date}.csv'
    s3_key = f'jaeho/{csv_file_path}'
    
    kwargs['ti'].xcom_push(key='s3_key', value=s3_key)
    kwargs['ti'].xcom_push(key='csv_file_path', value=csv_file_path)
    
    headers = {
        'content-type': 'application/json; charset=utf-8',
        'authorization': f"Bearer {API_ACCESS_TOKEN}",
        'appkey': API_APP_KEY,
        'appsecret': API_APP_SECRET,
        'tr_id': 'FHKST01010100',
        'tr_cont': 'N',
        'custtype': 'P'
    }
    # 종목 코드 목록.
    df_krx = fdr.StockListing('KRX')
    fid_input_iscd_list = list(df_krx['Code'])
    try:
        print("주식현재가 시세에 대한 API 호출을 시작합니다.")
        print('execution_date:', execution_date)
        print('s3_key:', s3_key)
        output_data = {}
        row_list = []
        
        for i in range(len(fid_input_iscd_list)):
            time.sleep(0.05)
            params = {
            'FID_COND_MRKT_DIV_CODE': 'J',
            'FID_INPUT_ISCD': fid_input_iscd_list[i],
            }
            response = requests.get(KIS_BASE_URL + VOLUME_RANK_URL, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                output_data = data['output']
                stock_info = list(output_data.values())
                if len(stock_info) <= 79:
                    stock_info.append(fid_input_iscd_list[i])
                    stock_info.append(execution_date)
                    print(i, len(stock_info))
                    row_list.append(stock_info)
            else:
                logging.error("ERROR : API response error")
        # data -> csv
        df = pd.DataFrame(row_list, columns=col_list)
        df.replace({' ': 'null'}, inplace=True)
        df.replace({'': 'null'}, inplace=True)
        df = df.applymap(lambda x: str(x).replace(',', ''))
        df.to_csv(csv_file_path, index=False)
        print(f'파일 {csv_file_path}를 로컬 디렉토리에 저장하였습니다')
        
    except Exception as e:
        logging.error(e)
        raise

# csv -> S3 로드하는 함수
def upload_to_s3(**kwargs):
    try:
        ti = kwargs['ti']
        csv_file_path = ti.xcom_pull(task_ids='fetch_data_and_load_as_csv', key='csv_file_path')
        s3_key = ti.xcom_pull(task_ids='fetch_data_and_load_as_csv', key='s3_key')
        s3_hook = S3Hook(aws_conn_id='aws_s3_conn_id')
        s3_hook.load_file(
            filename=csv_file_path,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True
        )
        print(f'파일 {csv_file_path}를 {s3_bucket}/{s3_key}(으)로 업로드하였습니다.')
    except Exception as e:
        logging.error(e)
        raise

# local directory에서 csv 파일 삭제하는 함수
def remove_csv(**kwargs):
    try:
        ti = kwargs['ti']
        csv_file_path = ti.xcom_pull(task_ids='fetch_data_and_load_as_csv', key='csv_file_path')
        os.remove(csv_file_path)
        print(f'파일 {csv_file_path}를 local directory에서 삭제하였습니다.')
    except Exception as e:
        logging.error(e)
        raise

# S3 -> Snowflake로 적재하는 함수
def s3_to_snowflake(**kwargs):
    try:
        ti = kwargs['ti']
        s3_key = ti.xcom_pull(task_ids='fetch_data_and_load_as_csv', key='s3_key')
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse=snowfalke_warehouse,
            database=snowflake_database,
            schema=snowflake_schema
            )
        cur = conn.cursor()
        sql_cpoy = f"""
            COPY INTO {snowflake_database}.{snowflake_schema}.{snowflake_stage_table}
            FROM 's3://{s3_bucket}/{s3_key}'
            credentials=(AWS_KEY_ID='{AWS_ACCESS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_ACCESS_KEY}')
            FILE_FORMAT = (type = 'CSV' skip_header=1, error_on_column_count_mismatch=false);
        """
        sql_insert = f"""
            INSERT INTO {snowflake_database}.{snowflake_schema}.{snowflake_table}
            SELECT * FROM {snowflake_database}.{snowflake_schema}.{snowflake_stage_table};
        """
        cur.execute(sql_cpoy)
        cur.execute(sql_insert)
        cur.execute('COMMIT')
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logging.error(e)
        raise
    
# API에서 데이터를 읽어와서 csv 파일로 저장하는 Task
fetch_data_task = PythonOperator(
    task_id='fetch_data_and_load_as_csv',
    python_callable=fetch_data_and_load_as_csv,
    provide_context=True,
    dag=dag,
)

# csv 파일을 S3에 업로드하는 Task
upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    provide_context=True,
    dag=dag,
)

# 로컬에 저장된 csv 파일을 삭제하는 Task
remove_csv_task = PythonOperator(
    task_id='remove_csv',
    python_callable=remove_csv,
    provide_context=True,
    dag=dag,
)

# S3에 업로드된 데이터를 Snowflake로 적재하는 Task
s3_to_snowflake_task = PythonOperator(
    task_id='s3_to_snowflake',
    python_callable=s3_to_snowflake,
    provide_context=True,
    dag=dag,
)

# Task 간의 의존성 설정
fetch_data_task >> upload_to_s3_task >> remove_csv_task >> s3_to_snowflake_task