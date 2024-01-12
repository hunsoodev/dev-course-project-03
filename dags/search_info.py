from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import os

# 접속 및 정보등록
import boto3
import csv
import snowflake.connector

@task
def import_csv():
    dag_folder = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(dag_folder, 'stockcode.csv')  # CSV 파일 상대 경로
    stockcode = []
    with open(file_path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            stockcode.append(row.pop())
    return stockcode

@task
def extract_and_transform(KIS_BASE_URL, VOLUME_RANK_URL, headers, stockcode):
    logging.info(datetime.utcnow())
    list_data = []
    start = False
    for code in stockcode:
        params = {
            "PDNO": code,
            "PRDT_TYPE_CD": "300"
        }
        response = requests.get(
            KIS_BASE_URL + VOLUME_RANK_URL, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            print(data)
        else:
            continue

        output_data = data.get('output')
        if 'output' in data:
            if start == False:
                keys_list = list(output_data.keys()) 
                keys_list_without_comma = [element.replace(',', '') for element in keys_list]
                list_data.append(keys_list_without_comma)
                start = True
            print(list(output_data.values()))
            values_list = list(output_data.values())
            values_list_without_comma = [element.replace(',', '') for element in values_list]
            list_data.append(values_list_without_comma)
        else:
            continue
    return list_data

@task
def load_to_S3(aws_access_key_id, aws_secret_access_key,list_data):
    logging.info("load started")
    csv_file_path = "search_info.csv"

    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        for row in list_data:
            writer.writerow(row)

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    bucket_name = 'programmers-bucket'
    local_file_path = 'search_info.csv'
    s3_folder_key = 'minhoe/'
    s3_file_key = s3_folder_key + local_file_path

    s3.upload_file(local_file_path, bucket_name, s3_file_key)
    logging.info("load done")

@task
def s3_to_snowflake(aws_access_key_id,aws_secret_access_key):
    # Snowflake 관련 정보
    snowflake_account = Variable.get('snowflake_account')
    snowflake_user = Variable.get('snowflake_user')
    snowflake_password = Variable.get('snowflake_password')
    snowfalke_warehouse = 'COMPUTE_WH'
    snowflake_database = 'minhoe'
    snowflake_schema = 'raw_data'

    try:
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse=snowfalke_warehouse,
            database=snowflake_database,
            schema=snowflake_schema
            )
        cur = conn.cursor()
        
        sql_stages = f"""CREATE OR REPLACE STAGE MY_CSV_STAGE
            URL = 's3://programmers-bucket/minhoe/'
            CREDENTIALS = (
            AWS_KEY_ID = '{aws_access_key_id}',
            AWS_SECRET_KEY = '{aws_secret_access_key}')
            DIRECTORY = (ENABLE = TRUE);
            """

        sql_create_tables = f"""
            CREATE OR REPLACE TABLE MINHOE.RAW_DATA.SEARCH_INFO (
                pdno VARCHAR(16777216),
                prdt_type_cd VARCHAR(16777216),
                prdt_name VARCHAR(16777216),
                prdt_name120 VARCHAR(16777216),
                prdt_abrv_name VARCHAR(16777216),
                prdt_eng_name VARCHAR(16777216),
                prdt_eng_name120 VARCHAR(16777216),
                prdt_eng_abrv_name VARCHAR(16777216),
                std_pdno VARCHAR(16777216),
                shtn_pdno VARCHAR(16777216),
                prdt_sale_stat_cd VARCHAR(16777216),
                prdt_risk_grad_cd VARCHAR(16777216),
                prdt_cls_cd VARCHAR(16777216),
                prdt_cls_name VARCHAR(16777216),
                sale_strt_dt VARCHAR(16777216),
                sale_end_dt VARCHAR(16777216),
                wrap_asst_type_cd VARCHAR(16777216),
                ivst_prdt_type_cd VARCHAR(16777216),
                ivst_prdt_type_cd_name VARCHAR(16777216),
                frst_erlm_dt VARCHAR(16777216)
            );
        """
        sql_copy = f"""
                COPY INTO MINHOE.RAW_DATA.SEARCH_INFO
                FROM @MY_CSV_STAGE
                FILES = ('search_info.csv')
                FILE_FORMAT = (TYPE = CSV skip_header=1);
        """
        cur.execute(sql_stages)
        cur.execute(sql_create_tables)
        cur.execute(sql_copy)
        cur.execute('COMMIT')
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logging.error(e)
        raise
    
    
with DAG(
    dag_id='search_info',
    start_date=datetime(2022, 10, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 2 * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=0),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    KIS_BASE_URL = "https://openapi.koreainvestment.com:9443"
    VOLUME_RANK_URL = "/uapi/domestic-stock/v1/quotations/chk-holiday"
    APP_KEY = Variable.get("APP_KEY")
    APP_SECRET = Variable.get("APP_SECRET")
    ACCESS_TOKEN = Variable.get("ACCESS_TOKEN")

    headers = {
        'content-type': 'application/json; charset=utf-8',
        'authorization': f"Bearer {ACCESS_TOKEN}",
        'appkey': APP_KEY,
        'appsecret': APP_SECRET,
        'tr_id': 'CTPF1604R',
        'tr_cont': 'N',
        'custtype': 'P'
    }

    stockcode = import_csv()
    list_data = extract_and_transform(
        KIS_BASE_URL, VOLUME_RANK_URL, headers, stockcode)
    aws_access_key_id = Variable.get('aws_access_key_id')
    aws_secret_access_key = Variable.get('aws_secret_access_key')
    load_to_S3(aws_access_key_id, aws_secret_access_key, list_data) >> s3_to_snowflake(aws_access_key_id,aws_secret_access_key)
