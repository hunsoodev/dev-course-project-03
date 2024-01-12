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
        params =  {
                "fid_input_iscd": code,
        }

        response = requests.get(KIS_BASE_URL + VOLUME_RANK_URL, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            print(data)
        else:
            continue
        
        output_data = data.get('output')
        if 'output' in data:
            if len(output_data) == 0:
                continue
            temp = output_data[0]
            if start == False:
                keys = list(temp.keys())
                row = ['stockcode']
                result = row + keys
                list_data.append(result)
                start = True
            values = list(temp.values())
            row = [code]
            result = row + values
            list_data.append(result)
        else:
            continue
    return list_data

@task
def load_to_S3(aws_access_key_id, aws_secret_access_key,list_data):
    logging.info("load started")
    csv_file_path = "program_trade_by_stock.csv"

    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        for row in list_data:
            writer.writerow(row)

    # S3 클라이언트 생성
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    bucket_name = 'programmers-bucket'
    local_file_path = 'program_trade_by_stock.csv'
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
            create or replace TABLE MINHOE.RAW_DATA.PROGRAM_TRADE_BY_STOCK (
                STOCKCODE NUMBER(38,0),
                BSOP_HOUR NUMBER(38,0),
                STCK_PRPR NUMBER(38,0),
                PRDY_VRSS NUMBER(38,0),
                PRDY_VRSS_SIGN NUMBER(38,0),
                PRDY_CTRT NUMBER(38,0),
                ACML_VOL NUMBER(38,0),
                WHOL_SMTN_SELN_VOL NUMBER(38,0),
                WHOL_SMTN_SHNU_VOL NUMBER(38,0),
                WHOL_SMTN_NTBY_QTY NUMBER(38,0),
                WHOL_SMTN_SELN_TR_PBMN NUMBER(38,0),
                WHOL_SMTN_SHNU_TR_PBMN NUMBER(38,0),
                WHOL_SMTN_NTBY_TR_PBMN NUMBER(38,0),
                WHOL_NTBY_VOL_ICDC NUMBER(38,0),
                WHOL_NTBY_TR_PBMN_ICDC NUMBER(38,0)
            );
        """
        sql_copy = f"""
                COPY INTO MINHOE.RAW_DATA.PROGRAM_TRADE_BY_STOCK
                FROM @MY_CSV_STAGE
                FILES = ('program_trade_by_stock.csv')
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
    dag_id='program_trade_by_stock',
    start_date=datetime(2022, 10, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 2 * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
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
        'tr_id': 'FHPPG04650100',
        'tr_cont': 'N',
        'custtype': 'P'
    }

    stockcode = import_csv()
    list_data = extract_and_transform(
        KIS_BASE_URL, VOLUME_RANK_URL, headers, stockcode)
    aws_access_key_id = Variable.get('aws_access_key_id')
    aws_secret_access_key = Variable.get('aws_secret_access_key')
    load_to_S3(aws_access_key_id, aws_secret_access_key, list_data) >> s3_to_snowflake(aws_access_key_id,aws_secret_access_key)
