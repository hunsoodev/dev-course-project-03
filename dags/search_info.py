from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import os

import boto3
import csv
import snowflake.connector

@task
def import_csv():
    dag_folder = os.path.dirname(os.path.realpath(__file__))
    file_path = os.path.join(dag_folder, 'stockcode.csv')
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
            create or replace TABLE MINHOE.RAW_DATA.SEARCH_INFO (
                PDNO VARCHAR(16777216),
                PRDT_TYPE_CD NUMBER(38,0),
                PRDT_NAME VARCHAR(16777216),
                PRDT_NAME120 VARCHAR(16777216),
                PRDT_ABRV_NAME VARCHAR(16777216),
                PRDT_ENG_NAME VARCHAR(16777216),
                PRDT_ENG_NAME120 VARCHAR(16777216),
                PRDT_ENG_ABRV_NAME VARCHAR(16777216),
                STD_PDNO VARCHAR(16777216),
                SHTN_PDNO NUMBER(38,0),
                PRDT_SALE_STAT_CD VARCHAR(16777216),
                PRDT_RISK_GRAD_CD VARCHAR(16777216),
                PRDT_CLSF_CD NUMBER(38,0),
                PRDT_CLSF_NAME VARCHAR(16777216),
                SALE_STRT_DT VARCHAR(16777216),
                SALE_END_DT VARCHAR(16777216),
                WRAP_ASST_TYPE_CD NUMBER(38,0),
                IVST_PRDT_TYPE_CD NUMBER(38,0),
                IVST_PRDT_TYPE_CD_NAME VARCHAR(16777216),
                FRST_ERLM_DT VARCHAR(16777216)
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
    start_date=datetime(2023, 12, 1),  
    schedule='0 2 * * *', 
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=0),
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
