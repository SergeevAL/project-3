import time
import requests
import json
import traceback
import pandas as pd
import sqlalchemy

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host
postgres_conn_id = 'postgresql_de'

nickname = 'serg3eva0992'
cohort = '2'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

def logging(logrecord, schema, log_table):
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df=pd.DataFrame(logrecord,index=[0])
    df.to_sql(log_table, engine, index=False, if_exists='append', schema=schema)

def create_staging():
    print('Init schema Staging')
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    file = open('/lessons/dags/sql/create_staging.sql')
    query = sqlalchemy.text(file.read())
    try:
        engine.execute(query)
        logging(
            {'proc_name':'Create',
            'msg':'Init schema Staging'},
            'staging',
            'events_log'
        )
    except Exception:
        print(traceback.format_exc())
    finally:
        file.close()

def create_mart():
    print('Init schema Mart')
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    file = open('/lessons/dags/sql/create_mart.sql')
    query = sqlalchemy.text(file.read())
    try:
        engine.execute(query)
    except Exception:
        print(traceback.format_exc())
    finally:
        file.close()

def generate_d_calendar():

    print('Making d_calendar')
    current = time.time()
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    file = open('/lessons/dags/sql/mart.d_calendar.sql')
    try:    
        query = sqlalchemy.text(file.read())
        result_proxy=engine.execute(query)
        end = time.time()
        logging(
            {'target_table': 'd_calendar',
            'source_table': '',
            'duration_ms': (end - current) * 1000,
            'rows': result_proxy.rowcount,
            'msg':f'Generate d_calendar'},
            'mart',
            'load_history',
        )
    except Exception:
        logging(
            {'status':'Error',
            'target_table': 'd_calendar',
            'msg':traceback.format_exc()},
            'mart',
            'load_history',
        )
   

###########


###########

with DAG(
        'create_schemas',
        default_args=args,
        description='Create schema staging and mart',
        catchup=True,
        schedule_interval=None,
        start_date=datetime.today() - timedelta(days=8),
) as dag:
    create_staging = PythonOperator(
        task_id='create_staging',
        python_callable=create_staging,
    )

    create_mart = PythonOperator(
        task_id='create_mart',
        python_callable=create_mart,
    )

    generate_d_calendar = PythonOperator(
        task_id='generate_d_calendar',
        python_callable=generate_d_calendar,
    )

{
create_staging 
>> create_mart 
>> generate_d_calendar
}
