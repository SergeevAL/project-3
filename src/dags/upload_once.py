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
    'depends_on_past':True,
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

list_tables_path = {}

### Logging

def logging(logrecord, schema, log_table):
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df=pd.DataFrame(logrecord,index=[0])
    df.to_sql(log_table, engine, index=False, if_exists='append', schema=schema)

### Upload in stage

def upload_once():
    print('Making request generate_report')
    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    print(f'Response is {response.content}')

    print('Making request get_report')
    report_id = None

    for i in range(30):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            list_tables_path = json.loads(response.content)['data']['s3_path']
            logging(
                {'proc_name':'Generate and get report',
                'status':'Success',
                'msg':f'We get report: {response.content}'},
                'staging',
                'events_log')
            break
        else:
            time.sleep(10)

    if not report_id:
        logging(
        {'proc_name':'Generate and get report',
        'status':'Error',
        'msg':'TimeOut Error'},
        'staging',
        'events_log'
    )
    else:
        for item in (list_tables_path):
            print(f'Processing: {item}')
            try:    
                current = time.time()
                df = pd.read_csv(list_tables_path[item])
                postgres_hook = PostgresHook(postgres_conn_id)
                engine = postgres_hook.get_sqlalchemy_engine()
                df.to_sql(item, engine, index=False, if_exists='replace', schema='staging')
                end = time.time()
                logging(
                    {'proc_name':'Upload',
                    'status':'Success',
                    'target_table': item,
                    'source_file': list_tables_path[item],
                    'duration_ms': (end - current) * 1000,
                    'rows': len(df),
                    'msg':f'Upload from {list_tables_path[item]} to staging.{item}'},
                    'staging',
                    'events_log'
                )
            except Exception:
                logging(
                    {'proc_name':'Upload',
                    'status':'Error',
                    'target_table': item,
                    'source_file': list_tables_path[item],
                    'msg':traceback.format_exc()},
                    'staging',
                    'events_log'
                )
                
### Upload in mart

def upload_d_items():
    try:
        print('Upload d_items')
        current = time.time()
        postgres_hook = PostgresHook(postgres_conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        file = open('/lessons/dags/sql/mart.d_item.sql')
        query = sqlalchemy.text(file.read())
        result_proxy=engine.execute(query)
        end = time.time()
        logging(
            {'status':'Success',
            'target_table': 'd_item',
            'source_table': 'user_orders_log',
            'duration_ms': (end - current) * 1000,
            'rows': result_proxy.rowcount,
            'msg':f'Upload from staging to mart: d_item'},
            'mart',
            'load_history',
        )
    except Exception:
        logging(
            {'status':'Error',
            'target_table': 'd_item',
            'source_table': 'user_orders_log',
            'msg':traceback.format_exc()},
            'mart',
            'load_history',
        )
        print(traceback.format_exc())

def upload_d_customer():
    try:
        print('Upload upload_d_customer')
        current = time.time()
        postgres_hook = PostgresHook(postgres_conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        file = open('/lessons/dags/sql/mart.d_customer.sql')
        query = sqlalchemy.text(file.read())
        result_proxy=engine.execute(query)
        end = time.time()
        logging(
            {'status':'Success',
            'target_table': 'd_customer',
            'source_table': 'user_orders_log',
            'duration_ms': (end - current) * 1000,
            'rows': result_proxy.rowcount,
            'msg':f'Upload from staging to mart: d_customer'},
            'mart',
            'load_history',
        )
    except Exception:
        logging(
            {'status':'Error',
            'target_table': 'd_customer',
            'source_table': 'user_orders_log',
            'msg':traceback.format_exc()},
            'mart',
            'load_history',
        )
        print(traceback.format_exc())


def upload_d_city():
    try:
        print('Upload d_city')
        current = time.time()
        postgres_hook = PostgresHook(postgres_conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        file = open('/lessons/dags/sql/mart.d_city.sql')
        query = sqlalchemy.text(file.read())
        result_proxy=engine.execute(query)
        end = time.time()
        logging(
            {'status':'Success',
            'target_table': 'd_city',
            'source_table': 'user_orders_log',
            'duration_ms': (end - current) * 1000,
            'rows': result_proxy.rowcount,
            'msg':f'Upload from staging to mart: d_city'},
            'mart',
            'load_history',
        )
    except Exception:
        logging(
            {'status':'Error',
            'target_table': 'd_city',
            'source_table': 'user_orders_log',
            'msg':traceback.format_exc()},
            'mart',
            'load_history',
        )
        print(traceback.format_exc())


def upload_f_sales():
    try:
        print('Upload f_sales')
        current = time.time()
        postgres_hook = PostgresHook(postgres_conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        file = open('/lessons/dags/sql/mart.f_sales.sql')
        query = sqlalchemy.text(file.read())
        result_proxy=engine.execute(query)
        end = time.time()
        logging(
            {'status':'Success',
            'target_table': 'f_sales',
            'source_table': 'user_orders_log',
            'duration_ms': (end - current) * 1000,
            'rows': result_proxy.rowcount,
            'msg':f'Upload from staging to mart: f_sales'},
            'mart',
            'load_history',
        )
    except Exception:
        logging(
            {'status':'Error',
            'target_table': 'f_sales',
            'source_table': 'user_orders_log',
            'msg':traceback.format_exc()},
            'mart',
            'load_history',
        )
        print(traceback.format_exc())

def upload_f_customer_retention():
    try:
        print('Upload f_customer_retention')
        current = time.time()
        postgres_hook = PostgresHook(postgres_conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        file = open('/lessons/dags/sql/mart.f_customer_retention.sql')
        query = sqlalchemy.text(file.read())
        result_proxy=engine.execute(query)
        end = time.time()
        logging(
            {'status':'Success',
            'target_table': 'f_customer_retention',
            'source_table': 'f_sales',
            'duration_ms': (end - current) * 1000,
            'rows': result_proxy.rowcount,
            'msg':f'Upload from f_sales to f_customer_retention'},
            'mart',
            'load_history',
        )
    except Exception:
        logging(
            {'status':'Error',
            'target_table': 'f_customer_retention',
            'source_table': 'f_sales',
            'msg':traceback.format_exc()},
            'mart',
            'load_history',
        )
        print(traceback.format_exc())

###########


###########

with DAG(
        'upload_once',
        default_args=args,
        description='Upload data from api',
        catchup=True,
        schedule_interval=None,
        start_date=datetime.today() - timedelta(days=8),
) as dag:
    upload_once = PythonOperator(
        task_id='upload_once',
        python_callable=upload_once,
    )
    upload_d_items = PythonOperator(
        task_id='upload_d_items',
        python_callable=upload_d_items,
    )
    upload_d_customer = PythonOperator(
        task_id='upload_d_customer',
        python_callable=upload_d_customer,
    )
    upload_d_city = PythonOperator(
        task_id='upload_d_city',
        python_callable=upload_d_city,
    )
    upload_f_sales = PythonOperator(
        task_id='upload_f_sales',
        python_callable=upload_f_sales,
    )
    upload_f_customer_retention = PythonOperator(
        task_id='upload_f_customer_retention',
        python_callable=upload_f_customer_retention,
    )

upload_once >> [upload_d_items,upload_d_customer,upload_d_city] >> upload_f_sales >> upload_f_customer_retention
