import time
import datetime
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


def logging(logrecord, schema, log_table):
    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df=pd.DataFrame(logrecord,index=[0])
    df.to_sql(log_table, engine, index=False, if_exists='append', schema=schema)

def upload_inc(ds):
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
            logging(
                {'proc_name':'Generate and get report',
                'status':'Success',
                'msg':f'We get report for increment: {response.content}'},
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
        start_date = datetime.strptime(ds, '%Y-%m-%d')
        start_date = datetime.strftime(start_date.date(), "%Y-%m-%dT%H:%M:%S")
        response = requests.get(f'{base_url}/get_increment?report_id={report_id}&date={start_date}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']

        if status == 'SUCCESS':
            target_date = json.loads(response.content)['date']
            list_tables_path = json.loads(response.content)['data']['s3_path']

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
                        {'proc_name':'Upload Increment',
                        'status':'Success',
                        'target_table': item,
                        'source_file': list_tables_path[item],
                        'duration_ms': (end - current) * 1000,
                        'rows': len(df),
                        'msg':f'Upload increment from {list_tables_path[item]} to staging.{item}',
                        'target_date': target_date},
                        'staging',
                        'events_log'
                    )
                except Exception:
                    logging(
                        {'proc_name':'Upload increment',
                        'status':'Error',
                        'target_table': item,
                        'source_file': list_tables_path[item],
                        'msg':traceback.format_exc(),
                        'target_date': target_date},
                        'staging',
                        'events_log'
                    )
        else:
            logging(
                {'proc_name':'Upload increment',
                'status':'Error',
                'msg':f'{response.content}',
                'target_date': start_date},
                'staging',
                'events_log'
            )



def upload_d_items(ds):
    try:
        print('Upload increment for d_items')
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
            'msg':f'Upload from staging to mart: d_item',
            'target_date': ds},
            'mart',
            'load_history',
        )
    except Exception:
        print(traceback.format_exc())
        logging(
            {'status':'Error',
            'target_table': 'd_item',
            'source_table': 'user_orders_log',
            'msg':traceback.format_exc(),
            'target_date': ds},
            'mart',
            'load_history',
        )


def upload_d_customer(ds):
    try:
        print('Upload increment for d_customer')
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
            'msg':f'Upload from staging to mart: d_customer',
            'target_date': ds},
            'mart',
            'load_history',
        )
    except Exception:
        print(traceback.format_exc())
        logging(
            {'status':'Error',
            'target_table': 'd_customer',
            'source_table': 'user_orders_log',
            'msg':traceback.format_exc(),
            'target_date': ds},
            'mart',
            'load_history',
        )


def upload_d_city(ds):
    try:
        print('Upload increment for d_city')
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
            'msg':f'Upload from staging to mart: d_city',
            'target_date': ds},
            'mart',
            'load_history',
        )
    except Exception:
        print(traceback.format_exc())
        logging(
            {'status':'Error',
            'target_table': 'd_city',
            'source_table': 'user_orders_log',
            'msg':traceback.format_exc(),
            'target_date': ds},
            'mart',
            'load_history',
        )


def upload_f_sales(ds):
    try:
        print('Upload increment f_sales')
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
            'msg':f'Upload from staging to mart: f_sales',
            'target_date': ds},
            'mart',
            'load_history',
        )
    except Exception:
        print(traceback.format_exc())
        logging(
            {'status':'Error',
            'target_table': 'f_sales',
            'source_table': 'user_orders_log',
            'msg':traceback.format_exc(),
            'target_date': ds},
            'mart',
            'load_history',
        )

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
        'upload_inc',
        default_args=args,
        description='Upload increment data from api',
        catchup=False,
        schedule_interval='@daily',
        start_date=datetime(2022,5,20),
        end_date=datetime(2022,6,11)
) as dag:
    upload_inc = PythonOperator(
        task_id='upload_inc',
        python_callable=upload_inc,
        op_kwargs={'ds': '{{ ds }}'},
    )
    upload_d_items = PythonOperator(
        task_id='upload_d_items',
        python_callable=upload_d_items,
        op_kwargs={'ds': '{{ ds }}'},
    )
    upload_d_customer = PythonOperator(
        task_id='upload_d_customer',
        python_callable=upload_d_customer,
        op_kwargs={'ds': '{{ ds }}'},
    )
    upload_d_city = PythonOperator(
        task_id='upload_d_city',
        python_callable=upload_d_city,
        op_kwargs={'ds': '{{ ds }}'},
    )
    upload_f_sales = PythonOperator(
        task_id='upload_f_sales',
        python_callable=upload_f_sales,
        op_kwargs={'ds': '{{ ds }}'},
    )
    upload_f_customer_retention = PythonOperator(
        task_id='upload_f_customer_retention',
        python_callable=upload_f_customer_retention,
    )

upload_inc >> [upload_d_items,upload_d_customer,upload_d_city] >> upload_f_sales >> upload_f_customer_retention
