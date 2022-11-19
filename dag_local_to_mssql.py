from airflow.models import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import pendulum
import logging

#create table
sql_create = """
    IF OBJECT_ID('dbo.product_list', 'U') IS NOT NULL DROP TABLE dbo.product_list
    CREATE TABLE dbo.product_list(
    brand_name Nvarchar(100), 
    product_name Nvarchar(100),
    product_code Nvarchar(100),
    price int,
    update_dt DATETIME2,
    PRIMARY KEY(product_code)
    )
    """

#insert data
#add GETDATE() to the end of query to record the current timestamp while inserting data
sql_insert = f"""
    INSERT INTO dbo.product_list
    VALUES (%s, %s, %s,%s, GETDATE())
    """

def create_table(mssql_conn_id:str,database:str,sql:str) -> None:
    """
    create table at mssql
    """
    logging.info('Time now: ' + datetime.now(pendulum.timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S"))
    mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema=database)
    conn = mssql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql.encode('utf8'))
    conn.commit()
    cursor.close()
    conn.close()

def insert_data(mssql_conn_id:str,database:str,csv_path:str, sql:str) -> None:
    """
    load the local csv data into the target table at mssql
    """
    import pandas as pd
    import numpy as np

    #load csv data into dataframe
    df = pd.read_csv(csv_path,encoding='utf-8',sep='|',dtype=str)
    #change null values to None in order to load into mssql
    df1 = df.replace({np.nan: None})
    #dump data values into list
    records = list(df1.itertuples(index=False, name=None))
    logging.info("data ready for insert.")

    mssql_hook = MsSqlHook(mssql_conn_id=mssql_conn_id, schema=database)
    conn = mssql_hook.get_conn()
    cursor = conn.cursor()
    cursor.executemany(sql,records)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("insert data DONE.")


#======================================================================================================
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

with DAG(
    dag_id='dag_local_to_mssql',
    max_active_tasks=2,
    start_date=datetime(2022, 11, 18,tzinfo=pendulum.timezone("Asia/Seoul")),
    schedule_interval='0 9 * * *',
    catchup=False,
    tags=['mssql_dev'],
    default_args=default_args
) as dag:

    start = BashOperator(task_id='start', bash_command='exit 0',do_xcom_push=False)

    t1 = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        op_kwargs={mssql_conn_id:'mssql_conn_chel',database:'chel_db','sql': sql_create},
        retries=0
        )

    sleep = BashOperator(task_id='sleep_1s', bash_command='sleep 1',do_xcom_push=False)

    t2 = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    op_kwargs={mssql_conn_id:'mssql_conn_chel',database:'chel_db','csv_path':'/home/chel/data/product_list.csv','sql': sql_insert},
    retries=0
    )

    end = BashOperator(task_id='end', bash_command='exit 0',do_xcom_push=False, trigger_rule="none_failed",retries=0)

    start >> t1 >> sleep >> t2 >> end