from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook

def query_hive_table(**kwargs):
    hook = HiveServer2Hook(hiveserver2_conn_id='id_hive', schema='youtube')

    sql = '''
        SELECT DISTINCT c.id_canal 
        FROM youtube.canais c 
        WHERE c.assunto = "No_Mans_Sky"
    '''
    results = hook.get_records(sql)
    print(results)
    for row in results:
        print(row)

with DAG(
    dag_id='hive_select_example',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['hive', 'example'],
) as dag:
    run_query = PythonOperator(
        task_id='run_hive_query',
        python_callable=query_hive_table,
    )

    run_query
