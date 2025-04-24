from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

def exemplo_funcao(data_interval_start: pendulum.DateTime, **context):
    print(f"Data Interval Start: {data_interval_start}")

with DAG(
    dag_id="exemplo_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    render_template_as_native_obj=True,  # Importante para passar o valor como pendulum.DateTime
    tags=["exemplo"],
) as dag:

    tarefa = PythonOperator(
        task_id="mostrar_data_interval_start",
        python_callable=exemplo_funcao,
        op_kwargs={"data_interval_start": "{{ data_interval_start }}"},
    )
