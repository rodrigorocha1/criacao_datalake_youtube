from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='dag_teste_jinja',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    template_searchpath=['/tmp'],  # onde o Airflow vai procurar templates (opcional)
    tags=['exemplo', 'jinja'],
) as dag:

    exibe_data = BashOperator(
        task_id='exibe_data',
        bash_command="echo 'A data de execução é {{ ts }}'",
    )

    def mostra_variavel(**kwargs):
        nome = Variable.get("meu_nome")
        print(f"Olá, {{ ds }}! Meu nome é {nome}")

    exibe_variavel = PythonOperator(
        task_id='exibe_variavel',
        python_callable=mostra_variavel,
        provide_context=True,
    )

    exibe_data >> exibe_variavel
