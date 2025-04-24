from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from unidecode import unidecode
from teste.soma import soma

from src.etl.etl_youtube import ETLYoutube
from src.services.apiyoutube.api_youtube import ApiYoutube
from src.services.manipulacao_dados.arquivo_json import ArquivoJson
from src.services.manipulacao_dados.operacao_banco_hive import OperacaoBancoHive
from src.services.manipulacao_dados.conexao_banco_hive import ConexaoBancoHive

default_args = {
    'start_date': datetime(2024, 1, 1)
}

assunto = ["No Man's Sky", "Python"]

etl_youtube = ETLYoutube(
    api_youtube=ApiYoutube(),
    arquivo=ArquivoJson(),
    operacoes_dados=OperacaoBancoHive(
        conexao=ConexaoBancoHive()
    )
)

with DAG(
        dag_id='dag_youtube_v3',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        tags=['youtube', 'dbt', 'etl']
) as dag:
    inicio_dag = EmptyOperator(
        task_id='inicio_dag',

    )

    with TaskGroup('task_youtube_historico_pesquisa', dag=dag) as tg1:
        lista_task_assunto = []
        for assunto in assunto:
            task_id_assunto = ''.join(
                filter(
                    lambda c: c.isalnum() or c.isspace(),
                    unidecode(assunto))).replace(' ', '').lower()

            busca_assunto = PythonOperator(
                task_id=f'task_assunto_{task_id_assunto}',
                python_callable=soma,
                op_args={'a': 1, 'b': 2}
            )
            lista_task_assunto.append(assunto)

    fim_dag = EmptyOperator(
        task_id='fim_dag'
    )

    inicio_dag >> tg1 >> fim_dag
