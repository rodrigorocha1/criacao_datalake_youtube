from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from unidecode import unidecode
from dags.src.etl.etl_youtube import ETLYoutube
from dags.src.services.apiyoutube.api_youtube import ApiYoutube
from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson
from dags.src.services.manipulacao_dados.operacao_banco_hive import OperacaoBancoHive
from dags.src.services.manipulacao_dados.conexao_banco_hive import ConexaoBancoHive

default_args = {
    'start_date': datetime(2024, 1, 1)
}

assunto = ["No Man's Sky", "Python"]

with DAG(
        dag_id='dag_youtube_v3',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        tags=['youtube', 'dbt', 'etl']
) as dag:
    etl_youtube = ETLYoutube(
        api_youtube=ApiYoutube(),
        arquivo=ArquivoJson(),
        operacoes_dados=OperacaoBancoHive(
            conexao=ConexaoBancoHive()
        )
    )
    inicio_dag = EmptyOperator(
        task_id='inicio_dag',

    )

    # with TaskGroup('task_youtube_historico_pesquisa', dag=dag) as tg1:
    #     lista_task_assunto = []
    #     for assunto in assunto:
    #         task_id_assunto = ''.join(
    #             filter(
    #                 lambda c: c.isalnum() or c.isspace(),
    #                 unidecode(assunto))).replace(' ', '').lower()
    #
    #         busca_assunto = PythonOperator(
    #             task_id=f'task_assunto_{task_id_assunto}',
    #             python_callable=etl_youtube.processo_etl_assunto_video,
    #             op_args={'assunto': assunto, 'data_pesquisa': }
    #         )

    fim_dag = EmptyOperator(
        task_id='fim_dag'
    )

    inicio_dag >> fim_dag
