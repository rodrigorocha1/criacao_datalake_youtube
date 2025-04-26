import pendulum

from unidecode import unidecode
from airflow.operators.bash import BashOperator

try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def executar_etl_assunto(**kwargs):

    from dags.src.services.manipulacao_dados.conexao_banco_hive import ConexaoBancoHive
    from dags.src.services.manipulacao_dados.operacao_banco_hive import OperacaoBancoHive
    from dags.src.etl.etl_youtube import ETLYoutube
    from dags.src.services.apiyoutube.api_youtube import ApiYoutube
    from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson

    api_youtube = ApiYoutube()
    arquivo = ArquivoJson()
    operacoes_dados = OperacaoBancoHive(conexao=ConexaoBancoHive())
    etl = ETLYoutube(api_youtube, operacoes_dados, arquivo)

    assunto = kwargs['assunto']
    data_publicacao_apos = kwargs['data_publicacao_apos']

    etl.processo_etl_assunto_video(
        assunto=assunto,
        data_publicacao_apos=data_publicacao_apos
    )


lista_assunto = ["No Man's Sky", "Cities Skylines", "Python"]

data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
hora_atual = int(data_hora_atual.hour)
data = data_hora_atual.format('YYYY_MM_DD')
data_hora_busca = data_hora_atual.subtract(days=1)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')


with DAG(
        dag_id='youtube_etl_dag',
        default_args=default_args,
        description='DAG para processo ETL dos vídeos e canais do YouTube por assunto',
        schedule_interval='@daily',
        start_date=datetime(2024, 4, 1),
        catchup=False,
        tags=['youtube', 'etl', 'api']
) as dag:
    inicio_dag = EmptyOperator(
        task_id='id_inicio_dag'
    )
    ip = '172.18.0.4'

    with TaskGroup('task_youtube_api_historico_pesquisa', dag=dag) as tg1:
        lista_task_assunto = []
        for assunto in lista_assunto:
            id_assunto = ''.join(
                filter(
                    lambda c: c.isalnum() or c.isspace(), unidecode(assunto)
                )
            ).replace(' ', '').lower()

            etl_assunto = PythonOperator(
                dag=dag,
                task_id=f'assunto_{id_assunto}',
                python_callable=executar_etl_assunto,
                op_kwargs={
                    'assunto': assunto,
                    'data_publicacao_apos': data_hora_busca
                }
            )
            lista_task_assunto.append(etl_assunto)

    fim_dag = EmptyOperator(
        task_id='id_fim_dag'
    )


    inicio_dag >> tg1 >> fim_dag
