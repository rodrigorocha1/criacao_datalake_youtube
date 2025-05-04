try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
import pendulum
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from unidecode import unidecode
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def executar_etl_assunto(**kwargs):
    from dags.src.services.manipulacao_dados.operacao_banco_hive_airlow import OperacaoBancoHiveAirflow
    from dags.src.etl.etl_youtube import ETLYoutube
    from dags.src.services.apiyoutube.api_youtube import ApiYoutube
    from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson

    api_youtube = ApiYoutube()
    arquivo = ArquivoJson()
    operacoes_dados = OperacaoBancoHiveAirflow()
    etl = ETLYoutube(api_youtube, operacoes_dados, arquivo)
    etl.assunto = kwargs['assunto']
    data_publicacao_apos = kwargs['data_publicacao_apos']
    print(
        f'===========Data hora busca =================={data_publicacao_apos}')
    etl.processo_etl_assunto_video(
        data_publicacao_apos=data_publicacao_apos
    )


def executar_etl_canais(**kwargs):
    from dags.src.services.manipulacao_dados.operacao_banco_hive_airlow import OperacaoBancoHiveAirflow
    from dags.src.etl.etl_youtube import ETLYoutube
    from dags.src.services.apiyoutube.api_youtube import ApiYoutube
    from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson

    api_youtube = ApiYoutube()
    arquivo = ArquivoJson()
    operacoes_dados = OperacaoBancoHiveAirflow()
    etl = ETLYoutube(api_youtube, operacoes_dados, arquivo)
    etl.assunto = kwargs['assunto']
    etl.processo_etl_canal()


def executar_etl_videos(**kwargs):
    from dags.src.services.manipulacao_dados.operacao_banco_hive_airlow import OperacaoBancoHiveAirflow
    from dags.src.etl.etl_youtube import ETLYoutube
    from dags.src.services.apiyoutube.api_youtube import ApiYoutube
    from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson

    api_youtube = ApiYoutube()
    arquivo = ArquivoJson()
    operacoes_dados = OperacaoBancoHiveAirflow()
    etl = ETLYoutube(api_youtube, operacoes_dados, arquivo)
    etl.assunto = kwargs['assunto']
    etl.processo_etl_video()


# lista_assunto = ["No Man's Sky", "Cities Skylines", "Python"]
lista_assunto = ["No Man's Sky"]
data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
hora_atual = int(data_hora_atual.hour)
data = data_hora_atual.format('YYYY_MM_DD')
data_hora_busca = data_hora_atual.subtract(minutes=60)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')


with DAG(
        dag_id='youtube_etl_dag_exemplos',
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

    with TaskGroup('task_youtube_api_historico_pesquisa', dag=dag) as tg_assunto:
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

    # with TaskGroup('task_youtube_api_canais', dag=dag) as tg_canais:
    #     lista_canais = []
    #     for assunto in lista_assunto:
    #         id_assunto = ''.join(
    #             filter(
    #                 lambda c: c.isalnum() or c.isspace(), unidecode(assunto)
    #             )
    #         ).replace(' ', '').lower()
    #         etl_canais = PythonOperator(
    #             task_id=f'canais_{id_assunto}',
    #             python_callable=executar_etl_canais,
    #             op_kwargs={
    #                 'assunto': assunto
    #             }

    #         )
    #         lista_canais.append(etl_canais)

    # with TaskGroup('task_youtube_api_video', dag=dag) as tg_videos:
    #     lista_videos = []
    #     for assunto in lista_assunto:
    #         id_assunto = ''.join(
    #             filter(
    #                 lambda c: c.isalnum() or c.isspace(), unidecode(assunto)
    #             )
    #         ).replace(' ', '').lower()
    #         etl_videos = PythonOperator(
    #             task_id=f'id_video_{id_assunto}',
    #             python_callable=executar_etl_videos,
    #             op_kwargs={
    #                 'assunto': assunto
    #             }
    #         )
    #         lista_videos.append(etl_videos)

    fim_dag = EmptyOperator(
        task_id='id_fim_dag'
    )

    inicio_dag >> tg_assunto >> fim_dag
