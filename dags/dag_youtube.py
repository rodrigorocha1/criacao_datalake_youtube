import pendulum
from src.operatorss.youtube_busca_operator import YoutubeBuscaOperator
from dags.src.hook.youtube_busca_assunto_hook import YoutubeBuscaAssuntoHook
from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson
from dags.src.services.manipulacao_dados.operacao_banco_hive_airlow import OperacaoBancoHiveAirflow
from airflow.utils.task_group import TaskGroup
from unidecode import unidecode
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

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
            ).replace(' ', '_').lower()

            etl_assunto = YoutubeBuscaOperator(
                task_id=id_assunto,
                operacao_hook=YoutubeBuscaAssuntoHook(
                    data_publicacao=data_hora_busca,
                    assunto_pesquisa=assunto
                ),
                assunto=assunto,
                arquivo_json=ArquivoJson(
                    camada='bronze',
                    entidade='assunto',
                    nome_arquivo='assunto.json',
                    caminho_particao=None,
                    opcao=2
                ),
                arquivo_temp_canal=ArquivoJson(
                    opcao=1
                ),
                operacao_banco=OperacaoBancoHiveAirflow()
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
