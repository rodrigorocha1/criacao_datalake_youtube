import pendulum
from src.operatorss.youtube_busca_operator import YoutubeBuscaOperator
from dags.src.hook.youtube_busca_assunto_hook import YoutubeBuscaAssuntoHook
from dags.src.operatorss.youtube_video_operator import YoutubeVideoOperator
from dags.src.hook.youtube_dados_videos_hook import YoutubeVideoHook

from dags.src.hook.youtube_canais_hook import YoutubeBuscaCanaisHook
from src.operatorss.youtube_canais_operator import YoutubeBuscaCanaisOperator

from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson
from dags.src.services.manipulacao_dados.operacao_banco_hive_airlow import OperacaoBancoHiveAirflow
from airflow.utils.task_group import TaskGroup
from unidecode import unidecode
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

ssh_hook = SSHHook(
    remote_host="172.25.0.20",
    username="root",
    password="root"
)

data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
hora_atual = int(data_hora_atual.hour)
data = data_hora_atual.format('YYYY_MM_DD')
data_hora_busca = data_hora_atual.subtract(hours=24)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')

with DAG(
        dag_id='youtube_etl_dag_exemplos',
        default_args=default_args,
        description='DAG para processo ETL dos vídeos e canais do YouTube por assunto',
        schedule_interval=None,
        start_date=datetime(2024, 4, 1),
        catchup=False,
        tags=['youtube', 'etl', 'api']
) as dag:
    lista_assunto = ["python", "palworld", "no man's sky"]
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
                    nome_arquivo='assunto.jsonl',
                    opcao=1
                ),

                arquivo_temp_json=ArquivoJson(
                    camada='temp',
                    nome_arquivo='temp_assunto.jsonl',
                    opcao=2,
                    entidade=None
                ),
                operacao_banco=OperacaoBancoHiveAirflow()
            )
            lista_task_assunto.append(etl_assunto)

    with TaskGroup('task_youtube_api_canais', dag=dag) as tg_canais:
        lista_canais = []
        for assunto in lista_assunto:
            id_assunto = ''.join(
                filter(
                    lambda c: c.isalnum() or c.isspace(), unidecode(assunto)
                )
            ).replace(' ', '_').lower()
            etl_canais = YoutubeBuscaCanaisOperator(
                task_id=f'canais_{id_assunto}',
                operacao_hook=YoutubeBuscaCanaisHook(),
                arquivo_json=ArquivoJson(
                    camada='bronze',
                    entidade='canais',
                    nome_arquivo='canal.jsonl',
                    opcao=1
                ),
                operacao_banco=OperacaoBancoHiveAirflow(),
                assunto=assunto,

            )
            lista_canais.append(etl_canais)

    with TaskGroup('task_youtube_api_video', dag=dag) as tg_videos:
        lista_videos = []
        for assunto in lista_assunto:
            id_assunto = ''.join(
                filter(
                    lambda c: c.isalnum() or c.isspace(), unidecode(assunto)
                )
            ).replace(' ', '').lower()
            etl_videos = YoutubeVideoOperator(
                task_id=f'id_video_{id_assunto}',
                arquivo_json=ArquivoJson(
                    camada='bronze',
                    entidade='videos',
                    nome_arquivo='video.jsonl',
                    opcao=1
                ),
                operacao_hook=YoutubeVideoHook(),
                operacao_banco=OperacaoBancoHiveAirflow(),
                assunto=assunto

            )
            lista_videos.append(etl_videos)

    ssh_dbt_canal = SSHOperator(
        task_id="id_ssh_dbt_canal",
        ssh_hook=ssh_hook,
        command=(
            "DBT_PROFILES_DIR=/usr/app/dbt/youtube "
            "dbt run --select prata_canal "
            "--project-dir /usr/app/dbt/youtube "
        ),
        retries=20,
        retry_delay=timedelta(minutes=20),
        cmd_timeout=240,  # Aumente o tempo de espera para o comando

    )

    ssh_dbt_video = SSHOperator(
        task_id="id_ssh_dbt_video",
        ssh_hook=ssh_hook,
        command=(
            "DBT_PROFILES_DIR=/usr/app/dbt/youtube "
            "dbt run --select prata_video "
            "--project-dir /usr/app/dbt/youtube "
        ),
        retries=20,
        retry_delay=timedelta(minutes=20),
        cmd_timeout=240,  # Aumente o tempo de espera para o comando

    )

    tasq_remove_temp = BashOperator(
        task_id='remove',
        bash_command='rm -rf /opt/airflow/datalake/temp/*'
    )

    fim_dag = EmptyOperator(
        task_id='id_fim_dag'
    )

    inicio_dag >> tg_assunto >> tg_canais >> tg_videos >> ssh_dbt_canal >> ssh_dbt_video >> tasq_remove_temp >> fim_dag
