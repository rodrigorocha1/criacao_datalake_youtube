try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from teste.soma import soma
from src.services.apiyoutube.api_youtube import ApiYoutube
from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson
from dags.src.services.manipulacao_dados.conexao_banco_hive import ConexaoBancoHive
from dags.src.services.manipulacao_dados.operacao_banco_hive import OperacaoBancoHive
from dags.src.etl.etl_youtube import ETLYoutube

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Instâncias dos serviços


def executar_etl(**kwargs):
    from dags.src.services.manipulacao_dados.conexao_banco_hive import ConexaoBancoHive
    from dags.src.services.manipulacao_dados.operacao_banco_hive import OperacaoBancoHive
    from dags.src.etl.etl_youtube import ETLYoutube

    api_youtube = ApiYoutube()
    arquivo = ArquivoJson()
    operacoes_dados = OperacaoBancoHive(conexao=ConexaoBancoHive())
    etl = ETLYoutube(api_youtube, operacoes_dados, arquivo)

    assunto = 'Danilo'
    data_publicacao_apos = '2025-04-23T00:00:00Z'
    data_pesquisa = '2025-04-23T00:00:00Z'

    etl.executar(assunto, data_publicacao_apos, data_pesquisa)

# Configuração da DAG
with DAG(
        dag_id='youtube_etl_dag',
        default_args=default_args,
        description='DAG para processo ETL dos vídeos e canais do YouTube por assunto',
        schedule_interval='@daily',
        start_date=datetime(2024, 4, 1),
        catchup=False,
        tags=['youtube', 'etl', 'api']
) as dag:
    assunto = 'Danilo'
    data_publicacao_apos = '2025-04-23T00:00:00Z'
    data_pesquisa = '2025-04-23T00:00:00Z'

    tarefa_etl_assunto_video = PythonOperator(
        task_id='etl_assunto_video',
        python_callable=executar_etl,
        provide_context=True
    )
    # Dependências
    tarefa_etl_assunto_video
