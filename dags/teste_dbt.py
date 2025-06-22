from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator

# Definir argumentos padrão para o DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 27),
}

ssh_hook = SSHHook(
    remote_host="172.25.0.20",
    username="root",
    password="root"
)

with DAG(
        dag_id='ssh_connection_example',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:

    ssh_task = SSHOperator(
        task_id="connect_via_ssh_with_password",
        ssh_hook=ssh_hook,
        command=(
            "DBT_PROFILES_DIR=/usr/app/dbt/youtube "
            "dbt debug "
            "--project-dir /usr/app/dbt/youtube "
        ),
        retries=20,
        retry_delay=timedelta(minutes=20),
        cmd_timeout=240,  # Aumente o tempo de espera para o comando

    )

    ssh_task
