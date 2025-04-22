from shlex import quote
from typing import Dict
import json
from dags.src.services.manipulacao_dados.arquivo import Arquivo
import docker


class ArquivoJson(Arquivo):

    def __init__(self):
        super().__init__()
        self.__container_name = 'hadoop_hive_dbt_container'
        self.__client = docker.from_env()
        self.__container = self.__client.containers.get(self.__container_name)

    def guardar_dados(self, dado: Dict):
        dado = json.dumps(dado)

        dado_json_escaped = dado.replace('"', '\\"')

        # Monta o comando bash com o dado JSON formatado
        comando = f"bash -c 'echo \"{dado_json_escaped}\" >> {self.caminho_completo}'"
        exec_log = self.__container.exec_run(
            cmd=comando,
            stdout=True,
            stderr=True
        )
        print(exec_log)


if __name__ == '__main__':
    aj = ArquivoJson()

    aj.camada = 'bronze'
    aj.termo_pesquisa = 'assunto'
    aj.caminho_particao = f'ano=2024/mes=1/dia=1/dia_semana=segunda-feira/assunto=teste'

    print(aj.caminho_completo)
