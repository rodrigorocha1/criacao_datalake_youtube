import datetime

try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from dags.src.operatorss.youtube_operator import YoutubeOperator
from dags.src.services.manipulacao_dados.ioperacao_dados import IOperacaoDados
from dags.src.hook.youtube_hook import YotubeHook
from dags.src.services.manipulacao_dados.arquivo import Arquivo
from typing import Dict


class YoutubeBuscaOperator(YoutubeOperator):

    def __init__(
            self,
            task_id: str,
            assunto: str,
            operacao_hook: YotubeHook,
            arquivo_json: Arquivo,
            arquivo_temp_json: Arquivo,
            operacao_banco: IOperacaoDados,
            **kwargs
    ):
        self.__arquivo_json = arquivo_json
        self.__tabela = 'bronze_assunto'

        self._arquivo_temp_json = arquivo_temp_json
        super().__init__(
            task_id=task_id,
            assunto=assunto,
            operacao_hook=operacao_hook,
            operacao_banco=operacao_banco,
            **kwargs
        )

    def gravar_dados(self, req: Dict):
        """
        Método para gravar dados
        :param req: requisição da API
        """
        req['assunto'] = self._assunto
        self.__arquivo_json.guardar_dados(dado=req)
        json_canal_video = {
            'ID_CANAL': req['snippet']['channelId'],
            'ID_VIDEO': req['id']['videoId'],
            'ASSUNTO': self._assunto

        }
        self._arquivo_temp_json.guardar_dados(dado=json_canal_video)

    def execute(self, context):
        """
        Método para executar a DAG
        :param context: contexto do apache airflow

        """

        consulta = self._criar_particao_datalake_camada(
            tabela_particao='bronze_assunto',
        )
        self.__arquivo_json.caminho_particao = self._criar_caminho_particao()
        self._operacao_banco.executar_consulta_dados(consulta=consulta, opcao_consulta=1)
        try:
            for json_response in self._operacao_hook.run():
                self.gravar_dados(req=json_response)
        except Exception as E:
            print(E)
            exit
