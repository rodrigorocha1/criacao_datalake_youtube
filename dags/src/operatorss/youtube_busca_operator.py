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
from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson
from typing import Dict


class YoutubeBuscaOperator(YoutubeOperator):

    def __init__(
            self,
            task_id: str,
            assunto: str,
            operacao_hook: YotubeHook,
            arquivo_json: ArquivoJson,
            arquivo_temp_canal: ArquivoJson,
            arquivo_temp_video: ArquivoJson,
            operacao_banco: IOperacaoDados,
            **kwargs
    ):
        self.__arquivo_json = arquivo_json
        self.__tabela = 'bronze_assunto'

        super().__init__(
            task_id=task_id,
            assunto=assunto,
            operacao_hook=operacao_hook,
            **kwargs
        )



    def gravar_dados(self, req: Dict):
        req['assunto'] = self._assunto
        self.__arquivo_json.guardar_dados(dado=req)

        json_canal = {

        }
        json_video = {}

    def execute(self, context):
        # Criar _particao
        consulta = self._criar_particao_datalake_camada(
            tabela_particao='bronze_assunto'
        )
        self._o
        try:
            for json_response in self._operacao_hook.run():
                self.gravar_dados(req=json_response)
        except Exception as E:
            print(E)
            exit
