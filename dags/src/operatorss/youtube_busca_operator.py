
from dags.src.operatorss.youtube_operator import YoutubeOperator
from hook.youtube_hook import YotubeHook
from src.services.manipulacao_dados.arquivo_json import ArquivoJson
from typing import Dict


class YoutubeBuscaOperator(YoutubeOperator):

    def __init__(
            self,
            task_id: str,
            assunto: str,
            operacao_hook: YotubeHook,
            arquivo_json: ArquivoJson,
            **kwargs
    ):
        self.__arquivo_json = arquivo_json
        super().__init__(
            task_id=task_id,
            assunto=assunto,
            operacao_hook=operacao_hook,
            **kwargs
        )

    def gravar_dados(self, req: Dict):
        req['assunto'] = self._assunto
        self.__arquivo_json.guardar_dados(dado=req)

    def execute(self, context):
        try:
            for json_response in self._operacao_hook.run():
                self.gravar_dados(req=json_response)
        except Exception as E:
            print(E)
            exit
