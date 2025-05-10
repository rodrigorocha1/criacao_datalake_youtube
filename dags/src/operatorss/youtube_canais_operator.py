from typing import Dict, Optional, Tuple
from dags.src.hook.youtube_hook import YotubeHook
from dags.src.operatorss.youtube_operator import YoutubeOperator
from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson


class YoutubeBuscaCanaisOperator(YoutubeOperator):

    def __init__(
            self,
            task_id: str,
            assunto: str,
            operacao_hook: YotubeHook,
            arquivo_json: ArquivoJson,
            **kwargs
    ):
        super().__init__(
            task_id=task_id,
            assunto=assunto,
            operacao_hook=operacao_hook,
            **kwargs
        )

    def gravar_dados(self, req: Dict):
        try:
            if len(req['items']) > 0 and req['items'][0]['snippet']['country'] == 'BR':
                id_canal = [req['items'][0]['id']]

                req['assunto'] = self._assunto

                self._arquivo_json.salvar_dados(dados=req)
                self._arquivo_pkl_canal.salvar_dados(id_canal)
        except:
            pass

    def execute(self, context):
        try:
            for json_response in self._operacao_hook.run():
                self.gravar_dados(json_response)
        except Exception as E:
            print(E)
            exit
