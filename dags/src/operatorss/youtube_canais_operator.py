from typing import Dict, Optional, Tuple
from dags.src.hook.youtube_hook import YotubeHook
from dags.src.operatorss.youtube_operator import YoutubeOperator
from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson
from dags.src.services.manipulacao_dados.ioperacao_dados import IOperacaoDados


class YoutubeBuscaCanaisOperator(YoutubeOperator):

    def __init__(
            self,
            task_id: str,
            assunto: str,
            operacao_hook: YotubeHook,
            arquivo_json: ArquivoJson,
            operacao_banco: IOperacaoDados,
            **kwargs
    ):
        self._arquivo_json = arquivo_json
        self._operacao_banco = operacao_banco
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

                self._arquivo_json.guardar_dados(dado=req)
                self._arquivo_pkl_canal.salvar_dados(id_canal)
        except:
            pass

    def __executar_consulta_canal_temp(self) -> str:
        consulta = """
            select distinct ID_CANAL
            from youtube.temp_canal_video

        """
        return consulta

    def execute(self, context):
        consulta = self._criar_particao_datalake_camada(
            tabela_particao='bronze_assunto',
        )
        self._arquivo_json.caminho_particao = self._criar_caminho_particao()
        # self._operacao_banco.executar_consulta_dados(consulta=consulta, opcao_consulta=1)
        try:
            for json_response in self._operacao_hook.run():
                self.gravar_dados(json_response)
        except Exception as E:
            print(E)
            exit
