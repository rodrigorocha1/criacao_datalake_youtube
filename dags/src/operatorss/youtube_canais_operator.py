from typing import Dict, Optional, Tuple
from dags.src.hook.youtube_hook import YotubeHook
from dags.src.operatorss.youtube_operator import YoutubeOperator
from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson
from dags.src.services.manipulacao_dados.ioperacao_dados import IOperacaoDados
from operator import itemgetter


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
            # if len(req['items']) > 0 and req['items'][0]['snippet']['country'] == 'BR':
            if True:
                print(f'Canal Brasileiro {req}')
                req['assunto'] = self._assunto
                self._arquivo_json.guardar_dados(dado=req)
        except:
            pass

    def __executar_consulta_canal_temp(self) -> str:

        consulta = """
            select  ID_CANAL
            from youtube.temp_canal_video

        """
        return consulta



    def execute(self, context):
        consulta = self._criar_particao_datalake_camada(
            tabela_particao='bronze_canais',
        )
        self._arquivo_json.caminho_particao = self._criar_caminho_particao()
        self._operacao_banco.executar_consulta_dados(
            consulta=consulta,
            opcao_consulta=1
        )
        consulta_temp = self.__executar_consulta_canal_temp()
        lista_temp_canais = self._operacao_banco.executar_consulta_dados(
            consulta=consulta_temp,
            opcao_consulta=2
        )
        print(lista_temp_canais)
        consulta_canais = self._executar_consulta_canal_bronze()
        lista_consulta_canais = self._operacao_banco.executar_consulta_dados(
            consulta=consulta_canais,
            opcao_consulta=2
        )

        lista_temp_canais = list(map(itemgetter(0), lista_temp_canais[1]))
        lista_consulta_canais = list(map(itemgetter(0), lista_consulta_canais[1]))
        lista_canais = lista_consulta_canais + lista_temp_canais
        lista_canais = list(set(lista_canais))



        try:
            for json_response in self._operacao_hook.run(id_canais=lista_canais):
                print(json_response)
                self.gravar_dados(json_response)
        except Exception as E:
            print(E)
            exit
