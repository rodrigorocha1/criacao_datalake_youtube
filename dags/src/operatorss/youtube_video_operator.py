from operator import itemgetter

try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Dict, Tuple
from dags.src.operatorss.youtube_operator import YoutubeOperator
from dags.src.services.manipulacao_dados.ioperacao_dados import IOperacaoDados
from dags.src.hook.youtube_hook import YotubeHook
from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson


class YoutubeVideoOperator(YoutubeOperator):
    def __init__(
            self,
            task_id: str,
            assunto: str,
            operacao_hook: YotubeHook,
            arquivo_json: ArquivoJson,
            operacao_banco: IOperacaoDados,
            **kwargs
    ):
        self._operacao_banco = operacao_banco
        self._arquivo_json = arquivo_json
        super().__init__(task_id=task_id, assunto=assunto, operacao_hook=operacao_hook, **kwargs)

    def __executar_consulta_canal_video_temp(self) -> str:
        consulta = f"""
            select  ID_CANAL, ID_VIDEO 
            from youtube.temp_canal_video
            where assunto = '{self._assunto}'
        """
        return consulta

    def __executar_consulta_video_bronze(self):
        consulta = f"""
            select v.id
            from youtube.bronze_videos v 
            where v.assunto = '{self._assunto}'
        """
        return consulta

    def gravar_dados(self, req: Dict):
        if len(req['items']) > 0:
            req['assunto'] = self._assunto
            self._arquivo_json.guardar_dados(dado=req)

    def execute(self, context):
        consulta = self._criar_particao_datalake_camada(
            tabela_particao='bronze_videos',
        )
        self._operacao_banco.executar_consulta_dados(consulta=consulta, opcao_consulta=1)
        self._arquivo_json.caminho_particao = self._criar_caminho_particao()
        consulta_video_temp = self.__executar_consulta_canal_video_temp()
        lista_canal_video_temp = self._operacao_banco.executar_consulta_dados(
            consulta=consulta_video_temp,
            opcao_consulta=2
        )

        consulta_canal_bronze = self._executar_consulta_canal_bronze()
        lista_consulta_canais_bronze = self._operacao_banco.executar_consulta_dados(
            consulta=consulta_canal_bronze,
            opcao_consulta=2
        )
        lista_consulta_canais_bronze = list(map(itemgetter(0), lista_consulta_canais_bronze[1]))
        lista_videos_brasileiros = [
            canal_video[1]
            for canal_video in lista_canal_video_temp[1]
            if canal_video[0] in lista_consulta_canais_bronze
        ]
        print('lista vídeos brasileiros')
        print(lista_videos_brasileiros)

        consulta_video_bronze = self.__executar_consulta_video_bronze()
        lista_videos_bronze = self._operacao_banco.executar_consulta_dados(
            consulta=consulta_video_bronze,
            opcao_consulta=2
        )
        print('Lista vídeos Bronze')
        print(lista_videos_bronze)
        print(lista_videos_bronze[1])
        lista_videos_bronze = list(map(itemgetter(0), lista_videos_bronze[1]))
        lista_videos = lista_videos_bronze + lista_videos_brasileiros
        lista_videos = list(set(lista_videos))
        print(lista_videos)
        try:
            for json_response in self._operacao_hook.run(ids_videos=lista_videos):
                print(json_response)
                self._arquivo_json.guardar_dados(json_response)
        except Exception as E:
            print(E)
            exit
