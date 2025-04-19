from typing import Generator, Dict, Any
from dotenv import load_dotenv
from googleapiclient.discovery import build
import os

from dags.src.services.apiyoutube.i_api_youtube import IApiYoutube

load_dotenv()


class ApiYoutube(IApiYoutube):

    def __init__(self):
        self.__API_KEY = os.environ['API_KEY']
        self.__youtube = build(
            'youtube',
            'v3',
            developerKey=self.__API_KEY
        )

    def obter_assunto(self, assunto: str, data_publicacao_apos: str) -> Generator[Dict[str, Any], None, None]:
        """
        Método para obter as requisição dos assuntos
        :param data_publicacao_apos: data de publicação do vídeo
        :type data_publicacao_apos: str
        :param assunto: assunto de pesquisa
        :type assunto: str
        :return: Um gerador com as respostas dos assuntos
        :rtype: Generator[Dict[str, Any], None, None]
        """

        request = self.__youtube.search().list(
            q=assunto,
            part='id,snippet',
            type='video',  # Corrigido de 'vide' para 'video'
            maxResults=50,
            publishedAfter='2025-04-01T00:00:00Z',
            order='date'

        )

        response = request.execute()

    def obter_dados_canais(self, id_canal: str) -> Dict[str, Any]:
        pass

    def obter_dados_videos(self, id_video: str) -> Generator[Dict[str, Any], None, None]:
        pass
