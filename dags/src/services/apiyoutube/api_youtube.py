from typing import Generator, Dict, Any, Tuple
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
        next_page_token = None
        while True:
            request = self.__youtube.search().list(
                q=assunto,
                part='id,snippet',
                type='video',  # Corrigido de 'vide' para 'video'
                maxResults=50,
                publishedAfter=data_publicacao_apos,
                order='date',
                pageToken=next_page_token

            )

            response = request.execute()
            yield from response['items']

            try:
                next_page_token = response['nextPageToken']
            except:
                break

    def obter_dados_canais(self, id_canal: str) -> Tuple[Dict[str, Any], bool]:
        """
        Método para buscar os dados dos canais
        :param id_canal: id do canal
        :type id_canal:  str
        :return: A lista com os dados dos canais
        :rtype: Tuple[Dict[str, Any], bool]
        """
        requests_canais = self.__youtube.channels().list(
            id=id_canal,
            part='snippet,statistics'
        )
        response = requests_canais.execute()

        return response, response['items'][0]['country']

    def obter_dados_videos(self, id_video: str) -> Dict[str, Any]:
        """
        Método para obter os dados estátisticos dos vídeos
        :param id_video: id do vídeo
        :type id_video: str
        :return: Um iterador com os dados dos canais
        :rtype: Generator[Dict[str, Any], None, None]
        """
        request_video = self.__youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=id_video
        )
        response = request_video.execute()
        return response['items']


if __name__ == '__main__':
    api_youtube = ApiYoutube()
    dados_canais = api_youtube.obter_dados_canais('UCCmONmoMVBaAOj_NLjA00nA')
    print(dados_canais)
