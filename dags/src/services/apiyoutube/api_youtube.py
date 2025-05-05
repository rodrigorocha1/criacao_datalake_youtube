try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Generator, Dict, Any, Tuple, Union

import requests
from airflow.models import Variable
from dotenv import load_dotenv

from dags.src.services.apiyoutube.i_api_youtube import IApiYoutube

load_dotenv()


class ApiYoutube(IApiYoutube):
    # Variable.get('CHAVE_YOUTUBE') #
    def __init__(self):
        self.__API_KEY = Variable.get('CHAVE_YOUTUBE')
        self.__url = 'https://youtube.googleapis.com/youtube/v3/'
        self.__headers = {
            'Accept': 'application/json'
        }

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

            params = {
                'part': 'snippet',
                'maxResults': 50,
                'publishedAfter': data_publicacao_apos,
                'key': self.__API_KEY,
                'q': assunto

            }

            if next_page_token is not None:
                params['pageToken'] = next_page_token
            url = self.__url + 'search'
            request = requests.get(
                url,
                headers=self.__headers,
                params=params
            )

            response = request.json()
            print(response)
            yield from response['items']

            try:
                next_page_token = response['nextPageToken']
            except:
                break

    def obter_dados_canais(self, id_canal: str) -> Union[Tuple[Dict[str, Any], str], Tuple[None, bool]]:
        """
        Método para buscar os dados dos canais
        :param id_canal: id do canal
        :type id_canal:  str
        :return: A lista com os dados dos canais
        :rtype: Union[Tuple[Dict[str, Any], str], Tuple[None, bool]]
        """
        url = self.__url + 'channels'
        params = {
            'part': 'snippet,statistics',
            'id': id_canal,
            'key': self.__API_KEY
        }
        try:
            requests_canais = requests.get(
                url=url,
                headers=self.__headers,
                params=params
            )
            response = requests_canais.json()

            return response, response['items'][0]['snippet']['country']
        except:
            return None, False

    def obter_dados_videos(self, id_video: str) -> Dict[str, Any]:
        """
        Método para obter os dados estátisticos dos vídeos
        :param id_video: id do vídeo
        :type id_video: str
        :return: Um iterador com os dados dos canais
        :rtype: Dict[str, Any]
        """
        params = {
            'part': 'statistics,contentDetails,id,snippet,status',
            'id': id_video,
            'key': self.__API_KEY,
            'regionCode': 'BR'
        }
        url = self.__url + 'videos'
        request_video = requests.get(
            url=url,
            params=params,
            headers=self.__headers
        )
        response = request_video.json()
        return response['items'][0]


if __name__ == '__main__':
    api_youtube = ApiYoutube()
    dados_canais = api_youtube.obter_assunto(
        assunto='Python', data_publicacao_apos='2025-04-21T16:50:46Z')
    for dado in dados_canais:
        print(dado)
