from typing import Any, Dict, Generator
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
import os

load_dotenv()


class YoutubeAPI:
    def __init__(self):
        self.__CHAVE = os.environ['CHAVE_API']
        self.__youtube = build('youtube', 'v3', developerKey=self.__CHAVE)

    def buscar_assunto(self, assunto: str, data_publicacao: str) -> Generator[Dict[str, Any], None, None]:
        """Método para buscar o assunto

        Args:
            assunto (str): assunto do vídeo
            data_publicacao (str): data de públicação

        Yields:
            Generator[Dict[str, Any], None, None]: Gerador com os vídeos
        """
        next_page_token = None
        while True:
            try:
                request = self.__youtube.search().list(
                    part='snippet',
                    q=assunto,
                    publishedAfter=data_publicacao,  # Formato ISO 8601
                    order='date',
                    pageToken=next_page_token

                )

                response = request.execute()

                yield from response['items']
                next_page_token = response['nextPageToken']
                print(next_page_token)
                if not next_page_token:
                    break
            except HttpError:
                mensagem = 'Erro ao fazer a requisicao'
            except KeyError:
                mensagem = 'Chave inexistente'
            except ValueError:
                mensagem = 'Erro no formato de valor'
            except AttributeError:
                mensagem = 'Erro de inicialização'
            except ConnectionError:
                mensagem = 'Erro de conexão'
            except TimeoutError:
                mensagem = 'timeout da api'
            except Exception as e:
                mensagem = 'Exceção não mapeada'

    def buscar_canais_brasileiros(self, id_canal: str) -> bool:
        """Método para recuperar canais brasileiros

        Args:
            id_canal (str): id do canal

        Returns:
            bool: verdadeiro se o canal for brasileiro, falso caso contrário
        """
        request = self.__youtube.channels().list(
            part='snippet,statistics',
            id=id_canal
        )

        response = request.execute()
        if response['items'][0]['snippet']['country'] == 'BR':
            return True
        return False

    def buscar_estatisticas_videos(self, id_video: str):
        request = self.__youtube.videos().list(
            part='snippet,statistics',
            id=id_video
        )
        response = request.execute()
        return response
