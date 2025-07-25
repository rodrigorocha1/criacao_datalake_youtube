try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from datetime import datetime
from typing import Iterable, List, Optional, Dict
from abc import ABC, abstractmethod
from airflow.providers.http.hooks.http import HttpHook
import requests
from dags.src.configuracao.configuracao import Configuracao


class YotubeHook(HttpHook, ABC):

    def __init__(self, conn_id: str = 'youtube_default') -> None:
        self._conn_id = conn_id
        self._URL = Configuracao.url
        self._CHAVE = Configuracao.chave
        super().__init__(http_conn_id=self._conn_id)

    @abstractmethod
    def _criar_url(self) -> str:
        pass

    def _executar_paginacao(self, url: str, session, params: List[Dict]) -> Iterable[Dict]:
        """Gerador para obter requisição da api do youtube

        Args:
            url (str): url da api
            session (_type_): session airflow
            params (List[Dict]): lista de parâmetos requeridos pela api

        Returns:
            Iterable[Dict]: a requisição da api

        Yields:
            Iterator[Iterable[Dict]]: um json com as requisições da api
        """
        i = 1


        next_token = ''
        for param in params:
            while next_token is not None:
                response = self.conectar_api(url, param, session)
                if response:
                    json_response = response.json()
                    json_response['data_pesquisa'] = datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S'
                    )

                    yield from json_response['items']
                    try:
                        next_token = json_response['nextPageToken']
                        param['pageToken'] = next_token
                        print('próximo token', next_token)
                    except KeyError:
                        break

                else:
                    break

    def conectar_api(self, url: str, params: Dict, session: requests.Session) -> Optional[requests.Response]:
        try:

            response = requests.Request('GET', url=url, params=params)
            prepared = session.prepare_request(response)
            return self.run_and_check(session=session,prepped_request=prepared, extra_options={})
        except Exception as e:
            print(f"Erro na conexão com API: {e}")
            return None

    @abstractmethod
    def run(self, **kwargs):
        pass
