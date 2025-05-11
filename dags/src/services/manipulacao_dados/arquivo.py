try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))

except ModuleNotFoundError:
    pass
from typing import Dict, Optional
from abc import ABC, abstractmethod


class Arquivo(ABC):

    def __init__(
            self,
            opcao: int,
            camada: Optional[str] = None,
            entidade: Optional[str] = None,

            nome_arquivo: Optional[str] = None,
    ):
        # self.__caminho_raiz = os.getcwd()

        self._caminho_raiz = '/opt/airflow'  # Caminho Raiz
        self._pasta_raiz_datalake = 'datalake'  # Nome do diretório do datalake
        self._camada = camada  # Bronze Prata ou ouro
        self._entidade = entidade  # Assunto, canal e vídeo
        self._caminho_particao = None  # Caminho da particao criada no hive
        self._nome_arquivo = nome_arquivo  # Nome do arquivo
        self._caminho_completo = None
        self._opcao = opcao

    @property
    def caminho_completo(self):
        return self._caminho_completo

    @caminho_completo.setter
    def caminho_completo(self):
        self._caminho_completo = os.path.join(
            self._caminho_raiz,
            self._pasta_raiz_datalake,
            self._camada,
            self._entidade,
            self._caminho_particao,
            self._nome_arquivo,
            self._nome_arquivo

        ) if self._opcao == 1 else os.path.join(
            self._caminho_raiz,
            self._pasta_raiz_datalake,
            self._entidade,
            self._nome_arquivo
        )

    @property
    def caminho_particao(self):
        return self._caminho_particao

    @caminho_particao.setter
    def caminho_particao(self, caminho_particao: str):
        self._caminho_particao = caminho_particao

    @abstractmethod
    def guardar_dados(self, dado: Dict):
        pass
