try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))

except ModuleNotFoundError:
    pass
import os
from abc import ABC, abstractmethod
from typing import Dict, Optional

from dags.enums.camada_enum import Camada


class Arquivo(ABC):

    def __init__(self):
        # self.__caminho_raiz = os.getcwd()

        self._caminho_raiz = '/opt/airflow'  # Caminho Raiz
        self._pasta_raiz_datalake = 'datalake'  # Nome do diretório do datalake
        self._camada = None  # Bronze Prata ou ouro
        self._entidade = None  # Assunto, canal e vídeo
        self._caminho_particao = None  # Caminho da particao criada no hive
        self._nome_arquivo = None  # Nome do arquivo

    @property
    def camada(self) -> Optional[Camada]:
        return self.__camada

    @camada.setter
    def camada(self, value: Camada):
        self._camada = value

    @property
    def entidade(self) -> Optional[str]:
        return self._entidade

    @entidade.setter
    def entidade(self, value: str):
        self._entidade = value

    @property
    def caminho_particao(self) -> Optional[str]:
        return self._caminho_particao

    @caminho_particao.setter
    def caminho_particao(self, value: str):
        self._caminho_particao = value

    @property
    def nome_arquivo(self) -> Optional[str]:
        return self._nome_arquivo

    @nome_arquivo.setter
    def nome_arquivo(self, value: str):
        self._nome_arquivo = value

    @property
    def caminho_datalake(self):

        return os.path.join(
            self._caminho_raiz,
            self._pasta_raiz_datalake,
            self._camada,
            self._entidade,
            self._caminho_particao,
            self._nome_arquivo
        )

    @abstractmethod
    def guardar_dados(self, dado: Dict):
        pass
