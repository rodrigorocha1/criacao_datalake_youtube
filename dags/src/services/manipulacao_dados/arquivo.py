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

        self.__caminho_raiz = '/'  # Caminho Raiz
        self.__pasta_raiz_datalake = 'datalake'  # Nome do diretório do datalake
        self.__camada = None  # Bronze Prata ou ouro
        self.__entidade = None  # Assunto, canal e vídeo
        self.__caminho_particao = None  # Caminho da particao criada no hive
        self.__nome_arquivo = None  # Nome do arquivo

    @property
    def camada(self) -> Optional[Camada]:
        return self.__camada

    @camada.setter
    def camada(self, value: Camada):
        self.__camada = value

    @property
    def entidade(self) -> Optional[str]:
        return self.__entidade

    @entidade.setter
    def entidade(self, value: str):
        self.__entidade = value

    @property
    def caminho_particao(self) -> Optional[str]:
        return self.__caminho_particao

    @caminho_particao.setter
    def caminho_particao(self, value: str):
        self.__caminho_particao = value

    @property
    def nome_arquivo(self) -> Optional[str]:
        return self.__nome_arquivo

    @nome_arquivo.setter
    def nome_arquivo(self, value: str):
        self.__nome_arquivo = value

    @property
    def caminho_datalake(self):
        return os.path.join(
            self.__caminho_raiz,
            self.__pasta_raiz_datalake,
            self.__camada,
            self.__entidade,
            self.__caminho_particao,
            self.__nome_arquivo
        )

    @abstractmethod
    def guardar_dados(self, dado: Dict):
        pass
