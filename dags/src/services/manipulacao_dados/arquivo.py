try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))

except ModuleNotFoundError:
    pass
from typing import Dict
from abc import ABC, abstractmethod


class Arquivo(ABC):

    def __init__(self, camada: str, entidade: str, caminho_particao: str, nome_arquivo: str):
        # self.__caminho_raiz = os.getcwd()

        self._caminho_raiz = '/opt/airflow'  # Caminho Raiz
        self._pasta_raiz_datalake = 'datalake'  # Nome do diretório do datalake
        self._camada = camada  # Bronze Prata ou ouro
        self._entidade = entidade  # Assunto, canal e vídeo
        self._caminho_particao = caminho_particao  # Caminho da particao criada no hive
        self._nome_arquivo = nome_arquivo  # Nome do arquivo

    @abstractmethod
    def guardar_dados(self, dado: Dict):
        pass
