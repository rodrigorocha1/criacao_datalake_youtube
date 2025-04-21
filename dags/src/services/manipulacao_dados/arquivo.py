from typing import Dict
from abc import ABC, abstractmethod
import os


class Arquivo(ABC):

    def __init__(self, datalake: str, assunto: str):
        self.__caminho_raiz = os.getcwd()
        self.__pasta_raiz_datalake = datalake
        self.__assunto = assunto

    @property
    def assunto(self) -> str:
        return self.__assunto

    @assunto.setter
    def assunto(self, assunto: str):
        self.__assunto = assunto

    @property
    def datalake(self) -> str:
        return self.__pasta_raiz_datalake

    @datalake.setter
    def datalake(self, datalake: str):
        self.__pasta_raiz_datalake = datalake

    @abstractmethod
    def guardar_dados(self, dado: Dict):
        pass
