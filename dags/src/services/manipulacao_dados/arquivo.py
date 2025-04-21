from typing import Dict, Optional
from abc import ABC, abstractmethod
import os


class Arquivo(ABC):

    def __init__(self):
        # self.__caminho_raiz = os.getcwd()
        self.__caminho_raiz = '/home/rodrigo/Documentos/projetos/criacao_datalake_youtube'
        self.__pasta_raiz_datalake = 'datalake'
        self.__camada = None
        self.__caminho_particao = None
        self.__termo_pesquisa = None
        self.__nome_arquivo = None

    @property
    def nome_arquivo(self):
        return self.__nome_arquivo

    @nome_arquivo.setter
    def nome_arquivo(self, nome_arquivo: str):
        self.__nome_arquivo = nome_arquivo

    @property
    def caminho_particao(self):
        return self.__caminho_particao

    @caminho_particao.setter
    def caminho_particao(self, caminho_particao: str):
        self.__caminho_particao = caminho_particao

    @property
    def camada(self):
        return self.__camada

    @camada.setter
    def camada(self, camada: str):
        self.__camada = camada

    @property
    def termo_pesquisa(self) -> str:
        return self.__termo_pesquisa

    @termo_pesquisa.setter
    def termo_pesquisa(self, termo_pesquisa: str):
        self.__termo_pesquisa = termo_pesquisa

    @property
    def diretorio(self):
        return os.path.join(
            self.__caminho_raiz,
            self.__pasta_raiz_datalake,
            self.__camada,
            self.__termo_pesquisa,
            self.__caminho_particao
        )


    @property
    def caminho_completo(self) -> Optional[str]:
        if None not in (self.__pasta_raiz_datalake, self.__camada, self.__termo_pesquisa):
            caminho = os.path.join(
                self.__caminho_raiz,
                self.__pasta_raiz_datalake,
                self.__camada,
                self.__termo_pesquisa,
                self.__caminho_particao,
                self.__nome_arquivo
            )

            return caminho
        return None

    @abstractmethod
    def guardar_dados(self, dado: Dict):
        pass
