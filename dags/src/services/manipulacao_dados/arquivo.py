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
            camada: Optional[str],
            entidade: Optional[str],
            nome_arquivo: Optional[str],
            caminho_particao: Optional[str] = None
    ):
        # self.__caminho_raiz = os.getcwd()

        self._caminho_raiz = '/opt/airflow'  # Caminho Raiz
        self._pasta_raiz_datalake = 'datalake'  # Nome do diretório do datalake
        self.camada = camada  # Bronze Prata ou ouro
        self.entidade = entidade  # Assunto, canal e vídeo
        self.caminho_particao = caminho_particao  # Caminho da particao criada no hive
        self.nome_arquivo = nome_arquivo  # Nome do arquivo
        self.opcao = opcao

    @abstractmethod
    def guardar_dados(self, dado: Dict):
        pass

    @property
    def caminho_completo(self) -> str:
        if self.opcao == 1:
            caminho = os.path.join(
                self._caminho_raiz,
                self._pasta_raiz_datalake,
                self.camada,
                self.entidade,
                self.caminho_particao
            )
        else:
            caminho = os.path.join(
                self._caminho_raiz,
                self._pasta_raiz_datalake,
                self.camada
            )


        return os.path.join(caminho, self.nome_arquivo)
