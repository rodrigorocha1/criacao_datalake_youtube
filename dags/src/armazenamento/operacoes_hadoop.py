from typing import Dict
from dags.src.armazenamento.iarmazenamento import Iarmazenamento
import docker


class OperacaoHadoop(Iarmazenamento[docker]):
    def __init__(self):
        self.__container = 'namenode'
        self.__cliente = docker.from_env()


    def armazenar_objeto(self, dados: Dict):
        pass

    def retornar_objeto(self) -> Dict:
        pass
