from abc import ABC, abstractmethod


class ConexaoBanco(ABC):
    @abstractmethod
    def obter_conexao(self):
        pass
