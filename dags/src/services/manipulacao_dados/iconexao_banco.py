from abc import ABC, abstractmethod


class IConexaoBanco(ABC):

    @abstractmethod
    def obter_conexao(self):
        pass
