from abc import ABC, abstractmethod


class IOperacaoBanco(ABC):

    @abstractmethod
    def executar_consulta(self, consulta: str) -> bool:
        pass
