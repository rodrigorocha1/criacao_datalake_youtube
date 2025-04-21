from abc import ABC, abstractmethod
from typing import Tuple, Any


class IOperacaoDados(ABC):

    @abstractmethod
    def executar_consulta_dados(self, consulta: str) -> Tuple[bool, Any]:
        pass
