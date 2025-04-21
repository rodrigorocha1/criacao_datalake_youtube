from abc import ABC, abstractmethod
from typing import Tuple, Any


class IOperacaoBanco(ABC):

    @abstractmethod
    def executar_consulta(self, consulta: str) -> Tuple[bool, Any]:
        pass
