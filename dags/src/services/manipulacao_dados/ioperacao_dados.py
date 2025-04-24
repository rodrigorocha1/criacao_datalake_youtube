try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from abc import ABC, abstractmethod
from typing import Tuple, Any


class IOperacaoDados(ABC):

    @abstractmethod
    def executar_consulta_dados(self, consulta: str) -> Tuple[bool, Any]:
        pass
