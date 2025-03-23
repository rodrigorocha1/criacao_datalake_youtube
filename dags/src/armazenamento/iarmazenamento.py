from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Dict

T = TypeVar('T')


class Iarmazenamento(ABC, Generic[T]):

    @abstractmethod
    def armazenar_objeto(self, dados: Dict):
        pass

    @abstractmethod
    def retornar_objeto(self) -> Dict:
        pass
