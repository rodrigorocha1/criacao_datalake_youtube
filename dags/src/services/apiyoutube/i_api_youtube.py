from abc import ABC, abstractmethod
from typing import Generator, Dict, Any


class IApiYoutube(ABC):

    @abstractmethod
    def obter_assunto(self, assunto: str) -> Generator[Dict[str, Any], None, None]:
        pass

    @abstractmethod
    def obter_dados_canais(self, id_canal: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def obter_dados_videos(self, id_video: str) -> Dict[str, Any]:
        pass
