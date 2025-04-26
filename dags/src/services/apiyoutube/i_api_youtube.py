
from abc import ABC, abstractmethod
from typing import Generator, Dict, Any, Tuple, Union


class IApiYoutube(ABC):

    @abstractmethod
    def obter_assunto(self, assunto: str, data_publicacao_apos: str) -> Generator[Dict[str, Any], None, None]:
        """
        Método para obter as requisição dos assuntos
        :param data_publicacao_apos: data de publicação do vídeo
        :type data_publicacao_apos: str
        :param assunto: assunto de pesquisa
        :type assunto: str
        :return: Um gerador com as respostas dos assuntos
        :rtype: Generator[Dict[str, Any], None, None]
        """
        pass

    @abstractmethod
    def obter_dados_canais(self, id_canal: str) -> Union[Tuple[Dict[str, Any], str], Tuple[None, bool]]:
        """
        Método para buscar os dados dos canais
        :param id_canal: id do canal
        :type id_canal:  str
        :return: A Tupla com os dados dos canais e
        :rtype: Union[Tuple[Dict[str, Any], str], Tuple[None, bool]]
        """
        pass

    @abstractmethod
    def obter_dados_videos(self, id_video: str) -> Dict[str, Any]:
        """
        Método para obter os dados estátisticos dos vídeos
        :param id_video: id do vídeo
        :type id_video: str
        :return: Um iterador com os dados dos canais
        :rtype:  Dict[str, Any]
        """
        pass
