try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
import json
from typing import Dict, Optional

from dags.src.services.manipulacao_dados.arquivo import Arquivo


class ArquivoJson(Arquivo):

    def __init__(
            self,
            opcao: int,
            camada: Optional[str] = None,
            entidade: Optional[str] = None,
            caminho_particao: Optional[str]= None,
            nome_arquivo: Optional[str]= None,

    ):
        super().__init__(
            camada=camada,
            entidade=entidade,
            caminho_particao=caminho_particao,
            nome_arquivo=nome_arquivo,
            opcao=opcao
        )

    def guardar_dados(self, dado: Dict):
        with open(self._caminho_completo, 'a') as arquivo_json:
            json.dump(dado, arquivo_json, ensure_ascii=False)
            arquivo_json.write('\n')
