try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
import json
from typing import Dict

from dags.src.services.manipulacao_dados.arquivo import Arquivo


class ArquivoJson(Arquivo):

    def __init__(self):
        super().__init__()

    def guardar_dados(self, dado: Dict, opcao: int = 1):
        caminho = self.diretorio if opcao == 1 else self.caminho_depara
        with open(os.path.join(caminho, self.nome_arquivo), 'a') as arquivo_json:
            json.dump(dado, arquivo_json, ensure_ascii=False)
            arquivo_json.write('\n')

if __name__ == '__main__':
    aj = ArquivoJson()

    aj.camada = 'bronze'
    aj.termo_pesquisa = 'assunto'
    aj.caminho_particao = f'ano=2024/mes=1/dia=1/dia_semana=segunda-feira/assunto=teste'

    print(aj.caminho_completo)
