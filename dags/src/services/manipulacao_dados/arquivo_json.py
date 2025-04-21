from typing import Dict
import json
import os
import stat

from dags.src.services.manipulacao_dados.arquivo import Arquivo


class ArquivoJson(Arquivo):

    def __init__(self):
        super().__init__()

    def guardar_dados(self, dado: Dict):
        if not os.access(self.diretorio, os.W_OK):
            os.chmod(self.diretorio, stat.S_IWUSR | stat.S_IRUSR)


        with open(self.caminho_completo, 'a', encoding='utf-8') as arquivo_json:
            json.dump(dado, arquivo_json, ensure_ascii=False)
            arquivo_json.write('\n')


if __name__ == '__main__':
    aj = ArquivoJson()

    aj.camada = 'bronze'
    aj.termo_pesquisa = 'assunto'
    aj.caminho_particao = f'ano=2024/mes=1/dia=1/dia_semana=segunda-feira/assunto=teste'

    print(aj.caminho_completo)
