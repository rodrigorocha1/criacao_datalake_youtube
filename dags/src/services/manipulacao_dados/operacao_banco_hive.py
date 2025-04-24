try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Tuple, Any
from dags.src.services.manipulacao_dados.iconexao_banco import IConexaoBanco
from dags.src.services.manipulacao_dados.ioperacao_dados import IOperacaoDados


class OperacaoBancoHive(IOperacaoDados):
    def __init__(self, conexao: IConexaoBanco):
        self.__conexao = conexao

    def executar_consulta_dados(self, consulta: str) -> Tuple[bool, Any]:
        try:
            with self.__conexao.obter_conexao() as conn:
                result = conn.execute(consulta)
            return True, result
        except Exception as e:
            print(e)
            return False, None


if __name__ == '__main__':
    from dags.src.services.manipulacao_dados.conexao_banco_hive import ConexaoBancoHive

    conexao_hive = ConexaoBancoHive()
    operacao = OperacaoBancoHive(conexao=conexao_hive)

    id_canal = 'b'
    consulta = f"""
            SELECT *
            FROM canais c 

        """

    sucesso, resultados = operacao.executar_consulta_dados(consulta=consulta)

    for resultado in resultados:
        print(resultado[0])
