from typing import Tuple, Any
from dags.src.services.manipulacao_dados.iconexao_banco import IConexaoBanco
from dags.src.services.manipulacao_dados.ioperacao_dados import IOperacaoDados


class OperacaoBancoHive(IOperacaoDados):
    def __init__(self, conexao: IConexaoBanco):
        self.__conexao = conexao

    def executar_consulta(self, consulta: str) -> Tuple[bool, Any]:
        try:
            with self.__conexao.obter_conexao() as conn:
                result = conn.execute(consulta)
            return True, result
        except Exception as e:
            return False, None


if __name__ == '__main__':
    from dags.src.services.manipulacao_dados.conexao_banco_hive import ConexaoBancoHive

    obh = OperacaoBancoHive(conexao=ConexaoBancoHive())

    consulta = f"""
           ALTER TABLE bronze_assunto
           ADD IF NOT EXISTS PARTITION (
               ano=2025,
               mes=4,
               dia=11,
               dia_semana='segunda',
               assunto="teste"
           )
           """

    a = obh.executar_consulta(consulta=consulta)
    print(a)
