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
            SELECT 1
            FROM canais c 
            WHERE c.id_canal = '{id_canal}'
            LIMIT 1 
        """

    sucesso, resultado = operacao.executar_consulta_dados(consulta=consulta)

    if sucesso and resultado:
        existe = any(resultado)
        if existe:
            print(f"Registro com id_canal='{id_canal}' existe.")
        else:
            print(f"Nenhum registro encontrado com id_canal='{id_canal}'.")
    else:
        print("Erro ao executar a consulta ou nenhum resultado retornado.")