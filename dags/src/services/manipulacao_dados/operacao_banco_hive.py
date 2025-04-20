from dags.src.services.manipulacao_dados.conexao_banco_hive import ConexaoBancoHive


class OperacaoBancoHive:
    def __init__(self):
        self.__conexao = ConexaoBancoHive()

    def executar_consulta(self, consulta: str):
        with self.__conexao.obter_conexao() as conn:
            result = conn.execute(consulta)

        return result
