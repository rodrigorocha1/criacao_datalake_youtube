try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from typing import Tuple, Any
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from dags.src.services.manipulacao_dados.ioperacao_dados import IOperacaoDados


class OperacaoBancoHiveAirflow(IOperacaoDados):
    def __init__(self):
        self.__id = 'id_hive'
        self.__schema = 'youtube'
        self.__hook = HiveServer2Hook(hiveserver2_conn_id=self.__id)

    def executar_consulta_dados(self, consulta: str, opcao_consulta: int) -> Tuple[bool, Any]:
        try:

            with self.__hook.get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(f'USE {self.__schema}')
                if opcao_consulta == 1:
                    result = cursor.execute(consulta)
                else:
                    result =self.__hook.get_records(consulta)
                print('Resultado Consulta')
                print(result)


                return True, result
        except Exception as e:
            print(e)
            return False, None
