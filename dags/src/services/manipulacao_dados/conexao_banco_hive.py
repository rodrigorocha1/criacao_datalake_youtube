try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from dags.src.services.manipulacao_dados.iconexao_banco import IConexaoBanco
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection


class ConexaoBancoHive(IConexaoBanco):
    def __init__(self):
        self.__host = '172.18.0.4'
        self.__port = 10000
        self.__username = 'rodrigo'
        self.__password = 'rodrigo3'
        self.__database = 'youtube'
        self.__auth = 'LDAP'

    def obter_conexao(self) -> Connection:
        url_hive = f'hive://{self.__username}@{self.__host}:{self.__port}/{self.__database}'
        engine = create_engine(url_hive)
        return engine.connect()


if __name__ == '__main__':
    c = ConexaoBancoHive()
    c.obter_conexao()
