

try:
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from datetime import datetime
from airflow.models import BaseOperator
from abc import ABC, abstractmethod
from dags.src.hook.youtube_hook import YotubeHook
from dags.src.services.manipulacao_dados.ioperacao_dados import IOperacaoDados
from typing import Dict
import pendulum
from pendulum.datetime import DateTime
from unidecode import unidecode


class YoutubeOperator(BaseOperator, ABC):
    def __init__(
            self,
            task_id,
            assunto: str,
            operacao_hook: YotubeHook,
            operacao_banco: IOperacaoDados,

            **kwargs
    ):
        self._operacao_hook = operacao_hook
        self._operacao_banco = operacao_banco
        self._assunto = unidecode(assunto.replace(' ', '_').replace("'", "")).lower()
        self._data = pendulum.parse(pendulum.now('America/Sao_Paulo').to_iso8601_string())
        super().__init__(task_id=task_id, **kwargs)

    def __obter_semana_portugues(self, data: DateTime) -> str:
        dias_semana = {
            0: 'Segunda-feira',
            1: 'Terca-feira',
            2: 'Quarta-feira',
            3: 'Quinta-feira',
            4: 'Sexta-feira',
            5: 'Sabado',
            6: 'Domingo'
        }

        nome_dia = dias_semana[data.weekday()]

        return nome_dia.replace(" ", "_")

    def _executar_consulta_canal_bronze(self) -> str:
        consulta = f"""
            select  bc.id
            from youtube.bronze_canais bc  
            where bc.assunto = '{self._assunto}'

        """

        return consulta

    def _criar_caminho_particao(self) -> str:
        caminho_particao = (
            f"ano={self._data.year}/"
            f"mes={self._data.month}/"
            f"dia={self._data.day}/"
            f"dia_semana={self.__obter_semana_portugues(data=self._data).replace(' ', '_')}/"
            f"assunto={self._assunto}"
        )

        return caminho_particao

    def _criar_particao_datalake_camada(self, tabela_particao: str) -> str:
        """
        Método para criar na partição na camada bronze
        :param tabela_particao:
        :return:
        """
        consulta = f"""
            ALTER TABLE {tabela_particao}
            ADD IF NOT EXISTS PARTITION (
                ano={self._data.year},
                mes={self._data.month},
                dia={self._data.day},
                dia_semana='{self.__obter_semana_portugues(data=self._data).replace(' ', '_')}',
                assunto="{self._assunto}"
            )
        """
        return consulta

    def _criar_particao_datalake_depara(self, tabela_particao: str) -> str:
        """
        Métpdp para criar a partição
        :param tabela_particao: tabela do banco
        :return: consulta
        """
        consulta = f"""
            ALTER TABLE {tabela_particao}
            ADD IF NOT EXISTS PARTITION (
            assunto="{self.__assunto}"
        )
        """
        return consulta

    @abstractmethod
    def gravar_dados(self, req: Dict):
        pass

    @abstractmethod
    def execute(self, context):
        pass
