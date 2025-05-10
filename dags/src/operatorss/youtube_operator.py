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
from typing import Dict
import pendulum
from pendulum.datetime import DateTime


class YoutubeOperator(BaseOperator, ABC):
    def __init__(
            self,
            task_id,
            assunto: str,
            operacao_hook: YotubeHook,

            **kwargs
    ):
        self._operacao_hook = operacao_hook
        self._assunto = assunto.replace(' ', '_')
        self._data = pendulum.parse(pendulum.now('America/Sao_Paulo').to_iso8601_string())
        super().__init__(task_id=task_id, **kwargs)

    def __obter_semana_portugues(self, data: DateTime) -> str:
        dias_semana = {
            0: 'Segunda-feira',
            1: 'Terça-feira',
            2: 'Quarta-feira',
            3: 'Quinta-feira',
            4: 'Sexta-feira',
            5: 'Sabado',
            6: 'Domingo'
        }

        nome_dia = dias_semana[data.weekday()]

        return nome_dia.replace(" ", "_")

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
        consulta = f"""
            ALTER TABLE {tabela_particao}
            ADD IF NOT EXISTS PARTITION (
                ano={self._data.year},
                mes={self._data.month},
                dia={self._data.year},
                dia_semana='{self.__obter_semana_portugues(data=self._data).replace(' ', '_')}',
                assunto="{self._assunto}"
            )
        """
        return consulta

    def _criar_particao_datalake_depara(self, tabela_particao: str) -> str:
        consulta = f"""
            ALTER TABLE {tabela_particao}
            ADD IF NOT EXISTS PARTITION (
            ssunto="{self.__assunto}"
        )
        """
        return consulta

    @abstractmethod
    def gravar_dados(self, req: Dict):
        pass

    @abstractmethod
    def execute(self, context):
        pass
