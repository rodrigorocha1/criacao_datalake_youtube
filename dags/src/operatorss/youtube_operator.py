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
from typing import Dict, Optional


class YoutubeOperator(BaseOperator, ABC):
    def __init__(
            self,
            task_id,
            assunto: str,
            operacao_hook: YotubeHook,

            **kwargs
    ):
        self._operacao_hook = operacao_hook
        self._assunto = assunto
        super().__init__(task_id=task_id, **kwargs)

    def __obter_semana_portugues(self, data: datetime) -> str:

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

    def __criar_particao_datalake_camada(self, tabela_particao: str, assunto: str):
        consulta = f"""
            ALTER TABLE {tabela_particao}
            ADD IF NOT EXISTS PARTITION (
                ano={self.__ano},
                mes={self.__mes},
                dia={self.__dia},
                dia_semana='{self.__dia_semana.replace(' ', '_')}',
                assunto="{assunto}"
            )
        """


    @abstractmethod
    def gravar_dados(self, req: Dict):
        pass

    @abstractmethod
    def execute(self, context):
        pass
    