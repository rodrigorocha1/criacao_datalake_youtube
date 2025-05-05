
from datetime import datetime
from typing import Dict

import pytz
from unidecode import unidecode

from dags.enums.camada_enum import Camada
from dags.src.services.apiyoutube.i_api_youtube import IApiYoutube
from dags.src.services.manipulacao_dados.arquivo import Arquivo
from dags.src.services.manipulacao_dados.ioperacao_dados import IOperacaoDados


class ETLYoutube:
    def __init__(
            self,
            api_youtube: IApiYoutube,
            operacoes_dados: IOperacaoDados,
            arquivo: Arquivo,
            assunto_pesquisa: str = None

    ):
        self.__api_youtube = api_youtube
        self.__operacoes_banco = operacoes_dados
        self.__operacoes_arquivo = arquivo
        self.__assunto_pesquisa = assunto_pesquisa
        self.__assunto = self.__assunto_pesquisa
        self.__data_coleta = datetime.now(pytz.timezone("America/Sao_Paulo"))
        self.__ano = self.__data_coleta.year
        self.__mes = self.__data_coleta.month
        self.__dia = self.__data_coleta.day
        self.__dia_semana = self.__obter_semana_portugues(self.__data_coleta)

    @property
    def assunto(self):
        return self.__assunto

    @assunto.setter
    def assunto(self, assunto: str):
        self.__assunto = unidecode(assunto).replace(' ', '_').replace("'", "")

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

    def __preparar_caminho_particao(
            self,
            nome_arquivo: str,
            nome_camada: Camada,
            entidade: str,
            opcao_particao: int = 1,

    ):
        """Método para preparar o caminho da partição 

        Args:
            nome_arquivo (str): nome do arquico
            nome_camada (Camada): Enun camanda bronze, prata, ouro, depara
            entidade (str): # Assunto, canal e vídeo
            opcao_particao (int, optional): opção de redirecionamento: 1 para criar 
             a partiçao com tempo, 2 para criar partição do assunto Defaults to 1.
        """
        self.__operacoes_arquivo.camada = nome_camada.value
        self.__operacoes_arquivo.entidade = entidade
        self.__operacoes_arquivo.caminho_particao = (
            f'ano={self.__ano}'
            f'/mes={self.__mes}'
            f'/dia={self.__dia}'
            f'/dia_semana={self.__dia_semana}'
            f'/assunto={self.assunto}'
        ) if opcao_particao == 1 else f'assunto={self.assunto}'
        self.__operacoes_arquivo.nome_arquivo = nome_arquivo

    def __criar_particao(self, tabela_particao: str, opcao_particao: int = 1):
        """Método para criar a partiçao 

        Args:
            tabela_particao (str): tabela_particao
            opcao_particao (int, optional): 1 para criar a partição com base na data
            2 para criar a partição só com assunto. Defaults to 1.
        """

        if opcao_particao == 1:
            consulta = f"""
                        ALTER TABLE {tabela_particao}
                        ADD IF NOT EXISTS PARTITION (
                            ano={self.__ano},
                            mes={self.__mes},
                            dia={self.__dia},
                            dia_semana='{self.__dia_semana.replace(' ', '_')}',
                            assunto="{self.__assunto}"
                    )
                    """
        else:
            consulta = f"""
                ALTER TABLE {tabela_particao}
                ADD IF NOT EXISTS PARTITION (
                    assunto="{self.__assunto}"
                )
            """
        dados = self.__operacoes_banco.executar_consulta_dados(
            consulta=consulta, opcao_consulta=1)

    def __inserir_dados_novos(
            self,
            tabela: str,
            coluna_verificacao: str,
            valor_verificacao: str,
            nome_arquivo: str,
            json_arquivo: Dict,
            entidade: str,
            camada: Camada
    ):
        """_summary_

        Args:
            tabela (str): nome da tabela
            coluna_verificacao (str): coluna do verificação do where
            valor_verificacao (str): valor do where
            nome_arquivo (str): nome do arquivo
            json_arquivo (Dict): json de resposta
            entidade (str): Entidade canal e vídeo
            camada (Camada): Enum Camada
        """

        consulta = f"""
            SELECT 1
            FROM youtube.{tabela} 
            WHERE {coluna_verificacao} = '{valor_verificacao}'
            AND assunto = {self.__assunto} 
            LIMIT 1   
        """

        resultado = self.__operacoes_banco.executar_consulta_dados(
            consulta=consulta, opcao_consulta=2)

        if not resultado:
            self.__preparar_caminho_particao(
                opcao_particao=2,
                nome_arquivo=nome_arquivo,
                entidade=entidade,
                nome_camada=camada.value,
            )

            self.__operacoes_arquivo.guardar_dados(dado=json_arquivo)

    def processo_etl_assunto_video(self, data_publicacao_apos: str):

        self.__preparar_caminho_particao(
            nome_arquivo='assunto.json',
            opcao_particao=1,
            entidade='assunto',
            nome_camada=Camada.Bronze
        )

        self.__criar_particao(
            tabela_particao='bronze_assunto',
            opcao_particao=2,
        )

        for response in self.__api_youtube.obter_assunto(
                assunto=self.__assunto_pesquisa,
                data_publicacao_apos=data_publicacao_apos
        ):
            response['data_pesquisa'] = data_publicacao_apos
            response['assunto'] = self.__assunto
            self.__operacoes_arquivo.guardar_dados(dado=response)
            dados_canais = self.__api_youtube.obter_dados_canais(
                id_canal=response['snippet']['channelId']
            )

            if dados_canais[1] == 'BR':
                dados_canais[0]['data_pesquisa'] = data_publicacao_apos
                dados_canais[0]['assunto'] = self.__assunto

                id_canal = response['snippet']['channelId']
                nome_canal = response['snippet']['channelTitle']
                json_canal = {'id_canal': id_canal, 'nome_canal': nome_canal}
                print(f'Canal Brasileiro: {json_canal}')

                self.__inserir_dados_novos(
                    tabela='canais',
                    nome_arquivo='canais.json',
                    coluna_verificacao='id_canal',
                    valor_verificacao=id_canal,
                    json_arquivo=json_canal,
                    entidade='canais',
                    camada=Camada.Depara

                )

                id_video = response['id']['videoId']
                titulo_video = response['snippet']['title']

                json_video = {'id_video': id_video,
                              'titulo_video': titulo_video}
                print(f'Vídeo Brasileiro {json_video}')
                self.__inserir_dados_novos(

                    tabela='videos',
                    nome_arquivo='videos.json',
                    coluna_verificacao='id_video',
                    valor_verificacao=id_video,
                    json_arquivo=json_video,
                    entidade='videos',
                    camada=Camada.Depara

                )

    def processo_etl_canal(self):

        self.__preparar_caminho_particao(
            nome_arquivo='canal.json',
            nome_camada=Camada.Bronze,
            entidade='canais',
            opcao_particao=1,
            assunto_tratado=self.__assunto
        )

        consulta = f"""
            SELECT DISTINCT c.id_canal
            FROM youtube.canais c 
            where c.assunto = "{self.__assunto}"
        """
        sucesso, resultados = self.__operacoes_banco.executar_consulta_dados(
            consulta=consulta, opcao_consulta=2)
        print('Resultados canais')
        print(sucesso, resultados)
        if sucesso:

            self.__criar_particao(tabela_particao='bronze_canais')

            for resultado in resultados:
                if resultado is not None:
                    id_canal = resultado[0]
                    response, _ = self.__api_youtube.obter_dados_canais(
                        id_canal=id_canal)
                    response = response['items'][0]
                    response['data_pesquisa'] = self.__data_coleta.strftime(
                        '%Y-%m-%d %H:%M:%S')
                    response['assunto'] = self.__assunto
                    self.__operacoes_arquivo.guardar_dados(dado=response)

                else:
                    pass

        else:
            pass
            # tratamento de erro

    def processo_etl_video(self):

        self.__preparar_caminho_particao(

            nome_arquivo='video.json',
            assunto_tratado=self.__assunto,
            nome_camada=Camada.Bronze.value,
            entidade='videos'

        )

        consulta = f"""
                    SELECT DISTINCT v.id_video
                    FROM youtube.videos v 
                    where v.assunto = "{self.__assunto}"
                """
        sucesso, resultados = self.__operacoes_banco.executar_consulta_dados(
            consulta=consulta, opcao_consulta=2)

        if sucesso:
            self.__criar_particao(tabela_particao='bronze_videos')
            for resultado in resultados:
                if resultado[0]:
                    id_video = resultado[0]
                    response = self.__api_youtube.obter_dados_videos(
                        id_video=id_video)
                    response['data_pesquisa'] = self.__data_coleta.strftime(
                        '%Y-%m-%d %H:%M:%S')
                    response['assunto'] = self.__assunto
                    self.__operacoes_arquivo.guardar_dados(dado=response)

                else:
                    pass

        else:
            pass
            # tratamento de erro


if __name__ == '__main__':
    from dags.src.services.apiyoutube.api_youtube import ApiYoutube
    from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson
    from dags.src.services.manipulacao_dados.conexao_banco_hive import ConexaoBancoHive
    from dags.src.services.manipulacao_dados.operacao_banco_hive import OperacaoBancoHive

    etl = ETLYoutube(
        api_youtube=ApiYoutube(),
        arquivo=ArquivoJson(),
        operacoes_dados=OperacaoBancoHive(
            conexao=ConexaoBancoHive()
        )
    )
    etl.assunto = "No Man's Sky"
    etl.processo_etl_assunto_video(data_publicacao_apos='2025-01-05T20:20:46Z')

    # etl.processo_etl_canal(assunto='Danilo', data_pesquisa='2025-04-23T18:50:46Z')
    # etl.processo_etl_video(assunto='Danilo', data_pesquisa='2025-04-23T18:50:46Z')
