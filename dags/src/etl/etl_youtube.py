
from datetime import datetime
from typing import Tuple

from unidecode import unidecode

from dags.src.services.apiyoutube.i_api_youtube import IApiYoutube
from dags.src.services.manipulacao_dados.arquivo import Arquivo
from dags.src.services.manipulacao_dados.ioperacao_dados import IOperacaoDados


class ETLYoutube:
    def __init__(
            self,
            api_youtube: IApiYoutube,
            operacoes_dados: IOperacaoDados,
            arquivo: Arquivo,

    ):
        self.__api_youtube = api_youtube
        self.__operacoes_banco = operacoes_dados
        self.__operacoes_arquivo = arquivo
        self.__assunto = None
        self.__data_coleta = datetime.now()
        self.__ano = self.__data_coleta.year
        self.__mes = self.__data_coleta.month
        self.__dia = self.__data_coleta.day
        self.__dia_semana = self.__obter_semana_portugues(self.__data_coleta)

    @property
    def assunto(self):
        return self.__assunto

    @assunto.setter
    def assunto(self, assunto: str):
        self.__assunto = assunto


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

        return nome_dia

    def __fazer_tratamento_assunto(self, assunto: str) -> str:
        assunto = assunto.replace("'", "").replace(' ', '_')
        return assunto

    def __preparar_caminho_particao(self, termo_pesquisa: str, nome_arquivo: str, assunto_tratado: str):
        self.__operacoes_arquivo.camada = 'bronze'
        self.__operacoes_arquivo.termo_pesquisa = termo_pesquisa
        self.__operacoes_arquivo.caminho_particao = (
            f'ano={self.__ano}'
            f'/mes={self.__mes}'
            f'/dia={self.__dia}'
            f'/dia_semana={self.__dia_semana.replace(" ", "_")}'
            f'/assunto={assunto_tratado}'
        )
        self.__operacoes_arquivo.nome_arquivo = nome_arquivo

    def __criar_particao(self, tabela_particao: str):
        consulta = f"""
                    ALTER TABLE {tabela_particao}
                    ADD IF NOT EXISTS PARTITION ( 
                        ano={self.__ano},
                        mes={self.__mes},
                        dia={self.__dia},
                        dia_semana='{self.__dia_semana.replace(' ', '_')}',
                        assunto="{unidecode(self.__assunto).replace(' ', '_').replace("'", "")}"
                )
                """
        dados = self.__operacoes_banco.executar_consulta_dados(consulta=consulta , opcao_consulta=1)


    def __inserir_dados_novos(
            self,
            assunto: str,
            tabela: str,
            coluna_verificacao: str,
            valor_verificacao: str,
            valor_insercao: Tuple[str, str]
    ):

        consulta = f"""
            SELECT 1
            FROM {tabela} 
            WHERE {coluna_verificacao} = '{valor_verificacao}'
            LIMIT 1   
        """
        sucesso, resultado = self.__operacoes_banco.executar_consulta_dados(consulta=consulta, opcao_consulta=2)
        print(sucesso, resultado)

        if not resultado:
            print('não existe')
            consulta = f"""
                    INSERT INTO {tabela} 
                    PARTITION (assunto="{assunto}")
                    VALUES {valor_insercao}
                """
            consulta_canal = self.__operacoes_banco.executar_consulta_dados(consulta=consulta, opcao_consulta=1)

    def processo_etl_assunto_video(self, data_publicacao_apos: str):
        assunto_tratado = self.__fazer_tratamento_assunto(assunto=self.__assunto)
        self.__preparar_caminho_particao(
            termo_pesquisa='assunto',
            nome_arquivo='asssunto.json',
            assunto_tratado=assunto_tratado
        )
        self.__criar_particao(tabela_particao='bronze_assunto')
        for response in self.__api_youtube.obter_assunto(
                assunto=self.__assunto,
                data_publicacao_apos=data_publicacao_apos
        ):
            response['data_pesquisa'] = data_publicacao_apos
            response['assunto'] = self.__assunto
            self.__operacoes_arquivo.guardar_dados(dado=response)
            dados_canais = self.__api_youtube.obter_dados_canais(id_canal=response['snippet']['channelId'])
            if dados_canais[1] == 'BR':
                dados_canais[0]['data_pesquisa'] = data_publicacao_apos
                dados_canais[0]['assunto'] = self.__assunto
                id_canal = response['snippet']['channelId']
                nome_canal = response['snippet']['channelTitle']
                print('Canal Brasileiro', id_canal)
                print('Video Brasilero')

                self.__inserir_dados_novos(
                    assunto=assunto_tratado,
                    tabela='canais',
                    valor_insercao=(id_canal, nome_canal),
                    coluna_verificacao='id_canal',
                    valor_verificacao=id_canal,

                )

                id_video = response['id']['videoId']
                titulo_video = response['snippet']['title']

                self.__inserir_dados_novos(
                    assunto=assunto_tratado,
                    tabela='videos',
                    valor_insercao=(id_video, titulo_video),
                    coluna_verificacao='id_video',
                    valor_verificacao=id_video

                )

    def processo_etl_canal(self):
        assunto_tratado = self.__fazer_tratamento_assunto(assunto=self.__assunto)
        self.__preparar_caminho_particao(
            termo_pesquisa='canal',
            nome_arquivo='canal.json',
            assunto_tratado=assunto_tratado
        )

        consulta = f"""
            SELECT DISTINCT c.id_canal
            FROM canais c 
            where c.assunto = "{assunto_tratado}"
        """
        sucesso, resultados = self.__operacoes_banco.executar_consulta_dados(consulta=consulta, opcao_consulta=2)
        print('Resultados canais')
        print(sucesso, resultados)
        if sucesso:

            self.__criar_particao(tabela_particao='bronze_canais')

            for resultado in resultados:
                if resultado is not None:
                    id_canal = resultado[0]
                    response, _ = self.__api_youtube.obter_dados_canais(id_canal=id_canal)
                    response = response['items'][0]
                    response['data_pesquisa'] = self.__data_coleta.strftime('%Y-%m-%d %H:%M:%S')
                    response['assunto'] = self.__assunto
                    self.__operacoes_arquivo.guardar_dados(dado=response)

                else:
                    pass

        else:
            pass
            # tratamento de erro

    def processo_etl_video(self):
        assunto_tratado = self.__fazer_tratamento_assunto(assunto=self.__assunto)
        self.__preparar_caminho_particao(
            termo_pesquisa='video',
            nome_arquivo='video.json',
            assunto_tratado=assunto_tratado
        )

        consulta = f"""
                    SELECT DISTINCT v.id_video
                    FROM videos v 
                    where v.assunto = "{assunto_tratado}"
                """
        sucesso, resultados = self.__operacoes_banco.executar_consulta_dados(consulta=consulta , opcao_consulta=2)

        if sucesso:
            self.__criar_particao(tabela_particao='bronze_videos')
            for resultado in resultados:
                print(resultado)
                if resultado[0]:
                    id_video = resultado[0]
                    response = self.__api_youtube.obter_dados_videos(id_video=id_video)
                    response['data_pesquisa'] = self.__data_coleta.strftime('%Y-%m-%d %H:%M:%S')
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
    # etl.processo_etl_assunto_video(assunto='Danilo', data_publicacao_apos='2025-04-23T18:50:46Z')
    # etl.processo_etl_canal(assunto='Danilo', data_pesquisa='2025-04-23T18:50:46Z')
    # etl.processo_etl_video(assunto='Danilo', data_pesquisa='2025-04-23T18:50:46Z')
