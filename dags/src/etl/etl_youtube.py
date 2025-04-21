from dags.src.services.apiyoutube.i_api_youtube import IApiYoutube
from dags.src.services.manipulacao_dados.ioperacao_dados import IOperacaoDados
from dags.src.services.manipulacao_dados.arquivo import Arquivo
from datetime import datetime
from dateutil import parser
from typing import Tuple


class ETLYoutube:
    def __init__(self, api_youtube: IApiYoutube, operacoes_dados: IOperacaoDados, arquivo: Arquivo):
        self.__api_youtube = api_youtube
        self.__operacoes_banco = operacoes_dados
        self.__operacoes_arquivo = arquivo

    def __obter_semana_portugues(self, data: datetime) -> str:

        dias_semana = {
            0: 'Segunda-feira',
            1: 'Terça-feira',
            2: 'Quarta-feira',
            3: 'Quinta-feira',
            4: 'Sexta-feira',
            5: 'Sábado',
            6: 'Domingo'
        }

        nome_dia = dias_semana[data.weekday()]

        return nome_dia

    def processo_etl_assunto_video(
            self,
            assunto: str,
            data_publicacao_apos: str,
            data_pesquisa='2025-04-01T00:00:00Z'
    ):
        data = parser.isoparse(data_publicacao_apos)
        ano = data.year
        mes = data.month
        dia = data.day
        dia_semana = self.__obter_semana_portugues(data=data)

        consulta = f"""
                        ALTER TABLE bronze_assunto
                        ADD IF NOT EXISTS PARTITION (
                            ano={ano},
                            mes={mes},
                            dia={dia},
                            dia_semana='{dia_semana}',
                            assunto="{assunto}"
                        )
                        """
        print(consulta)
        dados = self.__operacoes_banco.executar_consulta_dados(consulta=consulta)



        if dados[0]:
            self.__operacoes_arquivo.camada = 'bronze'
            self.__operacoes_arquivo.termo_pesquisa = 'assunto'
            self.__operacoes_arquivo.caminho_particao = f'ano={ano}/mes={mes}/dia={dia}/dia_semana={dia_semana}/assunto={assunto}'
            self.__operacoes_arquivo.nome_arquivo = 'assunto.json'

            for response in self.__api_youtube.obter_assunto(
                    assunto=assunto,
                    data_publicacao_apos=data_publicacao_apos
            ):

                response['data_pesquisa'] = data_pesquisa
                response['assunto'] = assunto
                self.__operacoes_arquivo.guardar_dados(dado=response)
                dados_canais = self.__api_youtube.obter_dados_canais(id_canal=response['snippet']['channelId'])

                if dados_canais[1] == 'BR':
                    dados_canais[0]['data_pesquisa'] = data_pesquisa
                    dados_canais[0]['assunto'] = assunto
                    id_canal = response['snippet']['channelId']
                    nome_canal = response['snippet']['channelTitle']
                    self.__inserir_dados_novos(
                        assunto=assunto,
                        tabela='canais',
                        valor_insercao=(id_canal, nome_canal),
                        coluna_verificacao='id_canal',
                        valor_verificacao=id_canal,

                    )

                    id_video = response['id']['videoId']
                    titulo_video = response['snippet']['title']

                    self.__inserir_dados_novos(
                        assunto=assunto,
                        tabela='videos',
                        valor_insercao=(id_video, titulo_video),
                        coluna_verificacao='id_video',
                        valor_verificacao=id_video

                    )

                # break
        else:
            # Tratamento de erro
            # parar a rotina
            pass

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
        sucesso, resultado = self.__operacoes_banco.executar_consulta_dados(consulta=consulta)
        if sucesso and resultado:
            existe = any(resultado)
            if not existe:
                consulta = f"""
                    INSERT INTO {tabela} 
                    PARTITION (assunto='{assunto}')
                    VALUES {valor_insercao}
                """
                consulta_canal = self.__operacoes_banco.executar_consulta_dados(consulta=consulta)


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
    etl.processo_etl_assunto_video(assunto='Danilo', data_publicacao_apos='2025-04-21T16:50:46Z')
