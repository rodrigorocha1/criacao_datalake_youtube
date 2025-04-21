from dags.src.services.apiyoutube.i_api_youtube import IApiYoutube
from dags.src.services.manipulacao_dados.ioperacao_banco import IOperacaoBanco
from datetime import datetime


class ETLYoutube:
    def __init__(self, api_youtube: IApiYoutube, operacoes_banco: IOperacaoBanco):
        self.__api_youtube = api_youtube
        self.__operacoes_banco = operacoes_banco

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
        # Criar partição
        for response in self.__api_youtube.obter_assunto(assunto=assunto, data_publicacao_apos=data_publicacao_apos):
            print(response)
            response['data_pesquisa'] = data_pesquisa
            response['assunto'] = assunto
            print(response)
            print(response['id']['videoId'], response['snippet']['title'])
            print(response['snippet']['channelId'], response['snippet']['channelTitle'])
            dados_canais = self.__api_youtube.obter_dados_canais(id_canal=response['snippet']['channelId'])
            # Gravar dados Vídeos
            if dados_canais[1] == 'BR':
                dados_canais[0]['data_pesquisa'] = data_pesquisa
                dados_canais[0]['assunto'] = assunto
                # Gravar dados canais
                print(dados_canais[0])
            break


if __name__ == '__main__':
    from dags.src.services.apiyoutube.api_youtube import ApiYoutube
    from dags.src.services.manipulacao_dados.operacao_banco_hive import OperacaoBancoHive
    from dags.src.services.manipulacao_dados.conexao_banco_hive import ConexaoBancoHive

    etl_youtube = ETLYoutube(
        api_youtube=ApiYoutube(),
        operacoes_banco=OperacaoBancoHive(
            conexao=ConexaoBancoHive()
        )

    )
    etl_youtube.processo_etl_assunto_video(
        assunto='python',
        data_publicacao_apos='2025-03-23T10:50:46Z'
    )
