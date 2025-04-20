from dags.src.services.apiyoutube.i_api_youtube import IApiYoutube


class ETLYoutube:
    def __init__(self, api_youtube: IApiYoutube):
        self.__api_youtube = api_youtube
        self.__arquivo_hadoop = None

    def processo_etl_assunto_video(
            self,
            assunto: str,
            data_publicacao_apos: str,
            data_pesquisa='2025-04-01T00:00:00Z'
    ):
        for response in self.__api_youtube.obter_assunto(assunto=assunto, data_publicacao_apos=data_publicacao_apos):
            print(response)
            response['data_pesquisa'] = data_pesquisa
            response['assunto'] = assunto
            print(response)
            print(response['id']['videoId'], response['snippet']['title'])
            print(response['snippet']['channelId'], response['snippet']['channelTitle'])
            dados_canais = self.__api_youtube.obter_dados_canais(id_canal=response['snippet']['channelId'])


if __name__ == '__main__':
    from dags.src.services.apiyoutube.api_youtube import ApiYoutube

    etl_youtube = ETLYoutube(api_youtube=ApiYoutube())
