from dags.src.services.apiyoutube.i_api_youtube import IApiYoutube


class ETLYoutube:
    def __init__(self, api_youtube: IApiYoutube):
        self.__api_youtube = api_youtube
        self.__arquivo_hadoop = None

    def processo_etl_assunto_video(self, assunto: str, data_publicacao_apos: str):
        for response in self.__api_youtube.obter_assunto(assunto=assunto, data_publicacao_apos=data_publicacao_apos):
            print(response)
