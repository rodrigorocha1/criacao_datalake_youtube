from dags.src.services.apiyoutube.i_api_youtube import IApiYoutube


class ETLYoutube:
    def __init__(self, api_youtube: IApiYoutube):
        self.__api_youtube = api_youtube
