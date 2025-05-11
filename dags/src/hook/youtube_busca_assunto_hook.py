from dags.src.hook.youtube_hook import YotubeHook


class YoutubeBuscaAssuntoHook(YotubeHook):

    def __init__(self, data_publicacao: str, assunto_pesquisa=str, conn_id='youtube_default'):
        self.__data_publicacao = data_publicacao
        self.__assunto_pesquisa = assunto_pesquisa
        super().__init__(conn_id=conn_id)

    def _criar_url(self) -> str:
        """Retorna a url

        Returns:
            str: _description_
        """
        return self._URL + '/search/'

    def run(self):
        """Método para rodar a dag

        Returns:
            _type_: _description_
        """

        session = self.get_conn()

        url = self._criar_url()
        params = [
            {
                'part': 'snippet',
                'key': self._CHAVE,
                'regionCode': 'BR',
                'relevanceLanguage': 'pt',
                'maxResults': '50',
                'publishedAfter': self.__data_publicacao,
                'q': self.__assunto_pesquisa,
                'pageToken': ''
            }
        ]

        response = self._executar_paginacao(
            url=url,
            session=session,
            params=params
        )

        return response


if __name__ == '__main__':
    for dados in YoutubeBuscaAssuntoHook(data_publicacao='2025-05-10T10:50:46Z',
                                         assunto_pesquisa='python',
                                         conn_id='youtube_default').run():
        print(dados)
