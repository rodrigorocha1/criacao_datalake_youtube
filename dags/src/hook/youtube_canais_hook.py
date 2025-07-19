from dags.src.hook.youtube_busca_assunto_hook import YotubeHook


class YoutubeBuscaCanaisHook(YotubeHook):
    def __init__(self, conn_id='youtube_default'):
        super().__init__(conn_id=conn_id)

    def _criar_url(self) -> str:
        return self._URL + '/channels/'

    def run(self, **kwargs):
        session = self.get_conn()
        lista_canais = kwargs['id_canais']
        url = self._criar_url()
        params = [
            {
                'part': 'snippet,statistics',
                'id': id_canal,
                'key': self._CHAVE,

            } for id_canal in lista_canais
        ]

        response = self._executar_paginacao(
            url=url,
            session=session,
            params=params
        )

        return response
