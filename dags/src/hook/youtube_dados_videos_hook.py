from dags.src.hook.youtube_hook import YotubeHook


class YoutubeVideoHook(YotubeHook):

    def __init__(self, conn_id='youtube_default'):
        super().__init__(conn_id=conn_id)

    def _criar_url(self) -> str :
        return self._URL + '/videos/'

    def run(self, **kwargs):
        session = self.get_conn()
        lista_video = kwargs['ids_videos']


        url = self._criar_url()
        params = []
        for id_video in lista_video:
            param = {
                'part': 'statistics,contentDetails,id,snippet,status',
                'id': id_video,
                'key': self._CHAVE,
                'regionCode': 'BR',
                'pageToken': ''

            }
            params.append(param)

        response = self._executar_paginacao(
            url=url,
            session=session,
            params=params
        )
        return response
