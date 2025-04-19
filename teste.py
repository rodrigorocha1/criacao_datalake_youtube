from dags.src.services.apiyoutube.api_youtube import ApiYoutube
import pendulum

data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
hora_atual = int(data_hora_atual.hour)
data = data_hora_atual.format('YYYY_MM_DD')
data_hora_busca = data_hora_atual.subtract(hours=7)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
print(data_hora_busca)
ay = ApiYoutube()

ay.obter_assunto(assunto='Python', data_publicacao_apos=data_hora_busca)

