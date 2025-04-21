from dags.src.services.apiyoutube.api_youtube import ApiYoutube
import pendulum

data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
hora_atual = int(data_hora_atual.hour)
data = data_hora_atual.format('YYYY_MM_DD')
# data_hora_busca = data_hora_atual.subtract(hours=7)

data_hora_busca = data_hora_atual.strftime('%Y-%m-%dT%H:%M:%SZ')

from dateutil import parser

data_str = '2025-04-21T00:00:00Z'
data_dt = parser.isoparse(data_str)
print(type(data_dt))
print(data_dt.strftime('%A'))


dias_semana = {
    0: 'Segunda-feira',
    1: 'Terça-feira',
    2: 'Quarta-feira',
    3: 'Quinta-feira',
    4: 'Sexta-feira',
    5: 'Sábado',
    6: 'Domingo'
}
nome_dia_pt = dias_semana[data_dt.weekday()]
print(nome_dia_pt)

