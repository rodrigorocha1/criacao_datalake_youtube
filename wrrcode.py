from unidecode import unidecode
import pendulum

assunto = "No Man's Sky"
assunto = ''.join(filter(lambda c: c.isalnum() or c.isspace(), unidecode(assunto))).replace(' ', '').lower()
print(assunto)

data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
print(type(data_hora_atual))
hora_atual = int(data_hora_atual.hour)
data = data_hora_atual.format('YYYY_MM_DD')
data_hora_busca = data_hora_atual.subtract(days=1)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
print(data_hora_busca)
