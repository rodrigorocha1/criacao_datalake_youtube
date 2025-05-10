

# Criando um DataFrame com dados fictícios
import pendulum


lista_assunto = ["No Man's Sky"]
data_hora_atual = pendulum.now('America/Sao_Paulo').to_iso8601_string()
data_hora_atual = pendulum.parse(data_hora_atual)
hora_atual = int(data_hora_atual.hour)
data = data_hora_atual.format('YYYY_MM_DD')
data_hora_busca = data_hora_atual.subtract(minutes=60)
print(data_hora_busca)
data_hora_busca = data_hora_busca.strftime('%Y-%m-%dT%H:%M:%SZ')
print(data_hora_busca, type(data_hora_busca))
