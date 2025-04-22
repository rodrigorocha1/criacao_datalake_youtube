import docker
import json
from dags.src.services.apiyoutube.api_youtube import ApiYoutube

# Cliente Docker
client = docker.from_env()

api_youtube = ApiYoutube()

data, _ = api_youtube.obter_dados_canais(id_canal='UCNnROTUy8Zskn44a07F-o-Q')
print(data['items'][0])

# Dados a serem gravados
# data = {'kind': 'youtube#channelListResponse', 'etag': 'a_nm3gteCJsXCfECl7bMw8agUlY',
#         'pageInfo': {'totalResults': 1, 'resultsPerPage': 5}, 'items': [
#         {'kind': 'youtube#channel', 'etag': 'kUjQFSwWphZp7jU01aNPpckvbZE', 'id': 'UCNnROTUy8Zskn44a07F-o-Q',
#          'snippet': {'title': 'Conectado Cortes', 'description': 'Trechos de vídeos do Canal Conectado.\n\n\n',
#                      'customUrl': '@conectadocortes', 'publishedAt': '2018-12-20T18:51:54Z', 'thumbnails': {'default': {
#                  'url': 'https://yt3.ggpht.com/P6UpfEDTjytvEtcYqPqYKr_O2JwK_gURpOpOQhMu7e-cj6T_mNBRjwih2V8-z3hjKqymimUC=s88-c-k-c0x00ffffff-no-rj',
#                  'width': 88, 'height': 88}, 'medium': {
#                  'url': 'https://yt3.ggpht.com/P6UpfEDTjytvEtcYqPqYKr_O2JwK_gURpOpOQhMu7e-cj6T_mNBRjwih2V8-z3hjKqymimUC=s240-c-k-c0x00ffffff-no-rj',
#                  'width': 240, 'height': 240}, 'high': {
#                  'url': 'https://yt3.ggpht.com/P6UpfEDTjytvEtcYqPqYKr_O2JwK_gURpOpOQhMu7e-cj6T_mNBRjwih2V8-z3hjKqymimUC=s800-c-k-c0x00ffffff-no-rj',
#                  'width': 800, 'height': 800}}, 'defaultLanguage': 'pt', 'localized': {'title': 'Conectado Cortes',
#                                                                                        'description': 'Trechos de vídeos do Canal Conectado.\n\n\n'},
#                      'country': 'BR'},
#          'statistics': {'viewCount': '1421', 'subscriberCount': '4360', 'hiddenSubscriberCount': False,
#                         'videoCount': '3'}}]}

json_data = json.dumps(data)

# Nome do container
container_name = 'hadoop_hive_dbt_container'
file_path = '/home/hadoop/datalake/teste/teste313.json'

# Comando para adicionar o JSON ao arquivo dentro do container
command = f"bash -c 'echo {json_data} >> {file_path}'"

try:
    # Obtém o container
    container = client.containers.get(container_name)

    # Executa o comando dentro do container
    exec_log = container.exec_run(cmd=command, stdout=True, stderr=True)

    # Exibe o resultado
    print("Comando executado com sucesso.")
    print("Saída:", exec_log.output.decode())
except docker.errors.NotFound:
    print(f"Container '{container_name}' não encontrado.")
except docker.errors.APIError as e:
    print("Erro ao executar o comando no container:")
    print(e.explanation)
