from dotenv import load_dotenv
from googleapiclient.discovery import build
import os

# Carrega as variáveis de ambiente
load_dotenv()

# Recupera a API key do arquivo .env
API_KEY = os.environ['API_KEY']

# Cria o cliente da API do YouTube
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Consulta de busca
consulta = 'No Man\'s Sky'  # Corrigido o uso da aspa dupla

# Requisição para buscar vídeos
request = youtube.search().list(
    q=consulta,
    part='id,snippet',
    type='video',  # Corrigido de 'vide' para 'video'
    maxResults=50,
    publishedAfter='2025-04-01T00:00:00Z',
    order='date'
)

# Executa a requisição
response = request.execute()

# Imprime os títulos dos vídeos encontrados
for item in response.get('items', []):
    title = item['snippet']['title']
    print(title)

# Requisição para buscar informações de um canal
channel_request = youtube.channels().list(
    id='UCJAyNFlzJDkU4ebLXT6uByQ',
    part='snippet,statistics'
)
channel_response = channel_request.execute()
print(channel_response)
# Extrai e imprime o ID do canal
request = youtube.videos().list(
    part="snippet,contentDetails,statistics",
    id='-CAzA5bDy3E'
)

# Executa a requisição e obtém a resposta
response = request.execute()
print('*' * 20)
print(response)