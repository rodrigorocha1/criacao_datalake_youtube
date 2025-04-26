from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
import os
import pickle

# Define o escopo de acesso
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']

# Função para autenticar e obter o serviço da API do YouTube
def authenticate_youtube():
    creds = None
    # O arquivo token.pickle armazena os tokens de acesso e atualização do usuário
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    # Se não houver credenciais válidas, o usuário precisa se autenticar
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secrets.json', SCOPES)
            creds = flow.run_local_server(port=0)

        # Salva as credenciais para o próximo uso
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    # Retorna o serviço da API do YouTube
    youtube = build('youtube', 'v3', credentials=creds)
    return youtube

# Exemplo de uso da função para obter dados da API
youtube = authenticate_youtube()

# Aqui você pode usar o `youtube` para chamar endpoints, por exemplo:
request = youtube.search().list(
    part="snippet",
    q="nomanssky",
    type="video"
)
response = request.execute()

print(response)