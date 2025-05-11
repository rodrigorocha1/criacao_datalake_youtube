

import json

# Inicializa o conjunto
conjunto_videos = set()

# Abre o arquivo
with open("datalake/temp/temp_assunto.jsonl", "r", encoding="utf-8") as arquivo:
    for linha in arquivo:
        dados = json.loads(linha)
        par = (dados["ID_CANAL"])
        conjunto_videos.add(par)

# Exibe o set
for video in conjunto_videos:
    print(video)