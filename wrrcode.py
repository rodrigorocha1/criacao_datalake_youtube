import json

# Lista com os três objetos JSON
dados = [
    {
        "nome": "Fernanda Oliveira",
        "idade": 42,
        "email": "fernanda.oliveira@example.com"
    },
    {
        "nome": "Fernanda Oliveira",
        "idade": 42,
        "email": "fernanda.oliveira@example.com"
    },
    {
        "nome": "Fernanda Oliveira",
        "idade": 42,
        "email": "fernanda.oliveira@example.com"
    }
]

# Escrevendo cada objeto em uma linha no arquivo 'usuarios.jsonl'
with open("usuarios.json", "w", encoding="utf-8") as f:
    for item in dados:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

print("Arquivo 'usuarios.jsonl' criado com sucesso.")

with open("usuarios.json", "r", encoding="utf-8") as f:
    dados = [json.loads(linha) for linha in f]

# Exibindo os dados lidos
for item in dados:
    print(item)
