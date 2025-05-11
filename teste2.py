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

# Escrevendo os dados no arquivo 'usuarios.json'
with open("usuarios.json", "w", encoding="utf-8") as f:
    json.dump(dados, f, ensure_ascii=False, indent=4)
