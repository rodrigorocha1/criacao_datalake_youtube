from dags.src.services.manipulacao_dados.arquivo_json import ArquivoJson

aj = ArquivoJson(
    camada='bronze',
    entidade='assunto',
    caminho_particao=None,
    nome_arquivo='teste.json',
    opcao=1
)
print(aj.__dict__)
print(aj.caminho_particao)
aj.caminho_particao = 'ano=2024/'
print(aj.__dict__)
print(aj.caminho_particao)
print(aj.caminho_completo)