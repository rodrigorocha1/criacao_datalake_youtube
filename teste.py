import docker

# Cria o cliente Docker
client = docker.from_env()

# Função para listar diretórios de um container
def listar_diretorios(container_name, caminho="/"):
    try:
        # Obtém o container específico pelo nome ou ID
        container = client.containers.get(container_name)

        # Executa o comando 'ls' dentro do container para listar os diretórios
        resultado = container.exec_run(f"ls -l {caminho}")

        # Exibe o resultado
        if resultado.exit_code == 0:
            print(f"Conteúdo do diretório {caminho} no container {container_name}:")
            print(resultado.output.decode('utf-8'))
        else:
            print(f"Erro ao listar diretórios: {resultado.output.decode('utf-8')}")
    except docker.errors.NotFound:
        print(f"Container {container_name} não encontrado.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

# Exemplo de uso: Listar diretórios de um container específico
container_name = "namenode"  # Substitua pelo nome ou ID do seu container
listar_diretorios(container_name, "/")
