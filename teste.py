import docker

# Conectando ao Docker
client = docker.from_env()

# Nome do container existente (substitua pelo nome do seu container)
container_name = 'hadoop_hive_dbt_container'

# Obtenha o container pelo nome
container = client.containers.get(container_name)

# Defina o caminho do arquivo que deseja criar dentro do container
container_file_path = '/arquivo.txt'

# Conteúdo do arquivo
file_content = "Este é um arquivo criado dentro do container Docker."

# Escreva o conteúdo no arquivo dentro do container
container.exec_run(f'echo "{file_content}" > sudo tee {container_file_path}')

print(f"Arquivo {container_file_path} criado no container {container_name}.")
