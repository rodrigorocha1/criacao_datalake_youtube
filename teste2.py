import subprocess
import json

# Dados iniciais a serem inseridos
data_initial = {"id": 1, "nome": "Alice", "idade": 30}

# Dados adicionais para o append
data_append = {"id": 3, "nome": "Charlie", "idade": 28}

# Caminho do arquivo no HDFS
hdfs_file_path = "/foo/data.json"


# Função para inserir dados diretamente no HDFS
def write_to_hdfs(data, hdfs_file_path):
    # Converte os dados em JSON para uma string
    json_data = json.dumps(data, indent=4) + '\n'

    # Usa subprocess para fazer o append diretamente no HDFS
    process = subprocess.Popen(
        ["docker", "exec", "-i", "namenode", "hadoop", "fs", "-appendToFile", "-", hdfs_file_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Envia os dados no formato JSON diretamente para o HDFS
    process.communicate(input=json_data.encode())


# Função para exibir o conteúdo do arquivo JSON no HDFS
def display_json_from_hdfs(hdfs_file_path):
    process = subprocess.Popen(
        ["docker", "exec", "-i", "namenode", "hadoop", "fs", "-cat", hdfs_file_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Lê a saída e imprime o conteúdo do arquivo
    stdout, stderr = process.communicate()

    if process.returncode == 0:
        print(stdout.decode())  # Exibe o conteúdo do arquivo JSON
    else:
        print(f"Erro ao ler o arquivo: {stderr.decode()}")


# Exibe o conteúdo inicial do arquivo JSON (antes da atualização)
print("Conteúdo do JSON antes da atualização:")
display_json_from_hdfs(hdfs_file_path)

# Passo 1: Inicializar o arquivo com dados
write_to_hdfs(data_initial, hdfs_file_path)

# Exibe o conteúdo após a inicialização (agora o arquivo deve conter os dados iniciais)
print("\nConteúdo do JSON após a primeira inserção:")
display_json_from_hdfs(hdfs_file_path)

# Passo 2: Fazer o append com novos dados
write_to_hdfs(data_append, hdfs_file_path)

# Exibe o conteúdo após o append (dados adicionais foram inseridos)
print("\nConteúdo do JSON após a atualização (append):")
display_json_from_hdfs(hdfs_file_path)
