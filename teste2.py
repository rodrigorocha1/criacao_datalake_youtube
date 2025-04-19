import pandas as pd

# Criando um DataFrame com dados fictícios
dados = {
    "ANO": [2024, 2024],
    "MES": [3, 3],
    "DIA": [29, 30],
    "ID_CANAL": ["UC123", "UC123"],
    "NM_CANAL": ["Canal Exemplo", "Canal Exemplo"],
    "ID_VIDEO": ["VID001", "VID002"],
    "NM_VIDEO": ["Video A", "Video B"],
    "TOTAL_VISUALIZACOES": [10000, 15000],
    "TOTAL_VISUALIZACOES_DIA": [5000, 7500],
    "TOTAL_INSCRITOS": [2000, 2000],  # Número total de inscritos
    "TOTAL_CURTIDAS": [500, 800],
    "TOTAL_COMENTARIOS": [100, 150],
}

df = pd.DataFrame(dados)

# Calculando o total de engajamento (curtidas + comentários)
df["TOTAL_ENGAJAMENTO"] = df["TOTAL_CURTIDAS"] + df["TOTAL_COMENTARIOS"]

# Calculando a Taxa de Engajamento por Inscrito
df["Taxa de Engajamento por Inscrito (%)"] = (df["TOTAL_ENGAJAMENTO"] / df["TOTAL_INSCRITOS"]) * 100

# Exibindo o DataFrame
print(df[["ID_VIDEO", "NM_VIDEO", "TOTAL_ENGAJAMENTO", "TOTAL_INSCRITOS", "Taxa de Engajamento por Inscrito (%)"]])
df.to_csv('docs/taxa_eng_inscritos.csv)