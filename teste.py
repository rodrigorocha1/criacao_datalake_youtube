from datetime import datetime

data_atual = datetime.now()

print(data_atual.strftime('%Y-%m-%d %H:%M:%S'))
print(data_atual.year)
print(data_atual.month)
print(data_atual.day)