from airflow.models import Variable


class Configuracao:
    url = Variable.get('URL_YOUTUBE')
    chave = Variable.get('CHAVE_YOUTUBE')
