FROM apache/superset:latest

USER root

# Instala dependências para compilar psycopg2 e outras libs necessárias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libsasl2-dev \
        python3-dev \
        libldap2-dev \
        libssl-dev \
        libpq-dev \
        build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Instala psycopg2-binary usando pip
RUN pip install --no-cache-dir psycopg2-binary

USER superset
