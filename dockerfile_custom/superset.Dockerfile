FROM apache/superset:latest

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libsasl2-dev \
        python3-dev \
        libldap2-dev \
        libssl-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER superset