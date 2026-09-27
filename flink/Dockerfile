FROM flink:1.20.1-scala_2.12-java11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && ln -s /usr/bin/python3 /usr/local/bin/python \
    && rm -rf /var/lib/apt/lists/*

# CORREÇÃO: job.py importa "requests" para enviar os alertas ao HBase REST
# (Stargate). Sem essa linha o job quebra com ModuleNotFoundError assim
# que tenta gravar o primeiro alerta.
RUN pip3 install --no-cache-dir apache-flink==1.20.1 requests==2.32.3

USER flink