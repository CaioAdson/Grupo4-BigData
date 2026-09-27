FROM eclipse-temurin:11-jre
ARG FLUME_VERSION=1.11.0
# Precisa bater com a versão usada pelo namenode/datanode no docker-compose.yml
ARG HADOOP_VERSION=3.4.1
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && curl -fsSL "https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz" | tar -xz -C /opt \
    && mv "/opt/apache-flume-${FLUME_VERSION}-bin" /opt/flume \
    # CORREÇÃO: a distribuição padrão do Flume NÃO inclui os jars cliente
    # do Hadoop necessários para o sink "hdfs" (é um problema documentado:
    # falha com ClassNotFoundException/NoClassDefFoundError em
    # org.apache.hadoop.io.SequenceFile$CompressionType). Sem isso, o
    # agente do Flume nem inicializa quando lê flume.conf.
    && curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
       -o /tmp/hadoop.tar.gz \
    && mkdir -p /tmp/hadoop \
    && tar -xzf /tmp/hadoop.tar.gz -C /tmp/hadoop --strip-components=1 \
    && cp /tmp/hadoop/share/hadoop/common/*.jar /opt/flume/lib/ \
    && cp /tmp/hadoop/share/hadoop/common/lib/*.jar /opt/flume/lib/ \
    && cp /tmp/hadoop/share/hadoop/hdfs/*.jar /opt/flume/lib/ \
    && cp /tmp/hadoop/share/hadoop/hdfs/lib/*.jar /opt/flume/lib/ \
    && rm -rf /tmp/hadoop /tmp/hadoop.tar.gz \
    && mkdir -p /state \
    && apt-get purge -y curl && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/flume/bin:${PATH}"
