FROM eclipse-temurin:17-jre-jammy

# Definir versões
ENV SPARK_VERSION=4.0.1
ENV HADOOP_VERSION=3.4.1
ENV DELTA_VERSION=4.0.0
ENV ICEBERG_VERSION=1.7.1
ENV SCALA_VERSION=2.13

# Definir variáveis de ambiente
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin:$HADOOP_HOME/bin
ENV SPARK_MASTER_HOST=0.0.0.0
ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8080
ENV SPARK_WORKER_WEBUI_PORT=8081
ENV SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/tmp/spark-events"
ENV JAVA_HOME=/opt/java/openjdk

# Instalar dependências
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    netcat-openbsd \
    procps \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Criar symlink para python
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Baixar e instalar Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Baixar e instalar Spark 4.0.1
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Baixar Delta Lake 4.0 para Spark 4.0
RUN wget https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar -P ${SPARK_HOME}/jars/ && \
    wget https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar -P ${SPARK_HOME}/jars/

# Instalar PySpark e Delta Lake para Python
RUN pip3 install --no-cache-dir pyspark==${SPARK_VERSION} delta-spark==${DELTA_VERSION}

# Criar diretórios necessários
RUN mkdir -p /tmp/spark-events && \
    mkdir -p ${SPARK_HOME}/work && \
    mkdir -p ${SPARK_HOME}/logs && \
    mkdir -p /data

# Configurar Spark 4.0 (ANSI mode agora é padrão)
RUN cp ${SPARK_HOME}/conf/spark-defaults.conf.template ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "# Event Logging" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.eventLog.enabled true" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.eventLog.dir file:///tmp/spark-events" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.history.fs.logDirectory file:///tmp/spark-events" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "# Delta Lake Configuration" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "# Spark Connect (novo no Spark 4.0)" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.connect.grpc.binding.port 15002" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "# ANSI mode (padrão no Spark 4.0)" >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo "spark.sql.ansi.enabled true" >> ${SPARK_HOME}/conf/spark-defaults.conf

# Script de inicialização atualizado para Spark 4.0
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
case "$1" in\n\
  master)\n\
    echo "========================================"\n\
    echo "Iniciando Spark 4.0.1 Master..."\n\
    echo "========================================"\n\
    ${SPARK_HOME}/sbin/start-master.sh\n\
    echo ""\n\
    echo "========================================"\n\
    echo "Iniciando Spark Connect Server..."\n\
    echo "========================================"\n\
    ${SPARK_HOME}/sbin/start-connect-server.sh \\\n\
      --packages io.delta:delta-spark_${SCALA_VERSION}:${DELTA_VERSION} \\\n\
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\\n\
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog &\n\
    sleep 5\n\
    echo ""\n\
    echo "========================================"\n\
    echo "Iniciando Spark History Server..."\n\
    echo "========================================"\n\
    ${SPARK_HOME}/sbin/start-history-server.sh\n\
    echo ""\n\
    echo "========================================"\n\
    echo "Spark Master UI: http://localhost:8080"\n\
    echo "History Server: http://localhost:18080"\n\
    echo "Spark Connect: grpc://localhost:15002"\n\
    echo "========================================"\n\
    tail -f ${SPARK_HOME}/logs/*\n\
    ;;\n\
  worker)\n\
    echo "========================================"\n\
    echo "Aguardando Spark Master..."\n\
    echo "========================================"\n\
    while ! nc -z spark-master 7077; do sleep 1; done\n\
    echo ""\n\
    echo "========================================"\n\
    echo "Iniciando Spark 4.0.1 Worker..."\n\
    echo "========================================"\n\
    ${SPARK_HOME}/sbin/start-worker.sh spark://spark-master:7077\n\
    echo ""\n\
    echo "========================================"\n\
    echo "Worker conectado ao Master"\n\
    echo "========================================"\n\
    tail -f ${SPARK_HOME}/logs/*\n\
    ;;\n\
  history)\n\
    echo "========================================"\n\
    echo "Iniciando Spark History Server..."\n\
    echo "========================================"\n\
    ${SPARK_HOME}/sbin/start-history-server.sh\n\
    echo ""\n\
    echo "========================================"\n\
    echo "History Server: http://localhost:18080"\n\
    echo "========================================"\n\
    tail -f ${SPARK_HOME}/logs/*\n\
    ;;\n\
  connect)\n\
    echo "========================================"\n\
    echo "Iniciando Spark Connect Server..."\n\
    echo "========================================"\n\
    ${SPARK_HOME}/sbin/start-connect-server.sh --packages io.delta:delta-spark_${SCALA_VERSION}:${DELTA_VERSION}\n\
    echo ""\n\
    echo "========================================"\n\
    echo "Spark Connect: grpc://localhost:15002"\n\
    echo "========================================"\n\
    tail -f ${SPARK_HOME}/logs/*\n\
    ;;\n\
  *)\n\
    echo "Uso: $0 {master|worker|history|connect}"\n\
    exit 1\n\
    ;;\n\
esac\n' > /start.sh && chmod +x /start.sh

WORKDIR ${SPARK_HOME}

# Portas expostas
EXPOSE 7077 8080 8081 18080 4040 15002

CMD ["/start.sh", "master"]