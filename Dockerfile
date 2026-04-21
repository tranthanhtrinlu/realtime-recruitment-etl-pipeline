FROM python:3.9-slim-bullseye

ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_VERSION=3.1.3
ENV HADOOP_VERSION=3.2
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    curl \
    bash \
    procps \
    tini \
    ca-certificates \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

RUN pip install --no-cache-dir \
    pyspark==3.1.3 \
    pandas \
    cassandra-driver \
    mysql-connector-python

RUN pip install --no-cache-dir kafka-python

RUN mkdir -p ${SPARK_HOME}/jars /opt/project /opt/scripts

RUN rm -f ${SPARK_HOME}/jars/*cassandra*connector*.jar

RUN curl -L -o ${SPARK_HOME}/jars/spark-cassandra-connector-assembly_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.1.0/spark-cassandra-connector-assembly_2.12-3.1.0.jar

RUN curl -L -o ${SPARK_HOME}/jars/mysql-connector-java-8.0.30.jar \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar

COPY scripts/start-spark.sh /opt/scripts/start-spark.sh
RUN chmod +x /opt/scripts/start-spark.sh

WORKDIR /opt/project
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/scripts/start-spark.sh"]