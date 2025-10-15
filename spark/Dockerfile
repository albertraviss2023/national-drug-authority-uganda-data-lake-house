# syntax=docker/dockerfile:1.6
FROM apache/spark:3.5.0

USER root

# Install key pakages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        openjdk-11-jre \
        procps \
        librdkafka-dev \
        curl \
        wget \
        zsh \
        git \
        freetds-dev \
        libssl-dev \
        build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$SPARK_HOME/sbin:$SPARK_HOME/bin:$PATH
# pip quality-of-life + ensure public index
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PIP_INDEX_URL=https://pypi.org/simple

WORKDIR $SPARK_HOME
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV SPARK_VERSION_SHORT=3.5
ENV SPARK_VERSION=3.5.0
ENV AWS_SDK_VERSION=1.12.777
ENV HADOOP_AWS_VERSION=3.4.0

# Download Spark dependencies (unchanged)
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.12/${SPARK_VERSION}/spark-hadoop-cloud_2.12-${SPARK_VERSION}.jar -o ${SPARK_HOME}/jars/spark-hadoop-cloud_2.12-${SPARK_VERSION}.jar

# Configure Iceberg (unchanged)
ENV ICEBERG_VERSION=1.9.1
ENV AWS_SDK_BUNDLE_VERSION=2.26.29

RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION_SHORT}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION_SHORT}_2.12-${ICEBERG_VERSION}.jar -o ${SPARK_HOME}/jars/iceberg-spark-runtime.jar
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar -o ${SPARK_HOME}/jars/aws-bundle-${AWS_SDK_BUNDLE_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${AWS_SDK_BUNDLE_VERSION}/url-connection-client-${AWS_SDK_BUNDLE_VERSION}.jar -o ${SPARK_HOME}/jars/url-connection-client-${AWS_SDK_BUNDLE_VERSION}.jar

# Add Kafka clients JAR (unchanged)
RUN curl https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.2/kafka-clients-3.6.2.jar -o ${SPARK_HOME}/jars/kafka-clients.jar

# Configure ZSH (unchanged)
RUN wget https://github.com/ohmyzsh/ohmyzsh/raw/master/tools/install.sh -O - | zsh || true
ENV TERM xterm
ENV ZSH_THEME robbyrussell
RUN chsh -s /usr/bin/zsh

# Configure Python
# ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
COPY spark/requirements.txt /opt/requirements.txt

# >>> Minimal speed fix: cache pip + prefer wheels <<<
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install --upgrade --no-input pip && \
    python3 -m pip install --no-input --prefer-binary -r /opt/requirements.txt

COPY spark/jupyter_server_config.py /root/.jupyter/
COPY spark/themes.jupyterlab-settings /root/.jupyter/lab/user-settings/@jupyterlab/apputils-extension/

# Copy the startup script for jupyter and spark
COPY spark/start-spark-jupyter-server.sh /opt/start-spark-jupyter-server.sh
RUN chmod +x /opt/start-spark-jupyter-server.sh

# Clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
