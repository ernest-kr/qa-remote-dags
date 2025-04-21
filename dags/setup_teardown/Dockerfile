ARG IMAGE_NAME
FROM ${IMAGE_NAME}
ENV HIVE_LIBRARY_VERSION=hive-2.3.9
ENV HADOOP_LIBRARY_VERSION=hadoop-2.10.1
USER root

RUN apt-get update -y \
    && apt-get install -y software-properties-common \
    && apt-get install -y wget procps gnupg2

# Install openjdk-8
RUN apt-add-repository 'deb http://archive.debian.org/debian stretch main' \
    && apt-get update && apt-get install -y openjdk-8-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64/

# Install Apache Hive (Download same version of library to match EMR Hive version created by DAG)
RUN curl "https://downloads.apache.org/hive/$HIVE_LIBRARY_VERSION/apache-$HIVE_LIBRARY_VERSION-bin.tar.gz" -o  "/tmp/apache-$HIVE_LIBRARY_VERSION-bin.tar.gz"  \
    && tar -xf /tmp/apache-$HIVE_LIBRARY_VERSION-bin.tar.gz -C /usr/local
ENV HIVE_HOME=/usr/local/apache-$HIVE_LIBRARY_VERSION-bin


# Install Apache Hadoop (Download same version of library to match EMR Hadoop version created by DAG)
RUN curl "https://archive.apache.org/dist/hadoop/common/$HADOOP_LIBRARY_VERSION/$HADOOP_LIBRARY_VERSION.tar.gz" -o "/tmp/$HADOOP_LIBRARY_VERSION.tar.gz" \
    && tar -xf /tmp/$HADOOP_LIBRARY_VERSION.tar.gz -C /usr/local
ENV HADOOP_HOME=/usr/local/$HADOOP_LIBRARY_VERSION
ENV HADOOP_CONF_DIR=$HADOOP_HOME/conf

RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules \
        jq

COPY setup.cfg ${AIRFLOW_HOME}/astronomer_providers/setup.cfg
COPY pyproject.toml ${AIRFLOW_HOME}/astronomer_providers/pyproject.toml

ENV PATH $PATH:$JAVA_HOME/bin::$HIVE_HOME/bin:$HADOOP_HOME/bin

RUN pip install -e "${AIRFLOW_HOME}/astronomer_providers[all,tests,mypy]"
RUN pip install apache-airflow-providers-slack
USER astro
