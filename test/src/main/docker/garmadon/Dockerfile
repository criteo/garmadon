#
# Centos base
#

# Pull base image.
FROM centos:7
MAINTAINER Nicolas Fraison <nfraison@yahoo.fr>

# Environment Path
ENV EPEL_VERSION latest-7

# Java
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
ENV PATH=${JAVA_HOME}/bin:/opt/hadoop/bin:/opt/spark/bin:/opt/flink/bin:$PATH

# Environment Version
ENV HADOOP2_VERSION=2.6.0-cdh5.15.0
ENV HADOOP3_VERSION=3.1.2
ENV SPARK_VERSION=2.4.1
ENV FLINK_VERSION=1.6.4

# Environment Config
ENV HADOOP2_CONF_DIR=/opt/hadoop2/etc/hadoop
ENV HADOOP3_CONF_DIR=/opt/hadoop3/etc/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV YARN_CONF_DIR=${HADOOP_CONF_DIR}
ENV SPARK_CONF_DIR=/opt/spark/conf
ENV HADOOP_LOG_DIR=/var/log/hadoop

ENV HADOOP_LIBEXEC_DIR=/opt/hadoop/libexec

# Add Epel, tooling and update
RUN rpm -ivh https://dl.fedoraproject.org/pub/epel/epel-release-${EPEL_VERSION}.noarch.rpm && \
    yum update -y && \
    yum install -y wget tar which net-tools curl tcpdump java-1.8.0-openjdk-devel && \
    yum clean all

# Download and unzip hadoop 2
RUN wget http://archive.cloudera.com/cdh5/cdh/5/hadoop-${HADOOP2_VERSION}.tar.gz && \
    tar xvfz hadoop-${HADOOP2_VERSION}.tar.gz -C /opt && \
    ln -s /opt/hadoop-${HADOOP2_VERSION} /opt/hadoop2 && \
    rm -f hadoop-${HADOOP2_VERSION}.tar.gz

# Download and unzip hadoop 3
RUN wget http://mirrors.ircam.fr/pub/apache/hadoop/common/hadoop-${HADOOP3_VERSION}/hadoop-${HADOOP3_VERSION}.tar.gz && \
    tar xvfz hadoop-${HADOOP3_VERSION}.tar.gz -C /opt && \
    ln -s /opt/hadoop-${HADOOP3_VERSION} /opt/hadoop3 && \
    rm -f hadoop-${HADOOP3_VERSION}.tar.gz

# Download and unzip spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.6.tgz && \
    tar xvfz spark-${SPARK_VERSION}-bin-hadoop2.6.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop2.6 /opt/spark && \
    rm -f spark-${SPARK_VERSION}-bin-hadoop2.6.tgz

# Download and unzip flink
RUN wget http://apache.mediamirrors.org/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-hadoop26-scala_2.11.tgz && \
    tar xvfz flink-${FLINK_VERSION}-bin-hadoop26-scala_2.11.tgz -C /opt && \
    ln -s /opt/flink-${FLINK_VERSION} /opt/flink && \
    rm -f flink-${FLINK_VERSION}-bin-hadoop26-scala_2.11.tgz

# Create all needed folders
RUN mkdir -p /opt/garmadon/conf-forwarder \
             /opt/garmadon/conf-es-reader \
             /opt/garmadon/lib \
             /data/hdfs /data/yarn ${HADOOP_LOG_DIR}

# Add configurations and entrypoint
ADD hadoop_conf/ ${HADOOP2_CONF_DIR}
ADD hadoop_conf/ ${HADOOP3_CONF_DIR}
ADD spark_conf ${SPARK_CONF_DIR}
ADD conf-forwarder/ /opt/garmadon/conf-forwarder
ADD conf-es-reader/ /opt/garmadon/conf-es-reader
ADD conf-hdfs-reader/ /opt/garmadon/conf-hdfs-reader
ADD scripts/ /usr/bin
ADD ressources/ /tmp

RUN chmod a+x /usr/bin/entrypoint.sh ${HADOOP2_CONF_DIR}/*.sh ${HADOOP3_CONF_DIR}/*.sh && \
    chmod -R 777 /data/hdfs /data/yarn ${HADOOP_LOG_DIR}

# Define default command.
CMD /bin/bash
