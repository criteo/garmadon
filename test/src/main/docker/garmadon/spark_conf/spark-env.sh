#!/usr/bin/env bash
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}