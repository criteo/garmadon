#!/usr/bin/env bash

function check_namenode_up {
    until hdfs dfs -ls hdfs://namenode/; do sleep 1; done
}

function check_resourcemanager_up {
    until curl http://resourcemanager:8088/; do sleep 1; done
}

function create_hdfs_folder {
    hdfs dfs -mkdir -p /var/log/hadoop-yarn/apps /var/log/hadoop-yarn/staging/history/done_intermediate /var/log/hadoop-yarn/staging/history/done \
                       /user/${USER} /tmp
    hdfs dfs -chown yarn:yarn /var/log/hadoop-yarn/apps /var/log/hadoop-yarn/staging/history/done_intermediate /var/log/hadoop-yarn/staging/history/done
    hdfs dfs -chmod 1777 /var/log/hadoop-yarn/apps /var/log/hadoop-yarn/staging /tmp
}

function client {
    until false; do sleep 600; done
}

function es-reader {
    java -cp /opt/garmadon/conf-es-reader:/opt/garmadon/lib/garmadon-elasticsearch-1.0-SNAPSHOT-uber.jar \
         com.criteo.hadoop.garmadon.elasticsearch.ElasticSearchReader kafka:9092 es-reader elasticsearch 9200 garmadon esuser espassword
}

function namenode {
    hdfs namenode -format -force
    hdfs namenode
}

function datanode {
    check_namenode_up
    hdfs datanode
}

function resourcemanager {
    check_namenode_up
    create_hdfs_folder
    yarn resourcemanager
}

function nodemanager {
    check_resourcemanager_up
    nohup java -cp /opt/garmadon/conf-forwarder:/opt/garmadon/lib/garmadon-forwarder-1.0-SNAPSHOT-uber.jar \
          com.criteo.hadoop.garmadon.forwarder.Forwarder > /var/log/garmadon-forwarder.log 2>&1 &
    yarn nodemanager
}

function historyserver {
    check_resourcemanager_up
    mapred historyserver
}

############# MAIN
case $1 in
    client)
        client
        /bin/bash
        ;;
    es-reader)
        es-reader
        ;;
    namenode)
        namenode
        ;;
    datanode)
        datanode
        ;;
    resourcemanager)
        resourcemanager
        ;;
    nodemanager)
        nodemanager
        ;;
    historyserver)
        historyserver
        ;;
esac