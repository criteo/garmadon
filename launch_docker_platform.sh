#!/usr/bin/env bash

DOCKER_COMPOSE_FOLDER=test/src/main/docker

function block_until_website_available() {
    until curl $1 > /dev/null 2>&1; do sleep 1; done
}

# Build artifacts
mvn clean install -Ppackaging -DpackageForDeploy -DskipTests

# Destroy old containers
pushd ${DOCKER_COMPOSE_FOLDER}
docker-compose down

# Start containers
docker-compose up --build --detach
popd

# Create garmadon ES template
block_until_website_available 'http://localhost:9200'
curl -XPUT 'http://localhost:9200/_template/garmadon' -H 'Content-Type: application/json' -d @elasticsearch/src/main/resources/es/template.json

# Create Kibana Index Pattern
block_until_website_available 'http://localhost:5601'
curl 'http://localhost:5601/api/saved_objects/index-pattern' -H 'Content-Type: application/json' -H 'kbn-version: 6.3.1' -d '{"attributes":{"title":"garmadon*","timeFieldName":"timestamp"}}'

# Create garmadon Grafana Datasource
block_until_website_available 'http://localhost:3000'
curl -u admin:secret -XPOST 'http://localhost:3000/api/datasources' -H 'Content-Type: application/json' -d '{"name":"garmadon","isDefault":false,"type":"elasticsearch","url":"http://elasticsearch:9200","access":"proxy","jsonData":{"keepCookies":[],"timeField":"timestamp","esVersion":5,"maxConcurrentShardRequests":256,"interval":"Daily"},"secureJsonFields":{},"database":"[garmadon-]YYYY-MM-DD"}'

# Import garmadon dashboards
curl -u admin:secret -XPOST 'http://localhost:3000/api/dashboards/import' -H 'Content-Type: application/json' -d @elasticsearch/src/main/resources/grafana/garmadon-compute.json
curl -u admin:secret -XPOST 'http://localhost:3000/api/dashboards/import' -H 'Content-Type: application/json' -d @elasticsearch/src/main/resources/grafana/garmadon-hdfs.json
curl -u admin:secret -XPOST 'http://localhost:3000/api/dashboards/import' -H 'Content-Type: application/json' -d @elasticsearch/src/main/resources/grafana/garmadon-yarn-application.json

## Run some test jobs
block_until_website_available 'http://localhost:8088'
block_until_website_available 'http://localhost:19888'

pushd ${DOCKER_COMPOSE_FOLDER}

# MapRed Teragen
docker-compose exec client yarn jar /opt/hadoop/share/hadoop/mapreduce2/hadoop-mapreduce-examples-2.6.0-cdh5.15.0.jar \
    teragen 1000000 /tmp/test/teragen

# MapRed Terasort
docker-compose exec client yarn jar /opt/hadoop/share/hadoop/mapreduce2/hadoop-mapreduce-examples-2.6.0-cdh5.15.0.jar \
    terasort /tmp/test/teragen /tmp/test/terasort

# MapRed Pi
docker-compose exec client yarn jar /opt/hadoop/share/hadoop/mapreduce2/hadoop-mapreduce-examples-2.6.0-cdh5.15.0.jar \
    pi 2 1000
popd