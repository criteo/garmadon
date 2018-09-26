#!/usr/bin/env bash

set -e

export GARMADON_RELEASE=$(mvn -Dexec.executable='echo' -Dexec.args='${project.version}' --non-recursive exec:exec -q)
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
docker-compose up --build -d
popd

# Create garmadon ES template
block_until_website_available 'http://localhost:9200'
curl -XPUT 'http://localhost:9200/_template/garmadon' -H 'Content-Type: application/json' -d @readers/elasticsearch/src/main/elasticsearch/es/template.json
curl -XDELETE 'http://localhost:9200/garmadon*' # Ensure no garmadon index have been created with bad mapping

# Create Kibana Index Pattern
block_until_website_available 'http://localhost:5601'
curl 'http://localhost:5601/api/saved_objects/index-pattern' -H 'Content-Type: application/json' -H 'kbn-version: 6.3.2' -d '{"attributes":{"title":"garmadon*","timeFieldName":"timestamp"}}'

# Create garmadon Grafana Datasource
block_until_website_available 'http://localhost:3000'
curl -u admin:secret -XPOST 'http://localhost:3000/api/datasources' -H 'Content-Type: application/json' -d '{"name":"garmadon","isDefault":false,"type":"elasticsearch","url":"http://elasticsearch:9200","access":"proxy","jsonData":{"keepCookies":[],"timeField":"timestamp","esVersion":56,"maxConcurrentShardRequests":256,"interval":"Hourly"},"secureJsonFields":{},"database":"[garmadon-]YYYY-MM-DD-HH"}'

# Import garmadon dashboards
curl -u admin:secret -XPOST 'http://localhost:3000/api/dashboards/import' -H 'Content-Type: application/json' -d @readers/elasticsearch/src/main/elasticsearch/grafana/garmadon-server-overview.json
curl -u admin:secret -XPOST 'http://localhost:3000/api/dashboards/import' -H 'Content-Type: application/json' -d @readers/elasticsearch/src/main/elasticsearch/grafana/garmadon-compute.json
curl -u admin:secret -XPOST 'http://localhost:3000/api/dashboards/import' -H 'Content-Type: application/json' -d @readers/elasticsearch/src/main/elasticsearch/grafana/garmadon-hdfs.json
curl -u admin:secret -XPOST 'http://localhost:3000/api/dashboards/import' -H 'Content-Type: application/json' -d @readers/elasticsearch/src/main/elasticsearch/grafana/garmadon-yarn-application.json
curl -u admin:secret -XPOST 'http://localhost:3000/api/dashboards/import' -H 'Content-Type: application/json' -d @readers/elasticsearch/src/main/elasticsearch/grafana/garmadon-standalone-jvm.json
curl -u admin:secret -XPOST 'http://localhost:3000/api/dashboards/import' -H 'Content-Type: application/json' -d @readers/elasticsearch/src/main/elasticsearch/grafana/garmadon-spark-job.json
curl -u admin:secret -XPOST 'http://localhost:3000/api/dashboards/import' -H 'Content-Type: application/json' -d @readers/elasticsearch/src/main/elasticsearch/grafana/garmadon-spark-job-stages.json
curl -u admin:secret -XPOST 'http://localhost:3000/api/dashboards/import' -H 'Content-Type: application/json' -d @readers/elasticsearch/src/main/elasticsearch/grafana/garmadon-spark-job-executors.json

## Run some test jobs
block_until_website_available 'http://localhost:8088'
block_until_website_available 'http://localhost:19888'

pushd ${DOCKER_COMPOSE_FOLDER}

# Ensure that garmadon agent doesn't failed with StandaloneModule
# We had issues due to NoClassDefFoundError
docker-compose exec client java -version

# MapRed Teragen
docker-compose exec client yarn jar /opt/hadoop/share/hadoop/mapreduce2/hadoop-mapreduce-examples-2.6.0-cdh5.15.0.jar \
    teragen 1000000 /tmp/test/teragen

# MapRed Terasort
docker-compose exec client yarn jar /opt/hadoop/share/hadoop/mapreduce2/hadoop-mapreduce-examples-2.6.0-cdh5.15.0.jar \
    terasort /tmp/test/teragen /tmp/test/terasort

# MapRed Pi
docker-compose exec client yarn jar /opt/hadoop/share/hadoop/mapreduce2/hadoop-mapreduce-examples-2.6.0-cdh5.15.0.jar \
    pi 2 1000

# SparkPi (Compute)
docker-compose exec client /opt/spark/bin/spark-submit --class org.apache.spark.examples.SparkPi \
    /opt/spark/examples/jars/spark-examples_2.11-2.2.2.jar 100

# Spark DFSReadWriteTest (Read/Write/Shuffle)
docker-compose exec client /opt/spark/bin/spark-submit --class org.apache.spark.examples.DFSReadWriteTest \
    /opt/spark/examples/jars/spark-examples_2.11-2.2.2.jar /opt/garmadon/conf-forwarder/server.properties /tmp

# Spark SQL (Interact with HDFS and execute lots of stage)
docker-compose exec client /opt/spark/bin/spark-submit \
    --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.initialExecutors=4 \
    --conf spark.dynamicAllocation.maxExecutors=4 --conf spark.dynamicAllocation.executorIdleTimeout=1s \
    --class org.apache.spark.examples.sql.SparkSQLExample /opt/spark/examples/jars/spark-examples_2.11-2.2.2.jar

# Spark yarn client
docker-compose exec client /opt/spark/bin/spark-submit \
    --deploy-mode client \
    --conf spark.shuffle.service.enabled=true --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.initialExecutors=4 \
    --conf spark.dynamicAllocation.maxExecutors=4 --conf spark.dynamicAllocation.executorIdleTimeout=1s \
    --class org.apache.spark.examples.sql.SparkSQLExample /opt/spark/examples/jars/spark-examples_2.11-2.2.2.jar
popd
