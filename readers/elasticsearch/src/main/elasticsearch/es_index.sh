#!/usr/bin/env bash

# Schema definition
# Those ones are only available in 6 and will improve search on time based event
# "index.sort.field" : "timestamp",
# "index.sort.order" : "desc"
# _all: disable -> we don't need to search on all field but only choose some specific keywords on some fields
# norms: false -> we don't need score
# index_options: freqs -> we don't need index positions
# index: false -> we can still aggregate but not filter on it
# eager_global_ordinals: true -> force computation of fielddata at index time during refresh
# Number of shards to be increased in prod (around 50go per shards max)
curl -XPUT 'localhost:9200/_template/garmadon' -H 'Content-Type: application/json' -d @es/template.json
# Block write in index and reduce number of replica
curl -XPUT 'localhost:9200/garmadon-20180611/_settings' -H 'Content-Type: application/json' -d'
{
    "index": {
        "blocks.write": "true",
        "number_of_replicas" : 1
    }
}
'

# Force merge of all segments in one
curl -XPOST 'localhost:9200/garmadon-20180611/_forcemerge?max_num_segments=1'
