#!/usr/bin/env bash

# Ensure events are routesd to date index
curl -XPUT 'localhost:9200/_ingest/pipeline/dailyindex?pretty' -H 'Content-Type: application/json' -d'
{
  "description": "daily date-time index naming",
  "processors" : [
    {
      "date_index_name" : {
        "field" : "timestamp",
        "index_name_prefix" : "garmadon-",
        "date_rounding" : "d"
      }
    }
  ]
}
'

curl -XPUT 'localhost:9200/_template/garmadon' -H 'Content-Type: application/json' -d'
{
  "index_patterns": ["garmadon-*"],
  "settings": {
    "number_of_shards": 1
  },
  "mappings": {
    "doc": {
      "_source": {
        "enabled": false
      },
      "properties": {
        "class_initialized": {
          "type": "long"
        },
        "class_inittime": {
          "type": "long"
        },
        "class_loaded": {
          "type": "long"
        },
        "class_loadtime": {
          "type": "long"
        },
        "class_unloaded": {
          "type": "long"
        },
        "class_verifytime": {
          "type": "long"
        },
        "code_comitted": {
          "type": "long"
        },
        "code_used": {
          "type": "long"
        },
        "compile_count": {
          "type": "long"
        },
        "compile_failed": {
          "type": "long"
        },
        "compile_invalidated": {
          "type": "long"
        },
        "compile_time": {
          "type": "long"
        },
        "compressedclassspace_comitted": {
          "type": "long"
        },
        "compressedclassspace_used": {
          "type": "long"
        },
        "descriptors_open": {
          "type": "long"
        },
        "eden_comitted": {
          "type": "long"
        },
        "eden_used": {
          "type": "long"
        },
        "gc(PS MarkSweep)_count": {
          "type": "long"
        },
        "gc(PS MarkSweep)_time": {
          "type": "long"
        },
        "gc(PS Scavenge)_count": {
          "type": "long"
        },
        "gc(PS Scavenge)_time": {
          "type": "long"
        },
        "heap_comitted": {
          "type": "long"
        },
        "heap_used": {
          "type": "long"
        },
        "metaspace_comitted": {
          "type": "long"
        },
        "metaspace_used": {
          "type": "long"
        },
        "nonheap_comitted": {
          "type": "long"
        },
        "nonheap_used": {
          "type": "long"
        },
        "old_comitted": {
          "type": "long"
        },
        "old_used": {
          "type": "long"
        },
        "os_physicalfree": {
          "type": "long"
        },
        "os_swapfree": {
          "type": "long"
        },
        "os_virtual": {
          "type": "long"
        },
        "safepoints_count": {
          "type": "long"
        },
        "safepoints_synctime": {
          "type": "long"
        },
        "safepoints_totaltime": {
          "type": "long"
        },
        "survivor_comitted": {
          "type": "long"
        },
        "survivor_used": {
          "type": "long"
        },
        "synclocks_contendedlockattempts": {
          "type": "long"
        },
        "synclocks_deflations": {
          "type": "long"
        },
        "synclocks_futilewakeups": {
          "type": "long"
        },
        "synclocks_inflations": {
          "type": "long"
        },
        "synclocks_monextant": {
          "type": "long"
        },
        "synclocks_notifications": {
          "type": "long"
        },
        "synclocks_parks": {
          "type": "long"
        },
        "threads_count": {
          "type": "long"
        },
        "threads_daemon": {
          "type": "long"
        },
        "threads_internal": {
          "type": "long"
        },
        "threads_total": {
          "type": "long"
        }
      }
    }
  }
}
'
