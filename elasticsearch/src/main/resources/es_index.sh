#!/usr/bin/env bash

# Schema definition
# Those ones are only available in 6 and will improve search on time based event
# "index.sort.field" : "timestamp",
# "index.sort.order" : "desc"
# _all: disable -> we don't need to search on all field but only choose some specific keywords on some fields
# norms: false -> we don't need score
# index_options: freqs -> we don't need index positions
# index: false -> we can still aggregate but not filter on it
# Number of shards to be increased in prod (around 50go per shards max)
curl -XPUT 'localhost:9200/_template/garmadon' -H 'Content-Type: application/json' -d'
{
  "template": "garmadon*",
  "settings": {
    "index" : {
      "number_of_shards": 20,
      "number_of_replicas" : 2,
      "merge.scheduler.max_thread_count": 1,
      "translog.flush_threshold_size": "1gb",
      "refresh_interval": "30s",
      "unassigned.node_left.delayed_timeout": "15m",
      "sort.field": "timestamp",
      "sort.order": "desc"
    },
    "analysis": {
      "analyzer": {
        "path_analyzer": {
          "tokenizer": "path_tokenizer"
        }
      },
      "tokenizer": {
        "path_tokenizer": {
          "type": "path_hierarchy",
          "delimiter": "/"
        }
      }
    }
  },
  "mappings": {
    "doc": {
      "dynamic_templates": [
        {
          "strings": {
            "match_mapping_type": "string",
            "mapping": {
              "type": "text",
              "norms": false,
              "index_options": "freqs"
            }
          }
        },
        {
          "percent": {
            "match": "*_%*",
            "mapping": {
              "type": "short",
              "index": false
            }
          }
        },
        {
          "unindexed_longs": {
            "match_mapping_type": "long",
            "mapping": {
              "type": "long",
              "index": false
            }
          }
        },
        {
          "unindexed_doubles": {
            "match_mapping_type": "double",
            "mapping": {
              "type": "float",
              "index": false
            }
          }
        }
      ],
      "_source": {
        "enabled": true
      },
      "properties": {
          "action" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "pid" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "framework" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "component" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          }
          "application_id" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "application_name" : {
            "type" : "text",
            "norms": false,
            "index_options": "freqs"
          },
          "attempt_id" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "cause" : {
            "type" : "text",
            "norms": false,
            "index_options": "freqs"
          },
          "class_initialized" : {
            "type" : "integer",
              "index": false
          },
          "class_inittime" : {
            "type" : "integer",
              "index": false
          },
          "class_loaded" : {
            "type" : "integer",
              "index": false
          },
          "class_loadtime" : {
            "type" : "integer",
              "index": false
          },
          "class_unloaded" : {
            "type" : "integer",
              "index": false
          },
          "class_veriftime" : {
            "type" : "integer",
              "index": false
          },
          "code_committed" : {
            "type" : "integer",
              "index": false
          },
          "code_init" : {
            "type" : "integer",
              "index": false
          },
          "code_max" : {
            "type" : "integer",
              "index": false
          },
          "code_used" : {
            "type" : "integer",
              "index": false
          },
          "collector_name" : {
            "type" : "text",
            "norms": false,
            "index_options": "freqs"
          },
          "compile_count" : {
            "type" : "integer",
              "index": false
          },
          "compile_failed" : {
            "type" : "integer",
              "index": false
          },
          "compile_invalidated" : {
            "type" : "integer",
              "index": false
          },
          "compile_threads" : {
            "type" : "integer",
              "index": false
          },
          "compile_time" : {
            "type" : "integer",
              "index": false
          },
          "compressedclassspace_committed" : {
            "type" : "integer",
              "index": false
          },
          "compressedclassspace_init" : {
            "type" : "integer",
              "index": false
          },
          "compressedclassspace_max" : {
            "type" : "integer",
              "index": false
          },
          "compressedclassspace_used" : {
            "type" : "integer",
              "index": false
          },
          "container_id" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "cpu_cores" : {
            "type" : "integer",
              "index": false
          },
          "delta_code" : {
            "type" : "long",
              "index": false
          },
          "delta_eden" : {
            "type" : "long",
              "index": false
          },
          "delta_metaspace" : {
            "type" : "long",
              "index": false
          },
          "delta_old" : {
            "type" : "long",
              "index": false
          },
          "delta_survivor" : {
            "type" : "long",
              "index": false
          },
          "descriptors_max" : {
            "type" : "integer",
              "index": false
          },
          "descriptors_open" : {
            "type" : "integer",
              "index": false
          },
          "dst_path" : {
            "type": "text",
            "norms": false,
            "index_options": "freqs",
            "analyzer": "path_analyzer",
            "search_analyzer": "keyword"
          },
          "eden_committed" : {
            "type" : "long",
              "index": false
          },
          "eden_init" : {
            "type" : "long",
              "index": false
          },
          "eden_max" : {
            "type" : "long",
              "index": false
          },
          "eden_used" : {
            "type" : "long",
              "index": false
          },
          "gc(G1 Old Generation)_count" : {
            "type" : "long",
              "index": false
          },
          "gc(G1 Old Generation)_time" : {
            "type" : "long",
              "index": false
          },
          "gc(G1 Young Generation)_count" : {
            "type" : "long",
              "index": false
          },
          "gc(G1 Young Generation)_time" : {
            "type" : "long",
              "index": false
          },
          "gc(PS MarkSweep)_count" : {
            "type" : "long",
              "index": false
          },
          "gc(PS MarkSweep)_time" : {
            "type" : "long",
              "index": false
          },
          "gc(PS Scavenge)_count" : {
            "type" : "long",
              "index": false
          },
          "gc(PS Scavenge)_time" : {
            "type" : "long",
              "index": false
          },
          "heap_committed" : {
            "type" : "long",
              "index": false
          },
          "heap_init" : {
            "type" : "long",
              "index": false
          },
          "heap_max" : {
            "type" : "long",
              "index": false
          },
          "heap_used" : {
            "type" : "long",
              "index": false
          },
          "hostname" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "limit" : {
            "type" : "long",
              "index": false
          },
          "machinecpu_ctxtswitches" : {
            "type" : "long",
              "index": false
          },
          "machinecpu_interrupts" : {
            "type" : "long",
              "index": false
          },
          "memory_physical" : {
            "type" : "integer",
              "index": false
          },
          "memory_swap" : {
            "type" : "integer",
              "index": false
          },
          "metaspace_committed" : {
            "type" : "integer",
              "index": false
          },
          "metaspace_init" : {
            "type" : "integer",
              "index": false
          },
          "metaspace_max" : {
            "type" : "integer",
              "index": false
          },
          "metaspace_used" : {
            "type" : "integer",
              "index": false
          },
          "nonheap_committed" : {
            "type" : "integer",
              "index": false
          },
          "nonheap_init" : {
            "type" : "integer",
              "index": false
          },
          "nonheap_max" : {
            "type" : "integer",
              "index": false
          },
          "nonheap_used" : {
            "type" : "integer",
              "index": false
          },
          "old_committed" : {
            "type" : "integer",
              "index": false
          },
          "old_init" : {
            "type" : "integer",
              "index": false
          },
          "old_max" : {
            "type" : "integer",
              "index": false
          },
          "old_used" : {
            "type" : "integer",
              "index": false
          },
          "os_physicalfree" : {
            "type" : "integer",
              "index": false
          },
          "os_physicaltotal" : {
            "type" : "integer",
              "index": false
          },
          "os_swapfree" : {
            "type" : "integer",
              "index": false
          },
          "os_swaptotal" : {
            "type" : "integer",
              "index": false
          },
          "os_virtual" : {
            "type" : "integer",
              "index": false
          },
          "pause_time" : {
            "type" : "long",
              "index": false
          },
          "process_ctxtswitches" : {
            "type" : "integer",
              "index": false
          },
          "process_interrupts" : {
            "type" : "integer",
              "index": false
          },
          "process_read" : {
            "type" : "integer",
              "index": false
          },
          "process_rss" : {
            "type" : "integer",
              "index": false
          },
          "process_threads" : {
            "type" : "integer",
              "index": false
          },
          "process_vsz" : {
            "type" : "integer",
              "index": false
          },
          "process_written" : {
            "type" : "integer",
              "index": false
          },
          "safepoints_count" : {
            "type" : "integer",
              "index": false
          },
          "safepoints_synctime" : {
            "type" : "integer",
              "index": false
          },
          "safepoints_totaltime" : {
            "type" : "integer",
              "index": false
          },
          "src_path" : {
            "type": "text",
            "norms": false,
            "index_options": "freqs",
            "analyzer": "path_analyzer",
            "search_analyzer": "keyword"
          },
          "state" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "survivor_committed" : {
            "type" : "integer",
              "index": false
          },
          "survivor_init" : {
            "type" : "integer",
              "index": false
          },
          "survivor_max" : {
            "type" : "integer",
              "index": false
          },
          "survivor_used" : {
            "type" : "integer",
              "index": false
          },
          "synclocks_contendedlockattempts" : {
            "type" : "integer",
              "index": false
          },
          "synclocks_deflations" : {
            "type" : "integer",
              "index": false
          },
          "synclocks_futilewakeups" : {
            "type" : "integer",
              "index": false
          },
          "synclocks_inflations" : {
            "type" : "integer",
              "index": false
          },
          "synclocks_monextant" : {
            "type" : "integer",
              "index": false
          },
          "synclocks_notifications" : {
            "type" : "integer",
              "index": false
          },
          "synclocks_parks" : {
            "type" : "integer",
              "index": false
          },
          "tag" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "threads_count" : {
            "type" : "long",
              "index": false
          },
          "threads_daemon" : {
            "type" : "integer",
              "index": false
          },
          "threads_internal" : {
            "type" : "integer",
              "index": false
          },
          "threads_total" : {
            "type" : "integer",
              "index": false
          },
          "timestamp" : {
            "type" : "date"
          },
          "type" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "uri" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "username" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "value" : {
            "type" : "long",
            "index": false
          },
          "path" : {
            "type" : "text",
            "norms": false,
            "index_options": "freqs"
          },
          "tx" : {
            "type" : "long",
            "index": false
          },
          "rx" : {
            "type" : "long",
            "index": false
          },
          "reads" : {
            "type" : "long",
            "index": false
          },
          "readsbytes" : {
            "type" : "long",
            "index": false
          },
          "writes" : {
            "type" : "long",
            "index": false
          },
          "writesbytes" : {
            "type" : "long",
            "index": false
          },
          "network" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
          "disk" : {
            "type": "keyword",
            "norms": false,
            "index_options": "freqs"
          },
        }
      }
    }
  }
}
'

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
