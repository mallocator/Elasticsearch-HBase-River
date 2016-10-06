#!/bin/bash

JSON=$(cat <<EOF
{
    "type":"hbase",
    "hbase":{
    	"index":"elasticSearchIndexName",
        "type":"elasticSearchTypeName",
	"host":"localhost",
	"port":"8080",
        "zhosts":"localhost:2181",
        "znode": "/elasticsearch_sink",
        "table":"table",
        "batchSize":1000,
        "idField":"PrimaryKeyFieldName",
        "interval":"60000"
    }
}
EOF
)

curl -XDELETE 127.0.0.1:9200/_river/river-hbase
echo
curl -XPUT 127.0.0.1:9200/_river/river-hbase/_meta -d "$JSON"
echo
