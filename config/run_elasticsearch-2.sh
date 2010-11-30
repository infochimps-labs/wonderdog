#!/usr/bin/env bash

# bump the # of open files way way up
ulimit -n 65536
# allow elasticsearch to lock itself into memory if JNA is installed
ulimit -l unlimited

ES_MAX_MEM=${ES_MAX_MEM-2800m}
node=${node-2}

echo "node=$node"

export ES_MIN_MEM=$ES_MAX_MEM
export ES_MAX_MEM=$ES_MAX_MEM
export ES_INCLUDE=/etc/elasticsearch/elasticsearch.in.sh
export ES_CONF_DIR=/etc/elasticsearch
export ES_DATA_DIR=/mnt$node/elasticsearch/data
export ES_WORK_DIR=/mnt$node/elasticsearch/work

exec chpst -u elasticsearch /usr/local/share/elasticsearch/bin/elasticsearch -p /var/run/elasticsearch/es-$node.pid -Des.config=/etc/elasticsearch/elasticsearch.yml
