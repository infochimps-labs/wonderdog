#!/usr/bin/env bash

ulimit -n 65536

node=${node-2}

sudo -u elasticsearch                               \
  ES_MIN_MEM=3200m                                  \
  ES_MAX_MEM=3200m                                  \
  ES_INCLUDE=/etc/elasticsearch/elasticsearch.in.sh \
  ES_CONF_DIR=/etc/elasticsearch                    \
  ES_DATA_DIR=/mnt$node/elasticsearch/data              \
  ES_WORK_DIR=/mnt$node/elasticsearch/work              \
  /usr/local/share/elasticsearch/bin/elasticsearch -p /var/run/elasticsearch/es-$node.pid -Des.config=/etc/elasticsearch/elasticsearch.yml
