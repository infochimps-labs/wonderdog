#!/usr/bin/env bash

#
# This lets you run multiple daemons on the same machine.  It points each
# daemon's data to /mnt$node/elasticsearch -- so running it with node='' will
# write to /mnt/elasticsearch, node=3 will write to /mnt3/elasticsearch.
#
# Usage:
#
#   sudo node=$node ES_MAX_MEM=1800m ./config/run_elasticsearch-2.sh ; done
#
# To run multiple nodes:
#
#   for node in '' 2 3 ; do sudo node=$node ES_MAX_MEM=1800m ./config/run_elasticsearch-2.sh ; done
#

# Which node?
node=${node-''}
echo "Running elasticsearch with node=$node"

# Where does elasticsearch live?
export ES_HOME=/usr/local/share/elasticsearch
export ES_CONF_DIR=/etc/elasticsearch
export ES_INCLUDE=$ES_CONF_DIR/elasticsearch.in.sh

# Where does data live?
ES_DATA_ROOT=/mnt$node/elasticsearch
export ES_DATA_DIR=$ES_DATA_ROOT/data
export ES_WORK_DIR=$ES_DATA_ROOT/work

# bump the # of open files way way up
ulimit -n 65536
# allow elasticsearch to lock itself into memory if JNA is installed
ulimit -l unlimited

# Force the heap size
export ES_MAX_MEM=${ES_MAX_MEM-1800m}
export ES_MIN_MEM=$ES_MAX_MEM

exec chpst -u elasticsearch $ES_HOME/bin/elasticsearch \
  -Des.config=/etc/elasticsearch/elasticsearch.yml \
  -p /var/run/elasticsearch/es-$node.pid
