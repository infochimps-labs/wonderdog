CLASSPATH=$CLASSPATH:$ES_HOME/lib/elasticsearch-0.11.0.jar:$ES_HOME/lib/*:$ES_HOME/lib/sigar/*

if [ "x$ES_MIN_MEM" = "x" ]; then
    ES_MIN_MEM=256m
fi
if [ "x$ES_MAX_MEM" = "x" ]; then
    ES_MAX_MEM=1500m
fi


# Arguments to pass to the JVM
JAVA_OPTS="$JAVA_OPTS -Xms${ES_MIN_MEM}"
JAVA_OPTS="$JAVA_OPTS -Xmx${ES_MAX_MEM}"
JAVA_OPTS="$JAVA_OPTS -Xss128k"

JAVA_OPTS="$JAVA_OPTS -Djline.enabled=true"

JAVA_OPTS="$JAVA_OPTS -XX:+AggressiveOpts"

JAVA_OPTS="$JAVA_OPTS -XX:+UseParNewGC"
JAVA_OPTS="$JAVA_OPTS -XX:+UseConcMarkSweepGC"
JAVA_OPTS="$JAVA_OPTS -XX:+CMSParallelRemarkEnabled"
JAVA_OPTS="$JAVA_OPTS -XX:SurvivorRatio=8"
JAVA_OPTS="$JAVA_OPTS -XX:MaxTenuringThreshold=1"
JAVA_OPTS="$JAVA_OPTS -XX:+HeapDumpOnOutOfMemoryError"
JAVA_OPTS="$JAVA_OPTS -XX:HeapDumpPath=$ES_HOME/work/heap"


# More options to consider LATER
# java.net.preferIPv4Stack=true: Better OOTB experience, especially with jgroups
#  -XX:CMSInitiatingOccupancyFraction=88 
# -XX:+UseCompressedOops


#
# ES_CONF_DIR=$HOME/Programming/wonderdog/config ;sudo -u hadoop ES_INCLUDE=$ES_CONF_DIR/elasticsearch.in.sh /usr/lib/elasticsearch/bin/elasticsearch -Des.config=$ES_CONF_DIR/elasticsearch.yml
#

