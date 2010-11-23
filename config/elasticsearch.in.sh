export ES_CONF_DIR=${ES_WORK_DIR-/etc/elasticsearch}
export ES_WORK_DIR=${ES_WORK_DIR-/mnt/elasticsearch/work}
export ES_DATA_DIR=${ES_DATA_DIR-/mnt/elasticsearch/data}

export CLASSPATH=$ES_HOME/plugins/cloud-aws.zip
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

JAVA_OPTS="$JAVA_OPTS -XX:+UseCompressedOops"

# ensures JMX accessible from outside world
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Djava.rmi.server.hostname=ec2-184-73-69-18.compute-1.amazonaws.com "

# More options to consider LATER
# java.net.preferIPv4Stack=true: Better OOTB experience, especially with jgroups
#  -XX:CMSInitiatingOccupancyFraction=88 

ES_JAVA_OPTS="$ES_JAVA_OPTS -Des.path.data=$ES_DATA_DIR -Des.path.work=$ES_WORK_DIR"

export JAVA_OPTS ES_JAVA_OPTS ES_MAX_MEM ES_MIN_MEM
