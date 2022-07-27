#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get Flink config
. "$bin"/config.sh

rotateLogFilesWithPrefix() {
    dir=$1
    prefix=$2
    while read -r log ; do
        rotateLogFile "$log"
    # find distinct set of log file names, ignoring the rotation number (trailing dot and digit)
    done < <(find "$dir" ! -type d -path "${prefix}*" | sed s/.[0-9][0-9]*$// | sort | uniq)
}

rotateLogFile() {
    log=$1;
    num=$MAX_LOG_FILE_NUMBER
    if [ -f "$log" -a "$num" -gt 0 ]; then
        while [ $num -gt 1 ]; do
            prev=`expr $num - 1`
            [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
            num=$prev
        done
        mv "$log" "$log.$num";
    fi
}

constructFlinkClassPath() {
    local FLINK_DIST
    local FLINK_CLASSPATH

    while read -d '' -r jarfile ; do
        if [[ "$jarfile" =~ .*/flink-dist[^/]*.jar$ ]]; then
            FLINK_DIST="$FLINK_DIST":"$jarfile"
        elif [[ "$FLINK_CLASSPATH" == "" ]]; then
            FLINK_CLASSPATH="$jarfile";
        else
            FLINK_CLASSPATH="$FLINK_CLASSPATH":"$jarfile"
        fi
    done < <(find "$FLINK_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)

    if [[ "$FLINK_DIST" == "" ]]; then
        # write error message to stderr since stdout is stored as the classpath
        (>&2 echo "[ERROR] Flink distribution jar not found in $FLINK_LIB_DIR.")

        # exit function with empty classpath to force process failure
        exit 1
    fi

    echo "$FLINK_CLASSPATH""$FLINK_DIST"
}

manglePathList() {
    UNAME=$(uname -s)
    # a path list, for example a java classpath
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -wp "$1"`
    else
        echo $1
    fi
}

JVM_ARGS="$JVM_ARGS -Xmx512m"
CLASS_TO_RUN=org.apache.flink.yarn.cli.FlinkYarnSessionCli

log=$FLINK_LOG_DIR/flink-yarn.log
out=$FLINK_LOG_DIR/flink-yarn.out
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j-yarn-session.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback-yarn.xml"

# Rotate log files
rotateLogFilesWithPrefix "$FLINK_LOG_DIR" "$FLINK_LOG_PREFIX"
CLASS_PATH=`manglePathList $(constructFlinkClassPath):$INTERNAL_HADOOP_CLASSPATHS`
#CLASS_PATH=`manglePathList $(constructFlinkClassPath):$(hadoop classpath)`
#CLASS_PATH=$(echo "${CLASS_PATH}" | sed "s#"$FLINK_HOME"/lib/slf4j-log4j12-1.7.7.jar:##g")
exec $JAVA_RUN $JVM_ARGS -classpath "$CLASS_PATH" $log_setting ${CLASS_TO_RUN} -j "$FLINK_LIB_DIR"/flink-dist*.jar "$@" > "$out" 2>&1


