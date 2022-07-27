#!/bin/bash

#Flink Worker.
USAGE="Usage: flink-worker.sh (start|stop)"

OPERATION=$1

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

parseBytes() {
    text=$1

    trimmed=$(echo -e "${text}" | tr -d '[:space:]')

    if [ -z "$trimmed" -o "$trimmed" == " " ]; then
        echo "$trimmed is an empty- or whitespace-only string"
	exit 1
    fi

    len=${#trimmed}
    pos=0

    while [ $pos -lt $len ]; do
	current=${trimmed:pos:1}
	if [[ ! $current < '0' ]] && [[ ! $current > '9' ]]; then
	    let pos+=1
	else
	    break
	fi
    done

    number=${trimmed:0:pos}

    unit=${trimmed:$pos}
    unit=$(echo -e "${unit}" | tr -d '[:space:]')
    unit=$(echo -e "${unit}" | tr '[A-Z]' '[a-z]')

    if [ -z "$number" ]; then
        echo "text does not start with a number"
        exit 1
    fi

    local multiplier
    if [ -z "$unit" ]; then
        multiplier=1
    else
        if matchesAny $unit "${BYTES_UNITS[*]}"; then
            multiplier=1
        elif matchesAny $unit "${KILO_BYTES_UNITS[*]}"; then
                multiplier=1024
        elif matchesAny $unit "${MEGA_BYTES_UNITS[*]}"; then
                multiplier=`expr 1024 * 1024`
        elif matchesAny $unit "${GIGA_BYTES_UNITS[*]}"; then
                multiplier=`expr 1024 * 1024 * 1024`
        elif matchesAny $unit "${TERA_BYTES_UNITS[*]}"; then
                multiplier=`expr 1024 * 1024 * 1024 * 1024`
        else
            echo "[ERROR] Memory size unit $unit does not match any of the recognized units"
            exit 1
        fi
    fi

    ((result=$number * $multiplier))

    if [ $[result / multiplier] != "$number" ]; then
        echo "[ERROR] The value $text cannot be re represented as 64bit number of bytes (numeric overflow)."
        exit 1
    fi

    echo "$result"
}

matchesAny() {
    str=$1
    variants=$2

    for s in ${variants[*]}; do
        if [ $str == $s ]; then
            return 0
        fi
    done

    return 1
}

getKibiBytes() {
    bytes=$1
    echo "$(($bytes >>10))"
}

getMebiBytes() {
    bytes=$1
    echo "$(($bytes >> 20))"
}

getGibiBytes() {
    bytes=$1
    echo "$(($bytes >> 30))"
}

getTebiBytes() {
    bytes=$1
    echo "$(($bytes >> 40))"
}



# if memory allocation mode is lazy and no other JVM options are set,
# set the 'Concurrent Mark Sweep GC'
if [[ $FLINK_TM_MEM_PRE_ALLOCATE == "false" ]] && [ -z "${FLINK_ENV_JAVA_OPTS}" ] && [ -z "${FLINK_ENV_JAVA_OPTS_TM}" ]; then
    export JVM_ARGS="$JVM_ARGS -XX:+UseG1GC"
fi

if [ ! -z "${FLINK_TM_HEAP_MB}" ] && [ "${FLINK_TM_HEAP}" == 0 ]; then
        echo "used deprecated key \`${KEY_TASKM_MEM_MB}\`, please replace with key \`${KEY_TASKM_MEM_SIZE}\`"
else
        flink_tm_heap_bytes=$(parseBytes ${FLINK_TM_HEAP})
        FLINK_TM_HEAP_MB=$(getMebiBytes ${flink_tm_heap_bytes})
fi

if [[ ! ${FLINK_TM_HEAP_MB} =~ ${IS_NUMBER} ]] || [[ "${FLINK_TM_HEAP_MB}" -lt "0" ]]; then
    echo "[ERROR] Configured TaskManager JVM heap size is not a number. Please set '${KEY_TASKM_MEM_SIZE}' in ${FLINK_CONF_FILE}."
    exit 1
fi

if [ "${FLINK_TM_HEAP_MB}" -gt "0" ]; then

    TM_HEAP_SIZE=$(calculateTaskManagerHeapSizeMB)
    # Long.MAX_VALUE in TB: This is an upper bound, much less direct memory will be used
    TM_MAX_OFFHEAP_SIZE="8388607T"

    export JVM_ARGS="${JVM_ARGS} -Xms${TM_HEAP_SIZE}M -Xmx${TM_HEAP_SIZE}M -XX:MaxDirectMemorySize=${TM_MAX_OFFHEAP_SIZE}"

fi

# Add TaskManager-specific JVM options
export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_TM}"

# Startup parameters
ARGS=("--configDir" "${FLINK_CONF_DIR}")
echo "FLINK_LOG_DIR: ${FLINK_LOG_DIR}"
echo "MASTER_ARGS: ${ARGS[@]}"

CLASS_TO_RUN=org.apache.flink.runtime.taskexecutor.TaskManagerRunner
FLINK_TM_CLASSPATH=`constructFlinkClassPath`
FLINK_LOG_PREFIX="${FLINK_LOG_DIR}/flink-worker"

log="${FLINK_LOG_PREFIX}.log"
out="${FLINK_LOG_PREFIX}.out"

log_setting=("-Dlog.file=${log}" "-Dlog4j.configuration=file:${FLINK_CONF_DIR}/log4j.properties" "-Dlogback.configurationFile=file:${FLINK_CONF_DIR}/logback.xml")

JAVA_VERSION=$(${JAVA_RUN} -version 2>&1 | sed 's/.*version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')

# Only set JVM 8 arguments if we have correctly extracted the version
if [[ ${JAVA_VERSION} =~ ${IS_NUMBER} ]]; then
    if [ "$JAVA_VERSION" -lt 18 ]; then
        JVM_ARGS="$JVM_ARGS -XX:MaxPermSize=256m"
    fi
fi

MY_PID=$(ps -ef | grep "$CLASS_TO_RUN" | grep -v grep | awk '{print $2}')
if [ "${MY_PID}" = "" ];then
	# Rotate log files
	rotateLogFilesWithPrefix "$FLINK_LOG_DIR" "$FLINK_LOG_PREFIX"
	# Evaluate user options for local variable expansion
	FLINK_ENV_JAVA_OPTS=$(eval echo ${FLINK_ENV_JAVA_OPTS})
	CLASS_PATH=`manglePathList "$FLINK_TM_CLASSPATH:$(hadoop classpath)"`
	CLASS_PATH=$(echo "${CLASS_PATH}" | sed "s#"$FLINK_HOME"/lib/slf4j-log4j12-1.7.7.jar:##g")
	echo "Starting $DAEMON daemon (pid: $!) on host $HOSTNAME."
	exec $JAVA_RUN $JVM_ARGS ${FLINK_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "${CLASS_PATH}" ${CLASS_TO_RUN} "${ARGS[@]}" > "$out" 2>&1
else
	echo "$DAEMON daemon (pid: $MY_PID) is running on host $HOSTNAME."
fi

