#!/bin/ksh

##############################################################################
# RID352 WRAPPER - HIVE ONLY VERSION
#
# USAGE:
#   ksh wrapper.ksh <ZONE> <RUN_MODE> <ENTITY_NAME> [SIZE]
#
# EXAMPLES:
#   ksh wrapper.ksh d FIRST_RUN all
#   ksh wrapper.ksh d FIRST_RUN all XLARGE
#   ksh wrapper.ksh s SECOND_RUN all MEDIUM
#   ksh wrapper.ksh u SINGLE_RUN cx6 SMALL
#
# RUN_MODE:
#   FIRST_RUN   -> if first run file exists, run cx6,cra,aud,acdv,php in order
#   SECOND_RUN  -> run based on trigger conditions
#   SINGLE_RUN  -> run only requested entity if eligible
#
# ENTITY_NAME:
#   all / cx6 / cra / aud / acdv / php
#
# SIZE:
#   SMALL / MEDIUM / LARGE / XLARGE
#   Optional. If not passed, entity default is used.
#
##############################################################################

##############################################################################
# Missing Parameter Check
##############################################################################
if [ $# -lt 3 ]; then
    echo "Unexpected number of arguments passed as input. Expected at least 3, Found $#"
    echo "USAGE: $0 <ZONE> <RUN_MODE> <ENTITY_NAME> [SIZE]"
    exit 1
else
    export ZONE="$1"
    export RUN_MODE="$2"
    export ENTITY_NAME="$3"
    export INPUT_SIZE="$4"
fi

##############################################################################
# INPUT PARAMETERS / COMMON VARIABLES
##############################################################################
export datetime=$(date +"%m_%d_%Y")
export START_TIME=$(date +"%Y-%m-%d %H:%M:%S.%6N")
export RUN_ID=$(date +"%m_%d_%Y")
export WRAPPER_LOG_NM="${ZONE}.RID352_${ENTITY_NAME}.${RUN_ID}_wrapper.log"
export LAUNCHER_LOG_NM="${ZONE}.RID352_${ENTITY_NAME}.${RUN_ID}_launcher.log"
export APP_NM="dsas"

##############################################################################
# Set environment parameter
##############################################################################
export edge_server_nm=$(hostname | cut -c 2)

##############################################################################
# ENV parameter resolution
# Update values as per your environment
##############################################################################
case "$ZONE" in
    d)
        export DSAS_APP_ID="etldsacd"
        export EDL_SERVER_NM="edldev2"
        export DB_PREFIX="dev"
        export APP_STATE="dev"
        export DCI_APP_ID="paseapas"
        export EMAIL_DL_ST="dsassupport@wellsfargo.com"
        export EMAIL_TO_LIST="dsassupport@wellsfargo.com"
        export EMAIL_CC_LIST="$EMAIL_DL_ST"
        export BEELINE_HIVE_CONNECT_STRING="jdbc:hive2://edl-b02-services.dev.bigdata.wellsfargo.net:10000/default;auth=maprsasl;saslQop=auth-conf;ssl=true"

        export SPARK_CONF_VAR="--conf spark.yarn.maxAppAttempts=1 --conf spark.sql.parquet.writeLegacyFormat=true --conf spark.yarn.archive=maprfs:///apps/spark-jars.zip --conf spark.sql.session.timeZone=UTC"
        export DCC_SPARK_DEPLOY_MODE="cluster"
        ;;

    s)
        export DSAS_APP_ID="etldsacs"
        export EDL_SERVER_NM="edlqa2"
        export DB_PREFIX="sit"
        export APP_STATE="sit"
        export DCI_APP_ID="paseapas"
        export EMAIL_DL_ST="dsassupport@wellsfargo.com"
        export EMAIL_TO_LIST="dsassupport@wellsfargo.com"
        export EMAIL_CC_LIST="$EMAIL_DL_ST"
        export BEELINE_HIVE_CONNECT_STRING="jdbc:hive2://edl-b02-services.sit.bigdata.wellsfargo.net:10000/default;auth=maprsasl;saslQop=auth-conf;ssl=true"

        export SPARK_CONF_VAR="--conf spark.yarn.maxAppAttempts=1 --conf spark.sql.parquet.writeLegacyFormat=true --conf spark.yarn.archive=maprfs:///apps/spark-jars.zip --conf spark.sql.session.timeZone=UTC"
        export DCC_SPARK_DEPLOY_MODE="cluster"
        ;;

    u)
        export DSAS_APP_ID="etldsacu"
        export EDL_SERVER_NM="edluat2"
        export DB_PREFIX="uat"
        export APP_STATE="uat"
        export DCI_APP_ID="paseapas"
        export EMAIL_DL_ST="dsassupport@wellsfargo.com"
        export EMAIL_TO_LIST="dsassupport@wellsfargo.com"
        export EMAIL_CC_LIST="$EMAIL_DL_ST"
        export BEELINE_HIVE_CONNECT_STRING="jdbc:hive2://edl-b02-services.uat.bigdata.wellsfargo.net:10000/default;auth=maprsasl;saslQop=auth-conf;ssl=true"

        export SPARK_CONF_VAR="--conf spark.yarn.maxAppAttempts=1 --conf spark.sql.parquet.writeLegacyFormat=true --conf spark.yarn.archive=maprfs:///apps/spark-jars.zip --conf spark.sql.session.timeZone=UTC"
        export DCC_SPARK_DEPLOY_MODE="cluster"
        ;;

    p)
        export DSAS_APP_ID="etldsacp"
        export EDL_SERVER_NM="edlprd2"
        export DB_PREFIX="prd"
        export APP_STATE="prd"
        export DCI_APP_ID="paseapas"
        export EMAIL_DL_ST="dsassupport@wellsfargo.com"
        export EMAIL_TO_LIST="dsassupport@wellsfargo.com"
        export EMAIL_CC_LIST="$EMAIL_DL_ST"
        export BEELINE_HIVE_CONNECT_STRING="jdbc:hive2://edl-b02-services.prd.bigdata.wellsfargo.net:10000/default;auth=maprsasl;saslQop=auth-conf;ssl=true"

        export SPARK_CONF_VAR="--conf spark.yarn.maxAppAttempts=1 --conf spark.sql.parquet.writeLegacyFormat=true --conf spark.yarn.archive=maprfs:///apps/spark-jars.zip --conf spark.sql.session.timeZone=UTC"
        export DCC_SPARK_DEPLOY_MODE="cluster"
        ;;

    *)
        echo "Invalid ZONE: $ZONE"
        exit 1
        ;;
esac

##############################################################################
# Common paths
##############################################################################
export LOG_PATH="/apps/run/${DSAS_APP_ID}/${APP_STATE}/${APP_NM}/log/app"
export DSAS_BASE_DATA_DIR="/apps/dat/${DSAS_APP_ID}/${APP_STATE}/dsas/serial"
export GENERIC_CONFIG_PATH="/apps/src/${DSAS_APP_ID}/${APP_STATE}/dsas-scripts/config/${ZONE}"
export EXEC_DIR="/apps/src/${DSAS_APP_ID}/${APP_STATE}/dsas-scripts"
export JAR_DIR="/apps/src/${DSAS_APP_ID}/${APP_STATE}/dsas-scripts/jars"
export LOG4J_CONFIG="/apps/src/${DSAS_APP_ID}/${APP_STATE}/dsas-scripts/config/${ZONE}/log4j.properties"

export WRAPPER_LOG="${LOG_PATH}/${WRAPPER_LOG_NM}"
export LAUNCHER_LOG="${LOG_PATH}/${LAUNCHER_LOG_NM}"

export FIRST_RUN_FILE="${GENERIC_CONFIG_PATH}/RID352_first_run.lkp"
export PROCESSED_FILE="${GENERIC_CONFIG_PATH}/RID352_first_run.prcd"
export MEMORY_CONF_FILE="${GENERIC_CONFIG_PATH}/spark_memory.conf"

##############################################################################
# Defaults / retry
##############################################################################
MAX_RETRIES=3
RETRY_WAIT_SEC=60

mkdir -p "$LOG_PATH"

echo "Script Begin..." >> "$WRAPPER_LOG"
echo "START_TIME: $START_TIME" >> "$WRAPPER_LOG"
echo "ZONE: $ZONE" >> "$WRAPPER_LOG"
echo "RUN_MODE: $RUN_MODE" >> "$WRAPPER_LOG"
echo "ENTITY_NAME: $ENTITY_NAME" >> "$WRAPPER_LOG"
echo "INPUT_SIZE: $INPUT_SIZE" >> "$WRAPPER_LOG"
echo "EDGE_SERVER_NM: $edge_server_nm" >> "$WRAPPER_LOG"
echo "EDL_SERVER_NM: $EDL_SERVER_NM" >> "$WRAPPER_LOG"
echo "MEMORY_CONF_FILE: $MEMORY_CONF_FILE" >> "$WRAPPER_LOG"

BATCH_START_EPOCH=$(date +%s)

##############################################################################
# Helper functions
##############################################################################
run_hive_scalar() {
    SQL_TXT="$1"
    beeline -u "$BEELINE_HIVE_CONNECT_STRING" \
        --silent=true \
        --showHeader=false \
        --outputformat=tsv2 \
        -e "$SQL_TXT" 2>>"$WRAPPER_LOG" | tail -1 | tr -d ' '
}

is_gt() {
    LEFT_VAL="$1"
    RIGHT_VAL="$2"
    [ -z "$LEFT_VAL" ] && return 1
    [ -z "$RIGHT_VAL" ] && return 0
    [ "$LEFT_VAL" \> "$RIGHT_VAL" ]
}

is_ge() {
    LEFT_VAL="$1"
    RIGHT_VAL="$2"
    [ -z "$LEFT_VAL" ] && return 1
    [ -z "$RIGHT_VAL" ] && return 0
    [ "$LEFT_VAL" \> "$RIGHT_VAL" ] || [ "$LEFT_VAL" = "$RIGHT_VAL" ]
}

##############################################################################
# Load memory / performance config from file
##############################################################################
load_memory_config() {
    SIZE="$1"

    [ -z "$SIZE" ] && SIZE="MEDIUM"

    if [ -f "$MEMORY_CONF_FILE" ]; then
        echo "Loading memory config for SIZE=$SIZE from $MEMORY_CONF_FILE" >> "$WRAPPER_LOG"

        . "$MEMORY_CONF_FILE"

        EXECUTOR_MEMORY=$(eval echo \$${SIZE}_EXECUTOR_MEMORY)
        DRIVER_MEMORY=$(eval echo \$${SIZE}_DRIVER_MEMORY)
        EXECUTOR_CORES=$(eval echo \$${SIZE}_EXECUTOR_CORES)
        NUM_EXECUTORS=$(eval echo \$${SIZE}_NUM_EXECUTORS)
        DRIVER_CORES=$(eval echo \$${SIZE}_DRIVER_CORES)
        EXECUTOR_MEMORY_OVERHEAD=$(eval echo \$${SIZE}_EXECUTOR_MEMORY_OVERHEAD)
        DRIVER_MEMORY_OVERHEAD=$(eval echo \$${SIZE}_DRIVER_MEMORY_OVERHEAD)
        DYNAMIC_ALLOCATION=$(eval echo \$${SIZE}_DYNAMIC_ALLOCATION)
        MIN_EXECUTORS=$(eval echo \$${SIZE}_MIN_EXECUTORS)
        MAX_EXECUTORS=$(eval echo \$${SIZE}_MAX_EXECUTORS)
        SHUFFLE_PARTITIONS=$(eval echo \$${SIZE}_SHUFFLE_PARTITIONS)
        DEFAULT_PARALLELISM=$(eval echo \$${SIZE}_DEFAULT_PARALLELISM)
        SQL_AUTO_BROADCAST=$(eval echo \$${SIZE}_SQL_AUTO_BROADCAST)
        NETWORK_TIMEOUT=$(eval echo \$${SIZE}_NETWORK_TIMEOUT)
        EXECUTOR_HEARTBEAT=$(eval echo \$${SIZE}_EXECUTOR_HEARTBEAT)
        KRYOSERIALIZER_BUFFER_MAX=$(eval echo \$${SIZE}_KRYOSERIALIZER_BUFFER_MAX)

    else
        echo "Memory config file not found. Using default fallback values." >> "$WRAPPER_LOG"

        EXECUTOR_MEMORY="4g"
        DRIVER_MEMORY="4g"
        EXECUTOR_CORES=2
        NUM_EXECUTORS=6
        DRIVER_CORES=1
        EXECUTOR_MEMORY_OVERHEAD="1g"
        DRIVER_MEMORY_OVERHEAD="1g"
        DYNAMIC_ALLOCATION="false"
        MIN_EXECUTORS=1
        MAX_EXECUTORS=6
        SHUFFLE_PARTITIONS=200
        DEFAULT_PARALLELISM=50
        SQL_AUTO_BROADCAST="-1"
        NETWORK_TIMEOUT="800s"
        EXECUTOR_HEARTBEAT="60s"
        KRYOSERIALIZER_BUFFER_MAX="512m"
    fi

    # Safety fallback if any value is blank
    [ -z "$EXECUTOR_MEMORY" ] && EXECUTOR_MEMORY="4g"
    [ -z "$DRIVER_MEMORY" ] && DRIVER_MEMORY="4g"
    [ -z "$EXECUTOR_CORES" ] && EXECUTOR_CORES=2
    [ -z "$NUM_EXECUTORS" ] && NUM_EXECUTORS=6
    [ -z "$DRIVER_CORES" ] && DRIVER_CORES=1
    [ -z "$EXECUTOR_MEMORY_OVERHEAD" ] && EXECUTOR_MEMORY_OVERHEAD="1g"
    [ -z "$DRIVER_MEMORY_OVERHEAD" ] && DRIVER_MEMORY_OVERHEAD="1g"
    [ -z "$DYNAMIC_ALLOCATION" ] && DYNAMIC_ALLOCATION="false"
    [ -z "$MIN_EXECUTORS" ] && MIN_EXECUTORS=1
    [ -z "$MAX_EXECUTORS" ] && MAX_EXECUTORS=6
    [ -z "$SHUFFLE_PARTITIONS" ] && SHUFFLE_PARTITIONS=200
    [ -z "$DEFAULT_PARALLELISM" ] && DEFAULT_PARALLELISM=50
    [ -z "$SQL_AUTO_BROADCAST" ] && SQL_AUTO_BROADCAST="-1"
    [ -z "$NETWORK_TIMEOUT" ] && NETWORK_TIMEOUT="800s"
    [ -z "$EXECUTOR_HEARTBEAT" ] && EXECUTOR_HEARTBEAT="60s"
    [ -z "$KRYOSERIALIZER_BUFFER_MAX" ] && KRYOSERIALIZER_BUFFER_MAX="512m"

    echo "Loaded SIZE=$SIZE EXECUTOR_MEMORY=$EXECUTOR_MEMORY DRIVER_MEMORY=$DRIVER_MEMORY EXECUTOR_CORES=$EXECUTOR_CORES NUM_EXECUTORS=$NUM_EXECUTORS" >> "$WRAPPER_LOG"
    echo "MEMORY_OVERHEAD EXECUTOR=$EXECUTOR_MEMORY_OVERHEAD DRIVER=$DRIVER_MEMORY_OVERHEAD DYNAMIC_ALLOCATION=$DYNAMIC_ALLOCATION MIN_EXECUTORS=$MIN_EXECUTORS MAX_EXECUTORS=$MAX_EXECUTORS" >> "$WRAPPER_LOG"
    echo "PERFORMANCE SHUFFLE_PARTITIONS=$SHUFFLE_PARTITIONS DEFAULT_PARALLELISM=$DEFAULT_PARALLELISM SQL_AUTO_BROADCAST=$SQL_AUTO_BROADCAST NETWORK_TIMEOUT=$NETWORK_TIMEOUT EXECUTOR_HEARTBEAT=$EXECUTOR_HEARTBEAT KRYO_BUFFER_MAX=$KRYOSERIALIZER_BUFFER_MAX" >> "$WRAPPER_LOG"
}

##############################################################################
# Trigger source / target date helpers - HIVE ONLY
##############################################################################
get_src_aud_max() {
    run_hive_scalar "
        select cast(max(date_created) as string)
        from dsas_fnish_sanitized.eoscar_aud_hist
    "
}

get_src_acdv_max() {
    run_hive_scalar "
        select cast(max(acdvresp_response_date_time) as string)
        from dsas_fnish_sanitized.eoscar_acdvarchive_hist
    "
}

get_src_cra_max() {
    run_hive_scalar "
        select cast(max(EventDate) as string)
        from dsas_fnish_sanitized.CRA_Maintenance
    "
}

get_src_cx6_hive_max() {
    run_hive_scalar "
        select cast(max(Approval_Decision_Date) as string)
        from dsas_fnish_sanitized.File_Progressions
        where Approval_Decision_ID = 1
          and System_Of_Record_ID in (2,6,4,13,14)
    "
}

get_tgt_cx6_max() {
    run_hive_scalar "
        select cast(max(Report_Date) as string)
        from dsas_conformed.RID352_RecommendedPHP_CX6
    "
}

get_tgt_acdv_max() {
    run_hive_scalar "
        select cast(max(Report_Date) as string)
        from dsas_conformed.RID352_RecommendedPHP_ACDV
    "
}

get_tgt_aud_max() {
    run_hive_scalar "
        select cast(max(Report_Date) as string)
        from dsas_conformed.RID352_RecommendedPHP_AUD
    "
}

get_tgt_cra_max() {
    run_hive_scalar "
        select cast(max(Report_Date) as string)
        from dsas_conformed.RID352_RecommendedPHP_CRA
    "
}

get_tgt_php_max() {
    run_hive_scalar "
        select cast(max(Report_Date) as string)
        from dsas_conformed.RID352_RecommendedPHP
    "
}

##############################################################################
# Completion / trigger checks
##############################################################################
is_cx6_completed() {
    CX6_TGT_MAX=$(get_tgt_cx6_max)
    AUD_SRC_MAX=$(get_src_aud_max)
    ACDV_SRC_MAX=$(get_src_acdv_max)
    CRA_SRC_MAX=$(get_src_cra_max)
    FP_SRC_MAX=$(get_src_cx6_hive_max)

    if is_ge "$CX6_TGT_MAX" "$AUD_SRC_MAX" &&
       is_ge "$CX6_TGT_MAX" "$ACDV_SRC_MAX" &&
       is_ge "$CX6_TGT_MAX" "$CRA_SRC_MAX" &&
       is_ge "$CX6_TGT_MAX" "$FP_SRC_MAX"; then
        return 0
    else
        return 1
    fi
}

should_run_cx6() {
    CX6_TGT_MAX=$(get_tgt_cx6_max)
    AUD_SRC_MAX=$(get_src_aud_max)
    ACDV_SRC_MAX=$(get_src_acdv_max)
    CRA_SRC_MAX=$(get_src_cra_max)
    FP_SRC_MAX=$(get_src_cx6_hive_max)

    echo "CX6 target max = $CX6_TGT_MAX" >> "$WRAPPER_LOG"
    echo "AUD source max = $AUD_SRC_MAX" >> "$WRAPPER_LOG"
    echo "ACDV source max = $ACDV_SRC_MAX" >> "$WRAPPER_LOG"
    echo "CRA source max = $CRA_SRC_MAX" >> "$WRAPPER_LOG"
    echo "File progression source max = $FP_SRC_MAX" >> "$WRAPPER_LOG"

    if is_gt "$AUD_SRC_MAX" "$CX6_TGT_MAX" ||
       is_gt "$ACDV_SRC_MAX" "$CX6_TGT_MAX" ||
       is_gt "$CRA_SRC_MAX" "$CX6_TGT_MAX" ||
       is_gt "$FP_SRC_MAX" "$CX6_TGT_MAX"; then
        return 0
    else
        return 1
    fi
}

should_run_acdv() {
    ACDV_TGT_MAX=$(get_tgt_acdv_max)
    AUD_SRC_MAX=$(get_src_aud_max)

    echo "ACDV target max = $ACDV_TGT_MAX" >> "$WRAPPER_LOG"
    echo "AUD source max for ACDV trigger = $AUD_SRC_MAX" >> "$WRAPPER_LOG"

    if is_gt "$AUD_SRC_MAX" "$ACDV_TGT_MAX" && is_cx6_completed; then
        return 0
    else
        return 1
    fi
}

should_run_aud() {
    AUD_TGT_MAX=$(get_tgt_aud_max)
    ACDV_SRC_MAX=$(get_src_acdv_max)

    echo "AUD target max = $AUD_TGT_MAX" >> "$WRAPPER_LOG"
    echo "ACDV source max for AUD trigger = $ACDV_SRC_MAX" >> "$WRAPPER_LOG"

    if is_gt "$ACDV_SRC_MAX" "$AUD_TGT_MAX" && is_cx6_completed; then
        return 0
    else
        return 1
    fi
}

should_run_cra() {
    CRA_TGT_MAX=$(get_tgt_cra_max)
    CRA_SRC_MAX=$(get_src_cra_max)

    echo "CRA target max = $CRA_TGT_MAX" >> "$WRAPPER_LOG"
    echo "CRA source max for CRA trigger = $CRA_SRC_MAX" >> "$WRAPPER_LOG"

    if is_gt "$CRA_SRC_MAX" "$CRA_TGT_MAX" && is_cx6_completed; then
        return 0
    else
        return 1
    fi
}

should_run_php() {
    PHP_TGT_MAX=$(get_tgt_php_max)
    CX6_TGT_MAX=$(get_tgt_cx6_max)
    ACDV_TGT_MAX=$(get_tgt_acdv_max)
    AUD_TGT_MAX=$(get_tgt_aud_max)
    CRA_TGT_MAX=$(get_tgt_cra_max)

    LATEST_UPSTREAM="$CX6_TGT_MAX"
    [ "$ACDV_TGT_MAX" \> "$LATEST_UPSTREAM" ] && LATEST_UPSTREAM="$ACDV_TGT_MAX"
    [ "$AUD_TGT_MAX"  \> "$LATEST_UPSTREAM" ] && LATEST_UPSTREAM="$AUD_TGT_MAX"
    [ "$CRA_TGT_MAX"  \> "$LATEST_UPSTREAM" ] && LATEST_UPSTREAM="$CRA_TGT_MAX"

    echo "PHP target max = $PHP_TGT_MAX" >> "$WRAPPER_LOG"
    echo "Latest upstream output max = $LATEST_UPSTREAM" >> "$WRAPPER_LOG"

    if is_gt "$LATEST_UPSTREAM" "$PHP_TGT_MAX"; then
        return 0
    else
        return 1
    fi
}

should_run_entity() {
    case "$1" in
        cx6)  should_run_cx6 ;;
        aud)  should_run_aud ;;
        cra)  should_run_cra ;;
        acdv) should_run_acdv ;;
        php)  should_run_php ;;
        *)    return 1 ;;
    esac
}

##############################################################################
# Entity config with app file + app args + size
##############################################################################
get_config() {
    ENTITY="$1"

    case "$ENTITY" in
        cx6)
            APP_PY="${EXEC_DIR}/py/RID352_RecommendedPHP_CX6.py"
            APP_ARGS="--zone ${ZONE} --run_mode ${RUN_MODE} --entity_name cx6 --target_table RID352_RecommendedPHP_CX6"
            DEFAULT_SIZE="XLARGE"
            ;;
        php)
            APP_PY="${EXEC_DIR}/py/RID352_RecommendedPHP.py"
            APP_ARGS="--zone ${ZONE} --run_mode ${RUN_MODE} --entity_name php --target_table RID352_RecommendedPHP"
            DEFAULT_SIZE="XLARGE"
            ;;
        cra)
            APP_PY="${EXEC_DIR}/py/RID352_RecommendedPHP_CRA.py"
            APP_ARGS="--zone ${ZONE} --run_mode ${RUN_MODE} --entity_name cra --target_table RID352_RecommendedPHP_CRA"
            DEFAULT_SIZE="SMALL"
            ;;
        aud)
            APP_PY="${EXEC_DIR}/py/RID352_RecommendedPHP_AUD.py"
            APP_ARGS="--zone ${ZONE} --run_mode ${RUN_MODE} --entity_name aud --target_table RID352_RecommendedPHP_AUD"
            DEFAULT_SIZE="SMALL"
            ;;
        acdv)
            APP_PY="${EXEC_DIR}/py/RID352_RecommendedPHP_ACDV.py"
            APP_ARGS="--zone ${ZONE} --run_mode ${RUN_MODE} --entity_name acdv --target_table RID352_RecommendedPHP_ACDV"
            DEFAULT_SIZE="SMALL"
            ;;
        *)
            echo "Invalid ENTITY in get_config: $ENTITY" >> "$WRAPPER_LOG"
            return 1
            ;;
    esac

    if [ -n "$INPUT_SIZE" ]; then
        FINAL_SIZE="$INPUT_SIZE"
    else
        FINAL_SIZE="$DEFAULT_SIZE"
    fi

    echo "ENTITY=$ENTITY DEFAULT_SIZE=$DEFAULT_SIZE FINAL_SIZE=$FINAL_SIZE" >> "$WRAPPER_LOG"

    load_memory_config "$FINAL_SIZE"

    return 0
}

##############################################################################
# Build spark submit dynamically
##############################################################################
build_spark_submit_cmd() {
    ENTITY="$1"
    get_config "$ENTITY" || return 1

    SPARK_SUBMIT_CMD="spark-submit \
      --master yarn \
      --queue ${DSAS_APP_ID} \
      --deploy-mode ${DCC_SPARK_DEPLOY_MODE} \
      --driver-memory ${DRIVER_MEMORY} \
      --executor-memory ${EXECUTOR_MEMORY} \
      --executor-cores ${EXECUTOR_CORES} \
      --num-executors ${NUM_EXECUTORS} \
      --conf spark.driver.cores=${DRIVER_CORES} \
      --conf spark.executor.memoryOverhead=${EXECUTOR_MEMORY_OVERHEAD} \
      --conf spark.driver.memoryOverhead=${DRIVER_MEMORY_OVERHEAD} \
      --conf spark.dynamicAllocation.enabled=${DYNAMIC_ALLOCATION} \
      --conf spark.dynamicAllocation.minExecutors=${MIN_EXECUTORS} \
      --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} \
      --conf spark.sql.shuffle.partitions=${SHUFFLE_PARTITIONS} \
      --conf spark.default.parallelism=${DEFAULT_PARALLELISM} \
      --conf spark.sql.autoBroadcastJoinThreshold=${SQL_AUTO_BROADCAST} \
      --conf spark.network.timeout=${NETWORK_TIMEOUT} \
      --conf spark.executor.heartbeatInterval=${EXECUTOR_HEARTBEAT} \
      --conf spark.kryoserializer.buffer.max=${KRYOSERIALIZER_BUFFER_MAX} \
      ${SPARK_CONF_VAR} \
      --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${LOG4J_CONFIG} \
      --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:${LOG4J_CONFIG} \
      --jars ${JAR_DIR}/*.jar"
}

##############################################################################
# Run with retry
##############################################################################
run_process_with_retry() {
    ENTITY="$1"
    get_config "$ENTITY" || return 1
    build_spark_submit_cmd "$ENTITY" || return 1

    ATTEMPT=1
    while [ $ATTEMPT -le $MAX_RETRIES ]
    do
        echo "[$ENTITY] Attempt $ATTEMPT started" >> "$WRAPPER_LOG"
        echo "[$ENTITY] APP_PY=$APP_PY" >> "$WRAPPER_LOG"
        echo "[$ENTITY] APP_ARGS=$APP_ARGS" >> "$WRAPPER_LOG"
        echo "[$ENTITY] FINAL_SIZE=$FINAL_SIZE" >> "$WRAPPER_LOG"
        echo "[$ENTITY] DRIVER_MEMORY=$DRIVER_MEMORY EXECUTOR_MEMORY=$EXECUTOR_MEMORY EXECUTOR_CORES=$EXECUTOR_CORES NUM_EXECUTORS=$NUM_EXECUTORS" >> "$WRAPPER_LOG"
        echo "[$ENTITY] DRIVER_CORES=$DRIVER_CORES EXECUTOR_MEMORY_OVERHEAD=$EXECUTOR_MEMORY_OVERHEAD DRIVER_MEMORY_OVERHEAD=$DRIVER_MEMORY_OVERHEAD" >> "$WRAPPER_LOG"
        echo "[$ENTITY] DYNAMIC_ALLOCATION=$DYNAMIC_ALLOCATION MIN_EXECUTORS=$MIN_EXECUTORS MAX_EXECUTORS=$MAX_EXECUTORS SHUFFLE_PARTITIONS=$SHUFFLE_PARTITIONS DEFAULT_PARALLELISM=$DEFAULT_PARALLELISM" >> "$WRAPPER_LOG"
        echo "[$ENTITY] SPARK CMD=$SPARK_SUBMIT_CMD" >> "$WRAPPER_LOG"

        eval $SPARK_SUBMIT_CMD \"$APP_PY\" $APP_ARGS >> "$LAUNCHER_LOG" 2>&1
        RC=$?

        if [ $RC -eq 0 ]; then
            echo "[$ENTITY] SUCCESS on attempt $ATTEMPT" >> "$WRAPPER_LOG"
            return 0
        fi

        echo "[$ENTITY] FAILED on attempt $ATTEMPT RC=$RC" >> "$WRAPPER_LOG"
        ATTEMPT=$((ATTEMPT + 1))

        if [ $ATTEMPT -le $MAX_RETRIES ]; then
            echo "[$ENTITY] Waiting $RETRY_WAIT_SEC sec before retry" >> "$WRAPPER_LOG"
            sleep $RETRY_WAIT_SEC
        fi
    done

    echo "[$ENTITY] FAILED after $MAX_RETRIES attempts" >> "$WRAPPER_LOG"
    return 1
}

##############################################################################
# Parallel middle group
##############################################################################
run_parallel_group() {
    SUCCESS_FILE="/tmp/rid352_parallel_success_$$.txt"
    : > "$SUCCESS_FILE"

    for P_ENTITY in cra aud acdv
    do
        (
            if should_run_entity "$P_ENTITY"; then
                echo "$P_ENTITY eligible, starting in parallel" >> "$WRAPPER_LOG"
                run_process_with_retry "$P_ENTITY"
                P_RC=$?
                if [ $P_RC -eq 0 ]; then
                    echo "$P_ENTITY" >> "$SUCCESS_FILE"
                fi
                exit $P_RC
            else
                echo "$P_ENTITY not eligible, skipping" >> "$WRAPPER_LOG"
                exit 2
            fi
        ) &
        PID=$!
        echo "$P_ENTITY started in parallel PID=$PID" >> "$WRAPPER_LOG"

        case "$P_ENTITY" in
            cra) CRA_PID=$PID ;;
            aud) AUD_PID=$PID ;;
            acdv) ACDV_PID=$PID ;;
        esac
    done

    wait $CRA_PID
    CRA_RC=$?
    wait $AUD_PID
    AUD_RC=$?
    wait $ACDV_PID
    ACDV_RC=$?

    echo "Parallel RCs CRA=$CRA_RC AUD=$AUD_RC ACDV=$ACDV_RC" >> "$WRAPPER_LOG"

    if [ -s "$SUCCESS_FILE" ]; then
        rm -f "$SUCCESS_FILE"
        return 0
    else
        rm -f "$SUCCESS_FILE"
        return 1
    fi
}

##############################################################################
# Flows
##############################################################################
run_first_run_flow() {
    if [ ! -f "$FIRST_RUN_FILE" ]; then
        echo "First-run file not available. Exiting." >> "$WRAPPER_LOG"
        exit 0
    fi

    echo "FIRST RUN FLOW STARTED" >> "$WRAPPER_LOG"

    for F_ENTITY in cx6 cra aud acdv php
    do
        run_process_with_retry "$F_ENTITY" || exit 1
    done

    mv "$FIRST_RUN_FILE" "$PROCESSED_FILE"
    RC=$?
    if [ $RC -ne 0 ]; then
        echo "Failed to rename first run file" >> "$WRAPPER_LOG"
        exit 1
    fi

    echo "FIRST RUN FLOW COMPLETED" >> "$WRAPPER_LOG"
}

run_second_run_flow() {
    echo "SECOND RUN FLOW STARTED" >> "$WRAPPER_LOG"

    if should_run_cx6; then
        echo "cx6 eligible" >> "$WRAPPER_LOG"
        run_process_with_retry "cx6" || exit 1
    else
        echo "cx6 not eligible, skipping" >> "$WRAPPER_LOG"
    fi

    if is_cx6_completed; then
        run_parallel_group
        MIDDLE_RC=$?
        echo "Middle parallel group RC=$MIDDLE_RC" >> "$WRAPPER_LOG"
    else
        echo "cx6 not completed, skipping cra/aud/acdv" >> "$WRAPPER_LOG"
    fi

    if should_run_php; then
        echo "php eligible at end" >> "$WRAPPER_LOG"
        run_process_with_retry "php" || exit 1
    else
        echo "php not eligible, skipping" >> "$WRAPPER_LOG"
    fi

    echo "SECOND RUN FLOW COMPLETED" >> "$WRAPPER_LOG"
}

run_single_process_flow() {
    case "$ENTITY_NAME" in
        cx6|cra|aud|acdv|php)
            if should_run_entity "$ENTITY_NAME"; then
                echo "$ENTITY_NAME eligible in single run" >> "$WRAPPER_LOG"
                run_process_with_retry "$ENTITY_NAME" || exit 1
            else
                echo "$ENTITY_NAME not eligible in single run, skipping" >> "$WRAPPER_LOG"
                exit 0
            fi
            ;;
        *)
            echo "Invalid ENTITY_NAME for SINGLE_RUN: $ENTITY_NAME" >> "$WRAPPER_LOG"
            exit 1
            ;;
    esac
}

##############################################################################
# Main control
##############################################################################
case "$RUN_MODE" in
    FIRST_RUN)
        run_first_run_flow
        ;;
    SECOND_RUN)
        run_second_run_flow
        ;;
    SINGLE_RUN)
        run_single_process_flow
        ;;
    *)
        echo "Invalid RUN_MODE: $RUN_MODE" >> "$WRAPPER_LOG"
        exit 1
        ;;
esac

##############################################################################
# End logging
##############################################################################
BATCH_END_TS=$(date '+%Y-%m-%d %H:%M:%S')
BATCH_END_EPOCH=$(date +%s)
TOTAL_DURATION=$((BATCH_END_EPOCH - BATCH_START_EPOCH))

echo "WRAPPER ENDED AT: $BATCH_END_TS" >> "$WRAPPER_LOG"
echo "TOTAL DURATION SEC: $TOTAL_DURATION" >> "$WRAPPER_LOG"
echo "Script End..." >> "$WRAPPER_LOG"

exit 0
