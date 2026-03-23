#!/bin/ksh

##############################################################################
# WRAPPER SCRIPT - FINAL (PARALLEL + RETRY + DYNAMIC ARGS)
##############################################################################

########################
# INPUT PARAMETERS
########################
RUN_MODE="$1"
PROCESS_NAME="$2"

########################
# CONFIG
########################
EXEC_DIR="/your/project/path"
FIRST_RUN_FILE="/your/path/first_run.lkp"
PROCESSED_FILE="/your/path/first_run.prcd"
LOG_DIR="/your/path/logs"

MAX_RETRIES=3
RETRY_WAIT_SEC=60

mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/wrapper_${RUN_MODE}_$(date +%Y%m%d_%H%M%S).log"

BATCH_START_EPOCH=$(date +%s)

########################
# SPARK CONFIG
########################
SPARK_SUBMIT="spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 4G \
  --executor-memory 8G \
  --num-executors 4 \
  --executor-cores 2"

########################
# ENTITY CONFIG (UPDATED)
########################
get_config() {
    ENTITY="$1"

    case "$ENTITY" in
        cx6)
            HIVE_DB="db_cx6"
            CHECK_TABLE="cx6_delta"
            DELTA_COL="cx6_dt"
            APP_PY="${EXEC_DIR}/py/cx6_process.py"
            APP_ARGS="--entity cx6 --mode full"
            ;;

        cra)
            HIVE_DB="db_cra"
            CHECK_TABLE="cra_delta"
            DELTA_COL="cra_dt"
            APP_PY="${EXEC_DIR}/py/cra_process.py"
            APP_ARGS="--entity cra --type file"
            ;;

        aud)
            HIVE_DB="db_aud"
            CHECK_TABLE="aud_delta"
            DELTA_COL="aud_dt"
            APP_PY="${EXEC_DIR}/py/aud_process.py"
            APP_ARGS="--entity aud --type db"
            ;;

        acdv)
            HIVE_DB="db_acdv"
            CHECK_TABLE="acdv_delta"
            DELTA_COL="acdv_dt"
            APP_PY="${EXEC_DIR}/py/acdv_process.py"
            APP_ARGS="--entity acdv --format parquet"
            ;;

        php)
            HIVE_DB="db_php"
            CHECK_TABLE="php_delta"
            DELTA_COL="php_dt"
            APP_PY="${EXEC_DIR}/py/php_process.py"
            APP_ARGS="--entity php --final yes"
            ;;

        *)
            echo "Invalid ENTITY=$ENTITY" >> "$LOG_FILE"
            return 1
            ;;
    esac

    return 0
}

########################
# GET DELTA VALUE
########################
get_delta_value() {
    ENTITY="$1"
    get_config "$ENTITY" || return 1

    SQL="select max(${DELTA_COL}) from ${HIVE_DB}.${CHECK_TABLE}"

    VAL=$(hive -S -e "$SQL" 2>>"$LOG_FILE" | tail -1 | tr -d ' ')

    [ -z "$VAL" ] && return 1

    echo "$VAL"
}

########################
# CHECK COUNT
########################
check_count() {
    ENTITY="$1"
    get_config "$ENTITY" || return 1

    VAL=$(get_delta_value "$ENTITY") || return 1

    SQL="select count(1) from ${HIVE_DB}.${CHECK_TABLE}
         where ${DELTA_COL}='${VAL}'"

    CNT=$(hive -S -e "$SQL" 2>>"$LOG_FILE" | tail -1 | tr -d ' ')
    echo "${CNT}|${VAL}"
}

########################
# RUN PROCESS WITH RETRY (UPDATED)
########################
run_process_with_retry() {
    ENTITY="$1"
    get_config "$ENTITY" || return 1

    VAL=$(get_delta_value "$ENTITY") || return 1

    ATTEMPT=1

    while [ $ATTEMPT -le $MAX_RETRIES ]
    do
        echo "[$ENTITY] Attempt $ATTEMPT" >> "$LOG_FILE"
        echo "APP_PY=$APP_PY" >> "$LOG_FILE"
        echo "APP_ARGS=$APP_ARGS" >> "$LOG_FILE"
        echo "DELTA_VAL=$VAL" >> "$LOG_FILE"

        $SPARK_SUBMIT "$APP_PY" \
            $APP_ARGS \
            --delta_col "$DELTA_COL" \
            --delta_val "$VAL" \
            >> "$LOG_FILE" 2>&1

        RC=$?

        if [ $RC -eq 0 ]; then
            echo "[$ENTITY] SUCCESS" >> "$LOG_FILE"
            return 0
        fi

        echo "[$ENTITY] FAILED RC=$RC" >> "$LOG_FILE"

        ATTEMPT=$((ATTEMPT + 1))

        if [ $ATTEMPT -le $MAX_RETRIES ]; then
            echo "Retrying in ${RETRY_WAIT_SEC}s..." >> "$LOG_FILE"
            sleep $RETRY_WAIT_SEC
        fi
    done

    echo "[$ENTITY] FAILED after retries" >> "$LOG_FILE"
    return 1
}

########################
# RUN IF DATA
########################
run_if_data() {
    ENTITY="$1"

    RESULT=$(check_count "$ENTITY") || return 1
    CNT=$(echo "$RESULT" | cut -d'|' -f1)

    if [ "$CNT" -lt 1 ]; then
        echo "$ENTITY skipped (no data)" >> "$LOG_FILE"
        return 2
    fi

    run_process_with_retry "$ENTITY"
    return $?
}

########################
# PARALLEL EXECUTION
########################
run_parallel_group() {
    ANY_SUCCESS=0
    PIDS=""

    for ENTITY in cra aud acdv
    do
        run_if_data "$ENTITY" &
        PID=$!
        echo "$ENTITY started PID=$PID" >> "$LOG_FILE"
        PIDS="$PIDS $PID"
    done

    for PID in $PIDS
    do
        wait $PID
        RC=$?

        if [ $RC -eq 0 ]; then
            ANY_SUCCESS=1
        fi
    done

    echo "$ANY_SUCCESS"
}

########################
# FIRST RUN
########################
run_first_run() {

    if [ ! -f "$FIRST_RUN_FILE" ]; then
        echo "First-run file missing → exit" >> "$LOG_FILE"
        exit 0
    fi

    for ENTITY in cx6 cra aud acdv php
    do
        run_process_with_retry "$ENTITY" || exit 1
    done

    mv "$FIRST_RUN_FILE" "$PROCESSED_FILE"
}

########################
# SECOND RUN
########################
run_second_run() {

    run_if_data "cx6"

    ANY_SUCCESS=$(run_parallel_group)

    if [ "$ANY_SUCCESS" -eq 1 ]; then
        run_if_data "php"
    else
        echo "No upstream success → skip php" >> "$LOG_FILE"
    fi
}

########################
# SINGLE RUN
########################
run_single() {
    run_process_with_retry "$PROCESS_NAME"
}

########################
# MAIN
########################
case "$RUN_MODE" in
    AUTO|FIRST_RUN)
        run_first_run
        ;;
    SECOND_RUN)
        run_second_run
        ;;
    SINGLE_RUN)
        run_single
        ;;
    *)
        echo "Invalid RUN_MODE" >> "$LOG_FILE"
        exit 1
        ;;
esac

END=$(date +%s)
echo "Total duration: $((END - BATCH_START_EPOCH)) sec" >> "$LOG_FILE"

exit 0
