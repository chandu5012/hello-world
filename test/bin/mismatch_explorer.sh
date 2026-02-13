#!/bin/bash
set -euo pipefail

log(){ echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] ${*:2}"; }
info(){ log INFO "$@"; }
mask_secret(){ [[ -z "${1:-}" || "${1:-}" == "N/A" ]] && echo "${1:-}" || echo "******"; }

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
APP_RUN="${APP_RUN:-/app/run}"
APP_PY="${APP_PY_MISMATCH:-/app/jobs/mismatch_explorer.py}"

info "=================================================="
info "Starting: $(basename "$0") | pid=$$ | host=$(hostname)"
info "=================================================="

run_id=""
source_db_type=""; source_host=""; source_port=""; source_database=""; source_username=""; source_password=""
source_table=""; source_pk=""; source_sql=""
target_db_type=""; target_host=""; target_port=""; target_database=""; target_username=""; target_password=""
target_table=""; target_pk=""; target_sql=""
trim_spaces="false"; ignore_case="false"; null_equals_empty="false"; numeric_tolerance="0"
time_zone="N/A"

for arg in "$@"; do
  case "$arg" in
    --run_id=*) run_id="${arg#*=}" ;;
    --source_db_type=*) source_db_type="${arg#*=}" ;;
    --source_host=*) source_host="${arg#*=}" ;;
    --source_port=*) source_port="${arg#*=}" ;;
    --source_database=*) source_database="${arg#*=}" ;;
    --source_username=*) source_username="${arg#*=}" ;;
    --source_password=*) source_password="${arg#*=}" ;;
    --source_table=*) source_table="${arg#*=}" ;;
    --source_pk=*) source_pk="${arg#*=}" ;;
    --source_sql=*) source_sql="${arg#*=}" ;;

    --target_db_type=*) target_db_type="${arg#*=}" ;;
    --target_host=*) target_host="${arg#*=}" ;;
    --target_port=*) target_port="${arg#*=}" ;;
    --target_database=*) target_database="${arg#*=}" ;;
    --target_username=*) target_username="${arg#*=}" ;;
    --target_password=*) target_password="${arg#*=}" ;;
    --target_table=*) target_table="${arg#*=}" ;;
    --target_pk=*) target_pk="${arg#*=}" ;;
    --target_sql=*) target_sql="${arg#*=}" ;;

    --trim_spaces=*) trim_spaces="${arg#*=}" ;;
    --ignore_case=*) ignore_case="${arg#*=}" ;;
    --null_equals_empty=*) null_equals_empty="${arg#*=}" ;;
    --numeric_tolerance=*) numeric_tolerance="${arg#*=}" ;;
    --time_zone=*) time_zone="${arg#*=}" ;;
  esac
done

tz="${time_zone:-N/A}"
driver_java_opts=""
[[ -n "$tz" && "$tz" != "N/A" ]] && driver_java_opts="-Duser.timezone=$tz"

# mismatch explorer small sizing
execs=1; cores=1; exec_mem="2g"; driver_mem="1g"

spark_cmd=(
  "$SPARK_HOME/bin/spark-submit"
  --master yarn --deploy-mode cluster
  --num-executors "$execs"
  --executor-cores "$cores"
  --executor-memory "$exec_mem"
  --driver-memory "$driver_mem"
)

[[ -n "$driver_java_opts" ]] && spark_cmd+=(
  --conf "spark.driver.extraJavaOptions=$driver_java_opts"
  --conf "spark.executor.extraJavaOptions=$driver_java_opts"
)

if [[ -f "$APP_RUN/cdp_profile" ]]; then
  source "$APP_RUN/cdp_profile"
  spark_cmd+=(
    --conf "spark.hadoop.fs.s3a.endpoint=${DPC_S3_ENDPOINT}"
    --conf "spark.hadoop.fs.s3a.security.credential.provider.path=${DPC_S3_JCEKS}"
    --conf "spark.hadoop.fs.s3a.path.style.access=true"
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=true"
    --conf "spark.sql.catalogImplementation=hive"
  )
fi

info "run_id=$run_id tz=$tz"
info "source=$source_db_type@$source_host:$source_port/$source_database user=$source_username pass=$(mask_secret "$source_password")"
info "target=$target_db_type@$target_host:$target_port/$target_database user=$target_username pass=$(mask_secret "$target_password")"

job_args=(
  "--run_id=$run_id"
  "--source_db_type=$source_db_type"
  "--source_host=$source_host"
  "--source_port=$source_port"
  "--source_database=$source_database"
  "--source_username=$source_username"
  "--source_password=$source_password"
  "--source_table=$source_table"
  "--source_pk=$source_pk"
  "--source_sql=$source_sql"

  "--target_db_type=$target_db_type"
  "--target_host=$target_host"
  "--target_port=$target_port"
  "--target_database=$target_database"
  "--target_username=$target_username"
  "--target_password=$target_password"
  "--target_table=$target_table"
  "--target_pk=$target_pk"
  "--target_sql=$target_sql"

  "--trim_spaces=$trim_spaces"
  "--ignore_case=$ignore_case"
  "--null_equals_empty=$null_equals_empty"
  "--numeric_tolerance=$numeric_tolerance"
  "--time_zone=$tz"
)

printf '[%s] [CMD ] ' "$(date '+%Y-%m-%d %H:%M:%S')"
printf '%q ' "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"; echo
exec "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"
