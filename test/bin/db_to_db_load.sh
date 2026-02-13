#!/bin/bash
set -euo pipefail

log(){ echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] ${*:2}"; }
info(){ log INFO "$@"; }

mask_secret(){ [[ -z "${1:-}" || "${1:-}" == "N/A" ]] && echo "${1:-}" || echo "******"; }

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
APP_RUN="${APP_RUN:-/app/run}"
APP_PY="${APP_PY_DB_LOAD:-/app/jobs/db_to_db_load.py}"

info "=================================================="
info "Starting: $(basename "$0") | pid=$$ | host=$(hostname)"
info "=================================================="

# Vars
load_source_type=""; load_source_host=""; load_source_port=""
load_source_database=""; load_source_username=""; load_source_password=""
load_source_table_name=""; load_source_primary_key=""; load_source_sql_query=""

load_target_type=""; load_target_host=""; load_target_port=""
load_target_database=""; load_target_username=""; load_target_password=""
load_target_table_name=""; load_target_primary_key=""; load_target_sql_query=""
load_target_load_type=""; load_target_hadoop_path=""; load_target_partition_id=""

load_time_zone="N/A"; time_zone="N/A"
load_unix_host=""; load_user_email=""; load_batch_id=""; load_unix_username=""; load_unix_password=""
target_load_type=""; hadoop_path=""; partition_id=""; table_mode=""
source_file_sql_query="N/A"

# Parse (FULL)
for arg in "$@"; do
  case "$arg" in
    --load_source_type=*) load_source_type="${arg#*=}" ;;
    --load_source_host=*) load_source_host="${arg#*=}" ;;
    --load_source_port=*) load_source_port="${arg#*=}" ;;
    --load_source_database=*) load_source_database="${arg#*=}" ;;
    --load_source_username=*) load_source_username="${arg#*=}" ;;
    --load_source_password=*) load_source_password="${arg#*=}" ;;
    --load_source_table_name=*) load_source_table_name="${arg#*=}" ;;
    --load_source_primary_key=*) load_source_primary_key="${arg#*=}" ;;
    --load_source_sql_query=*) load_source_sql_query="${arg#*=}" ;;

    --load_target_type=*) load_target_type="${arg#*=}" ;;
    --load_target_host=*) load_target_host="${arg#*=}" ;;
    --load_target_port=*) load_target_port="${arg#*=}" ;;
    --load_target_database=*) load_target_database="${arg#*=}" ;;
    --load_target_username=*) load_target_username="${arg#*=}" ;;
    --load_target_password=*) load_target_password="${arg#*=}" ;;
    --load_target_table_name=*) load_target_table_name="${arg#*=}" ;;
    --load_target_primary_key=*) load_target_primary_key="${arg#*=}" ;;
    --load_target_sql_query=*) load_target_sql_query="${arg#*=}" ;;
    --load_target_load_type=*) load_target_load_type="${arg#*=}" ;;
    --load_target_hadoop_path=*) load_target_hadoop_path="${arg#*=}" ;;
    --load_target_partition_id=*) load_target_partition_id="${arg#*=}" ;;

    --load_time_zone=*) load_time_zone="${arg#*=}" ;;
    --load_unix_host=*) load_unix_host="${arg#*=}" ;;
    --load_user_email=*) load_user_email="${arg#*=}" ;;
    --load_batch_id=*) load_batch_id="${arg#*=}" ;;
    --load_unix_username=*) load_unix_username="${arg#*=}" ;;
    --load_unix_password=*) load_unix_password="${arg#*=}" ;;

    --time_zone=*) time_zone="${arg#*=}" ;;
    --target_load_type=*) target_load_type="${arg#*=}" ;;
    --hadoop_path=*) hadoop_path="${arg#*=}" ;;
    --partition_id=*) partition_id="${arg#*=}" ;;
    --table_mode=*) table_mode="${arg#*=}" ;;
    --source_file_sql_query=*) source_file_sql_query="${arg#*=}" ;;
  esac
done

# Region from load_unix_host
region="dev"
h="${load_unix_host:-}"
shopt -s nocasematch
[[ "$h" == *"sit"* ]] && region="sit"
[[ "$h" == *"uat"* ]] && region="uat"
[[ "$h" == *"dev"* ]] && region="dev"
shopt -u nocasematch

case "$region" in
  dev) execs=2; cores=2; exec_mem="4g"; driver_mem="2g" ;;
  sit) execs=4; cores=3; exec_mem="8g"; driver_mem="4g" ;;
  uat) execs=6; cores=4; exec_mem="12g"; driver_mem="6g" ;;
esac

# TZ priority load_time_zone > time_zone
tz="${load_time_zone:-N/A}"
[[ -z "$tz" || "$tz" == "N/A" ]] && tz="${time_zone:-N/A}"

driver_java_opts=""
if [[ -n "$tz" && "$tz" != "N/A" ]]; then
  driver_java_opts="-Duser.timezone=$tz"
fi

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

info "Region=$region tz=$tz execs=$execs cores=$cores exec_mem=$exec_mem driver_mem=$driver_mem"
info "Source=$load_source_type@$load_source_host:$load_source_port/$load_source_database user=$load_source_username pass=$(mask_secret "$load_source_password")"
info "Target=$load_target_type@$load_target_host:$load_target_port/$load_target_database user=$load_target_username pass=$(mask_secret "$load_target_password")"

job_args=(
  "--load_source_type=$load_source_type"
  "--load_source_host=$load_source_host"
  "--load_source_port=$load_source_port"
  "--load_source_database=$load_source_database"
  "--load_source_username=$load_source_username"
  "--load_source_password=$load_source_password"
  "--load_source_table_name=$load_source_table_name"
  "--load_source_primary_key=$load_source_primary_key"
  "--load_source_sql_query=$load_source_sql_query"

  "--load_target_type=$load_target_type"
  "--load_target_host=$load_target_host"
  "--load_target_port=$load_target_port"
  "--load_target_database=$load_target_database"
  "--load_target_username=$load_target_username"
  "--load_target_password=$load_target_password"
  "--load_target_table_name=$load_target_table_name"
  "--load_target_primary_key=$load_target_primary_key"
  "--load_target_sql_query=$load_target_sql_query"
  "--load_target_load_type=$load_target_load_type"
  "--load_target_hadoop_path=$load_target_hadoop_path"
  "--load_target_partition_id=$load_target_partition_id"

  "--load_time_zone=$tz"
  "--load_unix_host=$load_unix_host"
  "--load_user_email=$load_user_email"
  "--load_batch_id=$load_batch_id"
  "--load_unix_username=$load_unix_username"
  "--load_unix_password=$load_unix_password"

  "--time_zone=$tz"
  "--target_load_type=$target_load_type"
  "--hadoop_path=$hadoop_path"
  "--partition_id=$partition_id"
  "--table_mode=$table_mode"
  "--source_file_sql_query=$source_file_sql_query"
)

printf '[%s] [CMD ] ' "$(date '+%Y-%m-%d %H:%M:%S')"
printf '%q ' "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"; echo

exec "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"
