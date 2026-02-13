#!/bin/bash
set -euo pipefail

log(){ echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] ${*:2}"; }
info(){ log INFO "$@"; }
mask_secret(){ [[ -z "${1:-}" || "${1:-}" == "N/A" ]] && echo "${1:-}" || echo "******"; }

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
APP_RUN="${APP_RUN:-/app/run}"
APP_PY="${APP_PY_FILELOAD:-/app/jobs/file_to_db_load.py}"

info "=================================================="
info "Starting: $(basename "$0") | pid=$$ | host=$(hostname)"
info "=================================================="

# Vars
fileload_source_file_type=""; fileload_source_location_type=""; fileload_source_unix_path=""
fileload_source_delimiter=""; fileload_source_file_sql_query=""; fileload_source_key_columns=""

fileload_target_type=""; fileload_target_host=""; fileload_target_port=""
fileload_target_database=""; fileload_target_username=""; fileload_target_password=""
fileload_target_table_name=""; fileload_target_primary_key=""; fileload_target_sql_query=""
fileload_target_load_type=""; fileload_target_hadoop_path=""; fileload_target_partition_id=""

fileload_time_zone="N/A"; time_zone="N/A"
fileload_unix_host=""; fileload_user_email=""; fileload_batch_id=""
fileload_unix_username=""; fileload_unix_password=""

# duplicates provided by UI
source_file_type=""; source_location_type=""; source_unix_path=""
source_delimiter=""; source_file_sql_query=""; source_key_columns=""
target_load_type=""; hadoop_path=""

for arg in "$@"; do
  case "$arg" in
    --fileload_source_file_type=*) fileload_source_file_type="${arg#*=}" ;;
    --fileload_source_location_type=*) fileload_source_location_type="${arg#*=}" ;;
    --fileload_source_unix_path=*) fileload_source_unix_path="${arg#*=}" ;;
    --fileload_source_delimiter=*) fileload_source_delimiter="${arg#*=}" ;;
    --fileload_source_file_sql_query=*) fileload_source_file_sql_query="${arg#*=}" ;;
    --fileload_source_key_columns=*) fileload_source_key_columns="${arg#*=}" ;;

    --fileload_target_type=*) fileload_target_type="${arg#*=}" ;;
    --fileload_target_host=*) fileload_target_host="${arg#*=}" ;;
    --fileload_target_port=*) fileload_target_port="${arg#*=}" ;;
    --fileload_target_database=*) fileload_target_database="${arg#*=}" ;;
    --fileload_target_username=*) fileload_target_username="${arg#*=}" ;;
    --fileload_target_password=*) fileload_target_password="${arg#*=}" ;;
    --fileload_target_table_name=*) fileload_target_table_name="${arg#*=}" ;;
    --fileload_target_primary_key=*) fileload_target_primary_key="${arg#*=}" ;;
    --fileload_target_sql_query=*) fileload_target_sql_query="${arg#*=}" ;;
    --fileload_target_load_type=*) fileload_target_load_type="${arg#*=}" ;;
    --fileload_target_hadoop_path=*) fileload_target_hadoop_path="${arg#*=}" ;;
    --fileload_target_partition_id=*) fileload_target_partition_id="${arg#*=}" ;;

    --fileload_time_zone=*) fileload_time_zone="${arg#*=}" ;;
    --fileload_unix_host=*) fileload_unix_host="${arg#*=}" ;;
    --fileload_user_email=*) fileload_user_email="${arg#*=}" ;;
    --fileload_batch_id=*) fileload_batch_id="${arg#*=}" ;;
    --fileload_unix_username=*) fileload_unix_username="${arg#*=}" ;;
    --fileload_unix_password=*) fileload_unix_password="${arg#*=}" ;;

    --time_zone=*) time_zone="${arg#*=}" ;;
    --source_file_type=*) source_file_type="${arg#*=}" ;;
    --source_location_type=*) source_location_type="${arg#*=}" ;;
    --source_unix_path=*) source_unix_path="${arg#*=}" ;;
    --source_delimiter=*) source_delimiter="${arg#*=}" ;;
    --source_file_sql_query=*) source_file_sql_query="${arg#*=}" ;;
    --source_key_columns=*) source_key_columns="${arg#*=}" ;;
    --target_load_type=*) target_load_type="${arg#*=}" ;;
    --hadoop_path=*) hadoop_path="${arg#*=}" ;;
  esac
done

region="dev"
h="${fileload_unix_host:-}"
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

tz="${fileload_time_zone:-N/A}"
[[ -z "$tz" || "$tz" == "N/A" ]] && tz="${time_zone:-N/A}"

driver_java_opts=""
[[ -n "$tz" && "$tz" != "N/A" ]] && driver_java_opts="-Duser.timezone=$tz"

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

info "Region=$region tz=$tz execs=$execs cores=$cores"
info "Target=$fileload_target_type@$fileload_target_host:$fileload_target_port/$fileload_target_database user=$fileload_target_username pass=$(mask_secret "$fileload_target_password")"

job_args=(
  "--fileload_source_file_type=$fileload_source_file_type"
  "--fileload_source_location_type=$fileload_source_location_type"
  "--fileload_source_unix_path=$fileload_source_unix_path"
  "--fileload_source_delimiter=$fileload_source_delimiter"
  "--fileload_source_file_sql_query=$fileload_source_file_sql_query"
  "--fileload_source_key_columns=$fileload_source_key_columns"

  "--fileload_target_type=$fileload_target_type"
  "--fileload_target_host=$fileload_target_host"
  "--fileload_target_port=$fileload_target_port"
  "--fileload_target_database=$fileload_target_database"
  "--fileload_target_username=$fileload_target_username"
  "--fileload_target_password=$fileload_target_password"
  "--fileload_target_table_name=$fileload_target_table_name"
  "--fileload_target_primary_key=$fileload_target_primary_key"
  "--fileload_target_sql_query=$fileload_target_sql_query"
  "--fileload_target_load_type=$fileload_target_load_type"
  "--fileload_target_hadoop_path=$fileload_target_hadoop_path"
  "--fileload_target_partition_id=$fileload_target_partition_id"

  "--fileload_time_zone=$tz"
  "--fileload_unix_host=$fileload_unix_host"
  "--fileload_user_email=$fileload_user_email"
  "--fileload_batch_id=$fileload_batch_id"
  "--fileload_unix_username=$fileload_unix_username"
  "--fileload_unix_password=$fileload_unix_password"

  "--time_zone=$tz"
  "--source_file_type=$source_file_type"
  "--source_location_type=$source_location_type"
  "--source_unix_path=$source_unix_path"
  "--source_delimiter=$source_delimiter"
  "--source_file_sql_query=$source_file_sql_query"
  "--source_key_columns=$source_key_columns"
  "--target_load_type=$target_load_type"
  "--hadoop_path=$hadoop_path"
)

printf '[%s] [CMD ] ' "$(date '+%Y-%m-%d %H:%M:%S')"
printf '%q ' "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"; echo
exec "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"
