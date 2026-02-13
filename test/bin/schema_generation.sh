#!/bin/bash
set -euo pipefail

log(){ echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] ${*:2}"; }
info(){ log INFO "$@"; }
mask_secret(){ [[ -z "${1:-}" || "${1:-}" == "N/A" ]] && echo "${1:-}" || echo "******"; }

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
APP_RUN="${APP_RUN:-/app/run}"
APP_PY="${APP_PY_SCHEMA:-/app/jobs/schema_generation.py}"

info "=================================================="
info "Starting: $(basename "$0") | pid=$$ | host=$(hostname)"
info "=================================================="

schema_source_type=""; schema_source_host=""; schema_source_port=""
schema_source_database=""; schema_source_username=""; schema_source_password=""
schema_source_schema_mode=""; schema_source_table_name=""; schema_source_table_list_path=""

schema_target_type=""; schema_target_host=""; schema_target_port=""
schema_target_database=""; schema_target_username=""; schema_target_password=""
schema_target_output_file_name=""

schema_unix_host=""; schema_user_email=""; schema_batch_id=""
schema_unix_username=""; schema_unix_password=""

time_zone="N/A"; schema_mode=""; output_file_name=""
source_file_sql_query="N/A"; target_file_sql_query="N/A"; tab=""

for arg in "$@"; do
  case "$arg" in
    --tab=*) tab="${arg#*=}" ;;
    --schema_mode=*) schema_mode="${arg#*=}" ;;
    --output_file_name=*) output_file_name="${arg#*=}" ;;

    --schema_source_type=*) schema_source_type="${arg#*=}" ;;
    --schema_source_host=*) schema_source_host="${arg#*=}" ;;
    --schema_source_port=*) schema_source_port="${arg#*=}" ;;
    --schema_source_database=*) schema_source_database="${arg#*=}" ;;
    --schema_source_username=*) schema_source_username="${arg#*=}" ;;
    --schema_source_password=*) schema_source_password="${arg#*=}" ;;
    --schema_source_schema_mode=*) schema_source_schema_mode="${arg#*=}" ;;
    --schema_source_table_name=*) schema_source_table_name="${arg#*=}" ;;
    --schema_source_table_list_path=*) schema_source_table_list_path="${arg#*=}" ;;

    --schema_target_type=*) schema_target_type="${arg#*=}" ;;
    --schema_target_host=*) schema_target_host="${arg#*=}" ;;
    --schema_target_port=*) schema_target_port="${arg#*=}" ;;
    --schema_target_database=*) schema_target_database="${arg#*=}" ;;
    --schema_target_username=*) schema_target_username="${arg#*=}" ;;
    --schema_target_password=*) schema_target_password="${arg#*=}" ;;
    --schema_target_output_file_name=*) schema_target_output_file_name="${arg#*=}" ;;

    --schema_unix_host=*) schema_unix_host="${arg#*=}" ;;
    --schema_user_email=*) schema_user_email="${arg#*=}" ;;
    --schema_batch_id=*) schema_batch_id="${arg#*=}" ;;
    --schema_unix_username=*) schema_unix_username="${arg#*=}" ;;
    --schema_unix_password=*) schema_unix_password="${arg#*=}" ;;

    --time_zone=*) time_zone="${arg#*=}" ;;
    --source_file_sql_query=*) source_file_sql_query="${arg#*=}" ;;
    --target_file_sql_query=*) target_file_sql_query="${arg#*=}" ;;
  esac
done

# region
region="dev"
h="${schema_unix_host:-}"
shopt -s nocasematch
[[ "$h" == *"sit"* ]] && region="sit"
[[ "$h" == *"uat"* ]] && region="uat"
[[ "$h" == *"dev"* ]] && region="dev"
shopt -u nocasematch

case "$region" in
  dev) execs=1; cores=1; exec_mem="2g"; driver_mem="1g" ;;
  sit) execs=2; cores=2; exec_mem="4g"; driver_mem="2g" ;;
  uat) execs=3; cores=2; exec_mem="6g"; driver_mem="3g" ;;
esac

tz="${time_zone:-N/A}"
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

# output file selection (prefer explicit output_file_name)
final_output="${output_file_name:-$schema_target_output_file_name}"

info "Region=$region tz=$tz execs=$execs cores=$cores"
info "schema_mode=$schema_mode output=$final_output"

job_args=(
  "--tab=$tab"
  "--schema_mode=$schema_mode"
  "--output_file_name=$final_output"

  "--schema_source_type=$schema_source_type"
  "--schema_source_host=$schema_source_host"
  "--schema_source_port=$schema_source_port"
  "--schema_source_database=$schema_source_database"
  "--schema_source_username=$schema_source_username"
  "--schema_source_password=$schema_source_password"
  "--schema_source_schema_mode=$schema_source_schema_mode"
  "--schema_source_table_name=$schema_source_table_name"
  "--schema_source_table_list_path=$schema_source_table_list_path"

  "--schema_target_type=$schema_target_type"
  "--schema_target_host=$schema_target_host"
  "--schema_target_port=$schema_target_port"
  "--schema_target_database=$schema_target_database"
  "--schema_target_username=$schema_target_username"
  "--schema_target_password=$schema_target_password"
  "--schema_target_output_file_name=$schema_target_output_file_name"

  "--schema_unix_host=$schema_unix_host"
  "--schema_user_email=$schema_user_email"
  "--schema_batch_id=$schema_batch_id"
  "--schema_unix_username=$schema_unix_username"
  "--schema_unix_password=$schema_unix_password"

  "--time_zone=$tz"
  "--source_file_sql_query=$source_file_sql_query"
  "--target_file_sql_query=$target_file_sql_query"
)

printf '[%s] [CMD ] ' "$(date '+%Y-%m-%d %H:%M:%S')"
printf '%q ' "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"; echo
exec "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"
