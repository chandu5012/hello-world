#!/bin/bash
set -euo pipefail

log(){ echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] ${*:2}"; }
info(){ log INFO "$@"; }
mask_secret(){ [[ -z "${1:-}" || "${1:-}" == "N/A" ]] && echo "${1:-}" || echo "******"; }

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
APP_RUN="${APP_RUN:-/app/run}"
APP_PY="${APP_PY_FILEDOWNLOAD:-/app/jobs/file_download.py}"

info "=================================================="
info "Starting: $(basename "$0") | pid=$$ | host=$(hostname)"
info "=================================================="

# Vars
filedownload_source_file_type=""; filedownload_source_delimiter=""; filedownload_source_location_type=""
filedownload_source_unix_path=""; filedownload_source_key_columns=""; filedownload_source_sql_query=""
filedownload_source_type=""; filedownload_source_host=""; filedownload_source_port=""
filedownload_source_database=""; filedownload_source_username=""; filedownload_source_password=""
filedownload_source_table_name=""; filedownload_source_primary_key=""

filedownload_target_file_type=""; filedownload_target_delimiter=""
filedownload_target_file_name=""; filedownload_target_number_of_rows=""

filedownload_unix_host=""; filedownload_user_email=""; filedownload_batch_id=""
filedownload_unix_username=""; filedownload_unix_password=""

source_type=""; target_file_name=""; number_of_rows=""; target_file_type=""; target_delimiter=""
time_zone="N/A"; source_file_sql_query="N/A"; target_file_sql_query="N/A"

for arg in "$@"; do
  case "$arg" in
    --filedownload_source_file_type=*) filedownload_source_file_type="${arg#*=}" ;;
    --filedownload_source_delimiter=*) filedownload_source_delimiter="${arg#*=}" ;;
    --filedownload_source_location_type=*) filedownload_source_location_type="${arg#*=}" ;;
    --filedownload_source_unix_path=*) filedownload_source_unix_path="${arg#*=}" ;;
    --filedownload_source_key_columns=*) filedownload_source_key_columns="${arg#*=}" ;;
    --filedownload_source_sql_query=*) filedownload_source_sql_query="${arg#*=}" ;;

    --filedownload_source_type=*) filedownload_source_type="${arg#*=}" ;;
    --filedownload_source_host=*) filedownload_source_host="${arg#*=}" ;;
    --filedownload_source_port=*) filedownload_source_port="${arg#*=}" ;;
    --filedownload_source_database=*) filedownload_source_database="${arg#*=}" ;;
    --filedownload_source_username=*) filedownload_source_username="${arg#*=}" ;;
    --filedownload_source_password=*) filedownload_source_password="${arg#*=}" ;;
    --filedownload_source_table_name=*) filedownload_source_table_name="${arg#*=}" ;;
    --filedownload_source_primary_key=*) filedownload_source_primary_key="${arg#*=}" ;;

    --filedownload_target_file_type=*) filedownload_target_file_type="${arg#*=}" ;;
    --filedownload_target_delimiter=*) filedownload_target_delimiter="${arg#*=}" ;;
    --filedownload_target_file_name=*) filedownload_target_file_name="${arg#*=}" ;;
    --filedownload_target_number_of_rows=*) filedownload_target_number_of_rows="${arg#*=}" ;;

    --filedownload_unix_host=*) filedownload_unix_host="${arg#*=}" ;;
    --filedownload_user_email=*) filedownload_user_email="${arg#*=}" ;;
    --filedownload_batch_id=*) filedownload_batch_id="${arg#*=}" ;;
    --filedownload_unix_username=*) filedownload_unix_username="${arg#*=}" ;;
    --filedownload_unix_password=*) filedownload_unix_password="${arg#*=}" ;;

    --source_type=*) source_type="${arg#*=}" ;;
    --target_file_name=*) target_file_name="${arg#*=}" ;;
    --number_of_rows=*) number_of_rows="${arg#*=}" ;;
    --target_file_type=*) target_file_type="${arg#*=}" ;;
    --target_delimiter=*) target_delimiter="${arg#*=}" ;;
    --time_zone=*) time_zone="${arg#*=}" ;;
    --source_file_sql_query=*) source_file_sql_query="${arg#*=}" ;;
    --target_file_sql_query=*) target_file_sql_query="${arg#*=}" ;;
  esac
done

# Region (small job, still allow region logic)
region="dev"
h="${filedownload_unix_host:-}"
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

info "Region=$region tz=$tz execs=$execs cores=$cores"
info "source_type=$source_type | target_file=$target_file_name rows=$number_of_rows"

job_args=(
  "--filedownload_source_file_type=$filedownload_source_file_type"
  "--filedownload_source_delimiter=$filedownload_source_delimiter"
  "--filedownload_source_location_type=$filedownload_source_location_type"
  "--filedownload_source_unix_path=$filedownload_source_unix_path"
  "--filedownload_source_key_columns=$filedownload_source_key_columns"
  "--filedownload_source_sql_query=$filedownload_source_sql_query"

  "--filedownload_source_type=$filedownload_source_type"
  "--filedownload_source_host=$filedownload_source_host"
  "--filedownload_source_port=$filedownload_source_port"
  "--filedownload_source_database=$filedownload_source_database"
  "--filedownload_source_username=$filedownload_source_username"
  "--filedownload_source_password=$filedownload_source_password"
  "--filedownload_source_table_name=$filedownload_source_table_name"
  "--filedownload_source_primary_key=$filedownload_source_primary_key"

  "--filedownload_target_file_type=$filedownload_target_file_type"
  "--filedownload_target_delimiter=$filedownload_target_delimiter"
  "--filedownload_target_file_name=$filedownload_target_file_name"
  "--filedownload_target_number_of_rows=$filedownload_target_number_of_rows"

  "--filedownload_unix_host=$filedownload_unix_host"
  "--filedownload_user_email=$filedownload_user_email"
  "--filedownload_batch_id=$filedownload_batch_id"
  "--filedownload_unix_username=$filedownload_unix_username"
  "--filedownload_unix_password=$filedownload_unix_password"

  "--source_type=$source_type"
  "--target_file_name=$target_file_name"
  "--number_of_rows=$number_of_rows"
  "--target_file_type=$target_file_type"
  "--target_delimiter=$target_delimiter"
  "--time_zone=$tz"
  "--source_file_sql_query=$source_file_sql_query"
  "--target_file_sql_query=$target_file_sql_query"
)

printf '[%s] [CMD ] ' "$(date '+%Y-%m-%d %H:%M:%S')"
printf '%q ' "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"; echo
exec "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"
