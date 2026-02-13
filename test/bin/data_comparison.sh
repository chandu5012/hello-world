#!/bin/bash
set -euo pipefail

# -------- Logging --------
log(){ echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] ${*:2}"; }
info(){ log INFO "$@"; }
warn(){ log WARN "$@"; }

mask_secret() {
  local s="${1:-}"
  [[ -z "$s" || "$s" == "N/A" ]] && { echo "$s"; return; }
  echo "******"
}

# -------- Defaults --------
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
APP_RUN="${APP_RUN:-/app/run}"
APP_PY="${APP_PY_DATA_COMPARISON:-/app/jobs/data_comparison.py}"

info "=================================================="
info "Starting: $(basename "$0") | user=$(whoami) | host=$(hostname) | pid=$$"
info "=================================================="

# -------- Variables --------
comparison_type_select=""; table_mode=""; comparison_type=""
comparison_source_type=""; comparison_source_host=""; comparison_source_port=""
comparison_source_database=""; comparison_source_username=""; comparison_source_password=""
comparison_source_table_name=""; comparison_source_primary_key=""; comparison_source_sql_query=""

comparison_target_type=""; comparison_target_host=""; comparison_target_port=""
comparison_target_database=""; comparison_target_username=""; comparison_target_password=""
comparison_target_table_name=""; comparison_target_primary_key=""; comparison_target_sql_query=""

comparison_source_file_type=""; comparison_source_location_type=""; comparison_source_unix_path=""
comparison_source_delimiter=""; comparison_source_file_sql_query=""; comparison_source_key_columns=""
comparison_source_file_upload=""; comparison_source_hadoop_path=""

comparison_target_file_type=""; comparison_target_location_type=""; comparison_target_unix_path=""
comparison_target_hadoop_path=""; comparison_target_delimiter=""; comparison_target_file_sql_query=""
comparison_target_key_columns=""

source_excel_file=""; target_excel_file=""
source_sql_query=""; source_table_name=""; source_key_columns=""
target_sql_query=""; target_table_name=""; target_key_columns=""
row_index=""

comparison_unix_host=""; comparison_unix_username=""; comparison_unix_password=""
comparison_user_email=""; comparison_batch_id=""
comparison_time_zone="N/A"; comparison_time_zone_manual=""; time_zone="N/A"
key_columns=""; source_file_sql_query="N/A"; target_file_sql_query="N/A"
target_hadoop_path=""; target_location_type=""; target_file_sql_query2=""
source_location_type=""; target_unix_path=""; source_file_sql_query2=""; target_file_sql_query3=""

# -------- Parse args (FULL) --------
for arg in "$@"; do
  case "$arg" in
    --comparison-type-select=*) comparison_type_select="${arg#*=}" ;;
    --table_mode=*) table_mode="${arg#*=}" ;;
    --comparison_type=*) comparison_type="${arg#*=}" ;;

    --comparison_source_type=*) comparison_source_type="${arg#*=}" ;;
    --comparison_source_host=*) comparison_source_host="${arg#*=}" ;;
    --comparison_source_port=*) comparison_source_port="${arg#*=}" ;;
    --comparison_source_database=*) comparison_source_database="${arg#*=}" ;;
    --comparison_source_username=*) comparison_source_username="${arg#*=}" ;;
    --comparison_source_password=*) comparison_source_password="${arg#*=}" ;;
    --comparison_source_table_name=*) comparison_source_table_name="${arg#*=}" ;;
    --comparison_source_primary_key=*) comparison_source_primary_key="${arg#*=}" ;;
    --comparison_source_sql_query=*) comparison_source_sql_query="${arg#*=}" ;;

    --comparison_target_type=*) comparison_target_type="${arg#*=}" ;;
    --comparison_target_host=*) comparison_target_host="${arg#*=}" ;;
    --comparison_target_port=*) comparison_target_port="${arg#*=}" ;;
    --comparison_target_database=*) comparison_target_database="${arg#*=}" ;;
    --comparison_target_username=*) comparison_target_username="${arg#*=}" ;;
    --comparison_target_password=*) comparison_target_password="${arg#*=}" ;;
    --comparison_target_table_name=*) comparison_target_table_name="${arg#*=}" ;;
    --comparison_target_primary_key=*) comparison_target_primary_key="${arg#*=}" ;;
    --comparison_target_sql_query=*) comparison_target_sql_query="${arg#*=}" ;;

    --comparison_source_file_type=*) comparison_source_file_type="${arg#*=}" ;;
    --comparison_source_location_type=*) comparison_source_location_type="${arg#*=}" ;;
    --comparison_source_unix_path=*) comparison_source_unix_path="${arg#*=}" ;;
    --comparison_source_hadoop_path=*) comparison_source_hadoop_path="${arg#*=}" ;;
    --comparison_source_file_upload=*) comparison_source_file_upload="${arg#*=}" ;;
    --comparison_source_delimiter=*) comparison_source_delimiter="${arg#*=}" ;;
    --comparison_source_file_sql_query=*) comparison_source_file_sql_query="${arg#*=}" ;;
    --comparison_source_key_columns=*) comparison_source_key_columns="${arg#*=}" ;;

    --comparison_target_file_type=*) comparison_target_file_type="${arg#*=}" ;;
    --comparison_target_location_type=*) comparison_target_location_type="${arg#*=}" ;;
    --comparison_target_unix_path=*) comparison_target_unix_path="${arg#*=}" ;;
    --comparison_target_hadoop_path=*) comparison_target_hadoop_path="${arg#*=}" ;;
    --comparison_target_delimiter=*) comparison_target_delimiter="${arg#*=}" ;;
    --comparison_target_file_sql_query=*) comparison_target_file_sql_query="${arg#*=}" ;;
    --comparison_target_key_columns=*) comparison_target_key_columns="${arg#*=}" ;;

    --source_excel_file=*) source_excel_file="${arg#*=}" ;;
    --target_excel_file=*) target_excel_file="${arg#*=}" ;;

    --source_sql_query=*) source_sql_query="${arg#*=}" ;;
    --source_table_name=*) source_table_name="${arg#*=}" ;;
    --source_key_columns=*) source_key_columns="${arg#*=}" ;;
    --target_sql_query=*) target_sql_query="${arg#*=}" ;;
    --target_table_name=*) target_table_name="${arg#*=}" ;;
    --target_key_columns=*) target_key_columns="${arg#*=}" ;;
    --row_index=*) row_index="${arg#*=}" ;;

    --comparison_unix_host=*) comparison_unix_host="${arg#*=}" ;;
    --comparison_unix_username=*) comparison_unix_username="${arg#*=}" ;;
    --comparison_unix_password=*) comparison_unix_password="${arg#*=}" ;;
    --comparison_user_email=*) comparison_user_email="${arg#*=}" ;;
    --comparison_batch_id=*) comparison_batch_id="${arg#*=}" ;;
    --comparison_time_zone=*) comparison_time_zone="${arg#*=}" ;;
    --comparison_time_zone_manual=*) comparison_time_zone_manual="${arg#*=}" ;;

    --time_zone=*) time_zone="${arg#*=}" ;;
    --key_columns=*) key_columns="${arg#*=}" ;;
    --source_file_sql_query=*) source_file_sql_query="${arg#*=}" ;;
    --target_file_sql_query=*) target_file_sql_query="${arg#*=}" ;;
    --target_hadoop_path=*) target_hadoop_path="${arg#*=}" ;;
    --target_location_type=*) target_location_type="${arg#*=}" ;;
    --target_file_sql_query=*) target_file_sql_query2="${arg#*=}" ;;
    --source_location_type=*) source_location_type="${arg#*=}" ;;
    --target_unix_path=*) target_unix_path="${arg#*=}" ;;
    --source_file_sql_query=*) source_file_sql_query2="${arg#*=}" ;;
    --target_file_sql_query=*) target_file_sql_query3="${arg#*=}" ;;
  esac
done

# -------- Region detection from comparison_unix_host --------
region="dev"
h="${comparison_unix_host:-}"
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

# -------- Timezone (priority: comparison_time_zone > time_zone) --------
tz="${comparison_time_zone:-N/A}"
[[ -z "$tz" || "$tz" == "N/A" ]] && tz="${time_zone:-N/A}"

driver_java_opts=""; exec_java_opts=""
if [[ -n "$tz" && "$tz" != "N/A" ]]; then
  driver_java_opts="-Duser.timezone=$tz"
  exec_java_opts="-Duser.timezone=$tz"
fi

# -------- Build spark cmd --------
spark_cmd=(
  "$SPARK_HOME/bin/spark-submit"
  --master yarn
  --deploy-mode cluster
  --num-executors "$execs"
  --executor-cores "$cores"
  --executor-memory "$exec_mem"
  --driver-memory "$driver_mem"
)

if [[ -n "$driver_java_opts" ]]; then
  spark_cmd+=( --conf "spark.driver.extraJavaOptions=$driver_java_opts" )
  spark_cmd+=( --conf "spark.executor.extraJavaOptions=$exec_java_opts" )
fi

# -------- CDP/DPC S3 conf if profile exists --------
if [[ -f "$APP_RUN/cdp_profile" ]]; then
  # shellcheck disable=SC1090
  source "$APP_RUN/cdp_profile"
  info "cdp_profile found at $APP_RUN/cdp_profile - adding S3A confs"
  spark_cmd+=(
    --conf "spark.hadoop.fs.s3a.endpoint=${DPC_S3_ENDPOINT}"
    --conf "spark.hadoop.fs.s3a.security.credential.provider.path=${DPC_S3_JCEKS}"
    --conf "spark.hadoop.fs.s3a.path.style.access=true"
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=true"
    --conf "spark.sql.catalogImplementation=hive"
  )
else
  info "cdp_profile not found - skipping S3A confs"
fi

# -------- Log summary (masked) --------
info "Args Summary:"
info " comparison_type_select=$comparison_type_select | comparison_type=$comparison_type | table_mode=$table_mode"
info " source_db=$comparison_source_type@$comparison_source_host:$comparison_source_port/$comparison_source_database user=$comparison_source_username pass=$(mask_secret "$comparison_source_password")"
info " target_db=$comparison_target_type@$comparison_target_host:$comparison_target_port/$comparison_target_database user=$comparison_target_username pass=$(mask_secret "$comparison_target_password")"
info " unix_host=$comparison_unix_host | region=$region | tz=$tz"
info " spark: execs=$execs cores=$cores exec_mem=$exec_mem driver_mem=$driver_mem"

# -------- Build job args (pass ALL parsed values) --------
job_args=(
  "--comparison-type-select=$comparison_type_select"
  "--table_mode=$table_mode"
  "--comparison_type=$comparison_type"

  "--comparison_source_type=$comparison_source_type"
  "--comparison_source_host=$comparison_source_host"
  "--comparison_source_port=$comparison_source_port"
  "--comparison_source_database=$comparison_source_database"
  "--comparison_source_username=$comparison_source_username"
  "--comparison_source_password=$comparison_source_password"
  "--comparison_source_table_name=$comparison_source_table_name"
  "--comparison_source_primary_key=$comparison_source_primary_key"
  "--comparison_source_sql_query=$comparison_source_sql_query"

  "--comparison_target_type=$comparison_target_type"
  "--comparison_target_host=$comparison_target_host"
  "--comparison_target_port=$comparison_target_port"
  "--comparison_target_database=$comparison_target_database"
  "--comparison_target_username=$comparison_target_username"
  "--comparison_target_password=$comparison_target_password"
  "--comparison_target_table_name=$comparison_target_table_name"
  "--comparison_target_primary_key=$comparison_target_primary_key"
  "--comparison_target_sql_query=$comparison_target_sql_query"

  "--comparison_source_file_type=$comparison_source_file_type"
  "--comparison_source_location_type=$comparison_source_location_type"
  "--comparison_source_unix_path=$comparison_source_unix_path"
  "--comparison_source_hadoop_path=$comparison_source_hadoop_path"
  "--comparison_source_file_upload=$comparison_source_file_upload"
  "--comparison_source_delimiter=$comparison_source_delimiter"
  "--comparison_source_file_sql_query=$comparison_source_file_sql_query"
  "--comparison_source_key_columns=$comparison_source_key_columns"

  "--comparison_target_file_type=$comparison_target_file_type"
  "--comparison_target_location_type=$comparison_target_location_type"
  "--comparison_target_unix_path=$comparison_target_unix_path"
  "--comparison_target_hadoop_path=$comparison_target_hadoop_path"
  "--comparison_target_delimiter=$comparison_target_delimiter"
  "--comparison_target_file_sql_query=$comparison_target_file_sql_query"
  "--comparison_target_key_columns=$comparison_target_key_columns"

  "--source_excel_file=$source_excel_file"
  "--target_excel_file=$target_excel_file"
  "--source_sql_query=$source_sql_query"
  "--source_table_name=$source_table_name"
  "--source_key_columns=$source_key_columns"
  "--target_sql_query=$target_sql_query"
  "--target_table_name=$target_table_name"
  "--target_key_columns=$target_key_columns"
  "--row_index=$row_index"

  "--comparison_unix_host=$comparison_unix_host"
  "--comparison_unix_username=$comparison_unix_username"
  "--comparison_unix_password=$comparison_unix_password"
  "--comparison_user_email=$comparison_user_email"
  "--comparison_batch_id=$comparison_batch_id"
  "--comparison_time_zone=$tz"
  "--comparison_time_zone_manual=$comparison_time_zone_manual"

  "--time_zone=$tz"
  "--key_columns=$key_columns"
  "--source_file_sql_query=$source_file_sql_query"
  "--target_file_sql_query=$target_file_sql_query"
  "--target_hadoop_path=$target_hadoop_path"
  "--target_location_type=$target_location_type"
  "--target_file_sql_query2=$target_file_sql_query2"
  "--source_location_type=$source_location_type"
  "--target_unix_path=$target_unix_path"
  "--source_file_sql_query2=$source_file_sql_query2"
  "--target_file_sql_query3=$target_file_sql_query3"
)

info "Submitting spark job..."
printf '[%s] [CMD ] ' "$(date '+%Y-%m-%d %H:%M:%S')"
printf '%q ' "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"; echo

exec "${spark_cmd[@]}" "$APP_PY" "${job_args[@]}"
