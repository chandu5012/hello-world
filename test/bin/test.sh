# -------------------------------
# Validate SPARK_HOME & detect cluster
# -------------------------------
if [[ -z "${SPARK_HOME:-}" ]]; then
  log_error "SPARK_HOME is NOT set. Please login to cluster properly."
  exit 1
fi

case "$SPARK_HOME" in
  /opt/mapr/*)
    CLUSTER_TYPE="mapr"
    ;;
  /opt/cloudera/*)
    CLUSTER_TYPE="cloudera"
    ;;
  *)
    CLUSTER_TYPE="unknown"
    ;;
esac

log_info "SPARK_HOME detected as: $SPARK_HOME"
log_info "Cluster type detected: $CLUSTER_TYPE"


case "$CLUSTER_TYPE" in
  mapr)
    log_info "Applying MapR-specific Spark configurations"

    SPARK_CONF+=(
      --conf "spark.sql.catalogImplementation=in-memory"
      --conf "spark.yarn.maxAppAttempts=1"
    )
    ;;

  cloudera)
    log_info "Applying Cloudera-specific Spark configurations"

    SPARK_CONF+=(
      --conf "spark.sql.catalogImplementation=hive"
      --conf "spark.sql.hive.metastore.jars=builtin"
      --conf "spark.yarn.maxAppAttempts=1"
    )
    ;;

  *)
    log_warn "Unknown SPARK_HOME pattern. No cluster-specific configs applied."
    ;;
esac

if [[ "$CLUSTER_TYPE" == "cloudera" && -f "$APP_RUN/cdp_profile" ]]; then
  log_info "Applying CDP/DPC S3 configurations"

  source "$APP_RUN/cdp_profile"

  SPARK_CONF+=(
    --conf "spark.hadoop.fs.s3a.endpoint=$DPC_S3_ENDPOINT"
    --conf "spark.hadoop.fs.s3a.security.credential.provider.path=$DPC_S3_JCEKS"
    --conf "spark.hadoop.fs.s3a.path.style.access=true"
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=true"
    --conf "spark.sql.catalogImplementation=hive"
  )
fi

