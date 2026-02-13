SPARK_CONF=()

if [[ -n "$TIME_ZONE" && "$TIME_ZONE" != "N/A" ]]; then
  SPARK_CONF+=(
    --conf "spark.driver.extraJavaOptions=-Duser.timezone=${TIME_ZONE}"
    --conf "spark.executor.extraJavaOptions=-Duser.timezone=${TIME_ZONE}"
  )
  echo "[INFO] Timezone enabled: $TIME_ZONE"
else
  echo "[INFO] Timezone not provided (N/A). Skipping timezone Spark configs."
fi
