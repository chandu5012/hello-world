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

# cleanup input (Windows CRLF safe)
time_zone="$(echo "$time_zone" | tr -d '\r' | xargs)"

# always apply log4j
driver_opts="-Dlog4j.configuration=file:$LOG4J_CONFIG"
exec_opts="-Dlog4j.configuration=file:$LOG4J_CONFIG"

# apply timezone ONLY if user provided it
if [[ -n "$time_zone" && "$time_zone" != "N/A" ]]; then
  driver_opts="-Duser.timezone=$time_zone $driver_opts"
  exec_opts="-Duser.timezone=$time_zone $exec_opts"

  SPARK_CONF+=(
    --conf "spark.sql.session.timeZone=$time_zone"
  )
fi

# always add extraJavaOptions (single occurrence)
SPARK_CONF+=(
  --conf "spark.driver.extraJavaOptions=$driver_opts"
  --conf "spark.executor.extraJavaOptions=$exec_opts"
)

echo "Effective timezone: ${time_zone:-N/A}"

