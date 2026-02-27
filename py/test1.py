# -*- coding: utf-8 -*-
from __future__ import print_function

import sys
import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
#                CONSTANTS (NO ARGS)
# ============================================================

APP_NAME = "php_payment_history_load"

MONTHS_BACK = 85
HISTORY_LEN = 84  # idx 0..83

# -------- Switches (TURN ON/OFF FILTERS) ----------
USE_AUD_ID_FILTER = False
USE_ACDV_CONTROL_FILTER = False

# -------- Job-1 (AUD) constants ----------
SRC_AUD_TABLE = "HIVE.dsas_fnish_sanitized.eoscar_aud_hist"
AUD_ID_LIST = ["101343866", "107047397", "107403444", "107669735"]

TGT_AUD_TABLE = "HIVE.my_db.php_aud_output_ext"
TGT_AUD_LOCATION = "/apps/hive/external/my_db/php_aud_output_ext"

# -------- Job-2 (ACDV) constants ----------
SRC_ACDV_TABLE = "HIVE.dsas_fnish_sanitized.eoscar_acdvArchive_hist"
ACDV_CONTROL_LIST = [
    "3978214565004",
    "99999097001872017",
    "396894994013003",
    "0789145041001"
]

TGT_ACDV_TABLE = "HIVE.my_db.php_acdv_output_ext"
TGT_ACDV_LOCATION = "/apps/hive/external/my_db/php_acdv_output_ext"

LOG_LEVEL = "INFO"
SAMPLE_SHOW = 0  # set 5 for debugging samples


# ============================================================
#                      LOGGING
# ============================================================

def setup_logging(level):
    logger = logging.getLogger("php_transform")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    handler.setFormatter(fmt)

    if not logger.handlers:
        logger.addHandler(handler)
    return logger


def log_df_info(logger, df, label, do_count=True, show_n=0):
    logger.info("========== %s ==========", label)
    df.printSchema()
    if do_count:
        c = df.count()
        logger.info("Row count: %s", c)
    if show_n and show_n > 0:
        df.show(show_n, truncate=False)


# ============================================================
#                HIVE TABLE CREATE HELPERS
# ============================================================

def ensure_external_partitioned_table(spark, table_name, location, logger, id_col_name):
    ddl = """
    CREATE EXTERNAL TABLE IF NOT EXISTS {tbl} (
        AccountNumber BIGINT,
        {id_col}      STRING,
        EventDate     DATE,
        PHPDate       DATE,
        PHPValue      STRING
    )
    PARTITIONED BY (php_month STRING)
    STORED AS PARQUET
    LOCATION '{loc}'
    """.format(tbl=table_name, loc=location, id_col=id_col_name)

    logger.info("Ensuring external partitioned table exists: %s", table_name)
    spark.sql(ddl)

    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("SET hive.exec.max.dynamic.partitions=5000")
    spark.sql("SET hive.exec.max.dynamic.partitions.pernode=5000")


def write_partitioned(logger, spark, df, target_table):
    logger.info("Writing to target table (append): %s", target_table)
    (df.write
       .mode("append")
       .format("hive")
       .insertInto(target_table))

    try:
        spark.sql("MSCK REPAIR TABLE {t}".format(t=target_table))
    except Exception as e:
        logger.warning("MSCK REPAIR TABLE failed (can ignore): %s", str(e))


# ============================================================
#                     TRANSFORM LOGIC
# ============================================================

def build_numbers_df(spark):
    return spark.range(0, HISTORY_LEN).select(F.col("id").cast("int").alias("idx"))


def build_common_columns(df, date_col, hist_col, acct_col, id_col, id_alias):
    event_date = F.to_date(F.col(date_col))
    php_date = F.add_months(F.trunc(event_date, "MM"), -(F.col("idx") + F.lit(1)))
    php_value = F.substring(F.col(hist_col), (F.col("idx") + F.lit(1)), F.lit(1))

    out = (df
           .withColumn("EventDate", event_date)
           .withColumn("PHPDate", php_date)
           .withColumn("PHPValue", php_value)
           .withColumn("AccountNumber", F.trim(F.col(acct_col)).cast("bigint"))
           .withColumn(id_alias, F.col(id_col).cast("string"))
           )
    return out


def apply_common_filters(df, base_date_col):
    df = df.where(F.col(base_date_col).isNotNull())
    df = df.where(F.to_date(F.col(base_date_col)) > F.add_months(F.current_date(), -int(MONTHS_BACK)))

    df = df.where(~F.coalesce(F.col("PHPValue"), F.lit("")).isin(["", "-"]))
    df = df.where(F.col("PHPDate") > F.add_months(F.current_date(), -int(MONTHS_BACK)))

    return df


def run_aud_job(spark, logger):
    logger.info("========== START AUD JOB ==========")

    ensure_external_partitioned_table(spark, TGT_AUD_TABLE, TGT_AUD_LOCATION, logger, "AUD_ID")

    numbers_df = build_numbers_df(spark)
    log_df_info(logger, numbers_df, "AUD - cteNumbers", do_count=True, show_n=SAMPLE_SHOW)

    src = spark.table(SRC_AUD_TABLE)

    base = src

    # Optional filter: AUD_ID list
    if USE_AUD_ID_FILTER:
        logger.info("AUD - Applying AUD_ID_LIST filter: %s", AUD_ID_LIST)
        base = base.where(F.col("aud_id").cast("string").isin(AUD_ID_LIST))
    else:
        logger.info("AUD - Skipping AUD_ID_LIST filter (USE_AUD_ID_FILTER=False)")

    # Base filters (AUD)
    base = (base
            .where(F.col("date_created").isNotNull())
            .where(F.col("date_opened").isNotNull())
            .where(F.trim(F.col("acct_num")).rlike("^[0-9]+$"))
            .where(F.col("seven_year_payment_history").isNotNull())
            .where(F.length(F.trim(F.col("seven_year_payment_history"))) > 0)
            )

    # months-back filter
    base = base.where(F.to_date(F.col("date_created")) > F.add_months(F.current_date(), -int(MONTHS_BACK)))

    log_df_info(logger,
                base.select("acct_num", "aud_id", "date_created", "seven_year_payment_history"),
                "AUD - Source after base filters",
                do_count=True, show_n=SAMPLE_SHOW)

    joined = base.crossJoin(numbers_df)

    tr = build_common_columns(
        joined,
        date_col="date_created",
        hist_col="seven_year_payment_history",
        acct_col="acct_num",
        id_col="aud_id",
        id_alias="AUD_ID"
    )

    tr = apply_common_filters(tr, base_date_col="date_created")

    log_df_info(logger,
                tr.select("AccountNumber", "AUD_ID", "EventDate", "PHPDate", "PHPValue"),
                "AUD - After explode + filters",
                do_count=True, show_n=SAMPLE_SHOW)

    w = Window.partitionBy(F.col("acct_num"), F.col("PHPDate")) \
              .orderBy(F.col("EventDate").desc(), F.col("AUD_ID").desc())

    tr = tr.withColumn("RowNum", F.row_number().over(w))

    final_df = (tr
                .where(F.col("RowNum") == 1)
                .select("AccountNumber", "AUD_ID", "EventDate", "PHPDate", "PHPValue")
                .orderBy(F.col("PHPValue").desc(), F.col("PHPDate").desc())
                )

    log_df_info(logger, final_df, "AUD - Final (RowNum=1)", do_count=True, show_n=SAMPLE_SHOW)

    out = (final_df
           .withColumn("php_month", F.date_format(F.col("PHPDate"), "yyyy-MM"))
           .select("AccountNumber", "AUD_ID", "EventDate", "PHPDate", "PHPValue", "php_month")
           )

    log_df_info(logger, out, "AUD - Final with partition", do_count=True, show_n=SAMPLE_SHOW)

    write_partitioned(logger, spark, out, TGT_AUD_TABLE)

    logger.info("========== END AUD JOB ==========")


def run_acdv_job(spark, logger):
    logger.info("========== START ACDV JOB ==========")

    ensure_external_partitioned_table(spark, TGT_ACDV_TABLE, TGT_ACDV_LOCATION, logger, "ACDV_ID")

    numbers_df = build_numbers_df(spark)
    log_df_info(logger, numbers_df, "ACDV - cteNumbers", do_count=True, show_n=SAMPLE_SHOW)

    src = spark.table(SRC_ACDV_TABLE)

    base = src

    # Optional filter: ACDVControlNumber list
    if USE_ACDV_CONTROL_FILTER:
        logger.info("ACDV - Applying ACDV_CONTROL_LIST filter: %s", ACDV_CONTROL_LIST)
        base = base.where(F.col("ACDVControlNumber").cast("string").isin(ACDV_CONTROL_LIST))
    else:
        logger.info("ACDV - Skipping ACDV_CONTROL_LIST filter (USE_ACDV_CONTROL_FILTER=False)")

    base = (base
            .where(F.col("ACDVRESP_RESPONSE_DATE_TIME").isNotNull())
            .where(F.trim(F.col("ACDVREQ_ACCT_NUM")).rlike("^[0-9]+$"))
            .where(F.col("ACDVRESP_SEVEN_YEAR_PAY_HIST").isNotNull())
            .where(F.length(F.trim(F.col("ACDVRESP_SEVEN_YEAR_PAY_HIST"))) > 0)
            )

    base = base.where(F.to_date(F.col("ACDVRESP_RESPONSE_DATE_TIME")) >
                      F.add_months(F.current_date(), -int(MONTHS_BACK)))

    log_df_info(logger,
                base.select("ACDVREQ_ACCT_NUM", "ACDVControlNumber", "ACDVRESP_RESPONSE_DATE_TIME", "ACDVRESP_SEVEN_YEAR_PAY_HIST"),
                "ACDV - Source after base filters",
                do_count=True, show_n=SAMPLE_SHOW)

    joined = base.crossJoin(numbers_df)

    tr = build_common_columns(
        joined,
        date_col="ACDVRESP_RESPONSE_DATE_TIME",
        hist_col="ACDVRESP_SEVEN_YEAR_PAY_HIST",
        acct_col="ACDVREQ_ACCT_NUM",
        id_col="ACDVControlNumber",
        id_alias="ACDV_ID"
    )

    tr = apply_common_filters(tr, base_date_col="ACDVRESP_RESPONSE_DATE_TIME")

    log_df_info(logger,
                tr.select("AccountNumber", "ACDV_ID", "EventDate", "PHPDate", "PHPValue"),
                "ACDV - After explode + filters",
                do_count=True, show_n=SAMPLE_SHOW)

    w = Window.partitionBy(F.col("ACDVREQ_ACCT_NUM"), F.col("PHPDate")) \
              .orderBy(F.col("EventDate").desc(), F.col("ACDV_ID").desc())

    tr = tr.withColumn("RowNum", F.row_number().over(w))

    final_df = (tr
                .where(F.col("RowNum") == 1)
                .select("AccountNumber", "ACDV_ID", "EventDate", "PHPDate", "PHPValue")
                .orderBy(F.col("PHPValue").desc(), F.col("PHPDate").desc())
                )

    log_df_info(logger, final_df, "ACDV - Final (RowNum=1)", do_count=True, show_n=SAMPLE_SHOW)

    out = (final_df
           .withColumn("php_month", F.date_format(F.col("PHPDate"), "yyyy-MM"))
           .select("AccountNumber", "ACDV_ID", "EventDate", "PHPDate", "PHPValue", "php_month")
           )

    log_df_info(logger, out, "ACDV - Final with partition", do_count=True, show_n=SAMPLE_SHOW)

    write_partitioned(logger, spark, out, TGT_ACDV_TABLE)

    logger.info("========== END ACDV JOB ==========")


def main():
    logger = setup_logging(LOG_LEVEL)
    start_ts = time.time()

    spark = (SparkSession.builder
             .appName(APP_NAME)
             .enableHiveSupport()
             .getOrCreate())

    logger.info("Spark version: %s", spark.version)
    logger.info("Python version: %s", sys.version.replace("\n", " "))

    # Run both
    run_aud_job(spark, logger)
    run_acdv_job(spark, logger)

    elapsed = int(time.time() - start_ts)
    logger.info("ALL JOBS COMPLETED in %s seconds", elapsed)

    spark.stop()


if __name__ == "__main__":
    main()
