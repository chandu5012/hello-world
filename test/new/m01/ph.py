# -*- coding: utf-8 -*-
from __future__ import print_function

import sys
import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
#                      CONSTANTS
# ============================================================

APP_NAME = "php_payment_history_load_v2"

MONTHS_BACK = 85
HISTORY_LEN = 84   # 0..83

# ---------- switches ----------
USE_AUD_ID_FILTER = False
USE_ACDV_CONTROL_FILTER = False

# For testing similar to SQL screenshot
USE_AUD_ACCOUNT_FILTER = False
AUD_TEST_ACCOUNT = "515836146209"

USE_ACDV_ACCOUNT_FILTER = False
ACDV_TEST_ACCOUNT = ""

# ---------- AUD ----------
SRC_AUD_TABLE = "HIVE.dsas_fnish_sanitized.eoscar_aud_hist"
AUD_ID_LIST = ["101343866", "107047397", "107403444", "107669735"]

TGT_AUD_TABLE = "dsas_conformed.crcoe_non_cx6_php_aud_table"
TGT_AUD_LOCATION = "/apps/hive/external/dsas_conformed/crcoe_non_cx6_php_aud_table"

# ---------- ACDV ----------
SRC_ACDV_TABLE = "HIVE.dsas_fnish_sanitized.eoscar_acdvArchive_hist"
ACDV_CONTROL_LIST = [
    "3978214565004",
    "99999097001872017",
    "396894994013003",
    "0789145041001"
]

TGT_ACDV_TABLE = "dsas_conformed.crcoe_non_cx6_php_acdv_table"
TGT_ACDV_LOCATION = "/apps/hive/external/dsas_conformed/crcoe_non_cx6_php_acdv_table"

LOG_LEVEL = "INFO"
SAMPLE_SHOW = 0


# ============================================================
#                      LOGGING
# ============================================================

def setup_logging(level):
    logger = logging.getLogger("php_transform")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger


def log_df_info(logger, df, label, do_count=True, show_n=0):
    logger.info("========== %s ==========", label)
    df.printSchema()
    if do_count:
        logger.info("Row count: %s", df.count())
    if show_n and show_n > 0:
        logger.info("Showing top %s rows", show_n)
        df.show(show_n, truncate=False)


# ============================================================
#                  SPARK / HIVE HELPERS
# ============================================================

def configure_spark(spark):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
    spark.conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("spark.hadoop.hive.exec.max.dynamic.partitions", "5000")
    spark.conf.set("spark.hadoop.hive.exec.max.dynamic.partitions.pernode", "5000")
    spark.conf.set("spark.debug.maxToStringFields", "2000")


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
    """.format(tbl=table_name, id_col=id_col_name, loc=location)

    logger.info("Ensuring external partitioned table exists: %s", table_name)
    spark.sql(ddl)


def write_partitioned(logger, spark, df, target_table):
    logger.info("Writing to target table (append): %s", target_table)
    (df.write
       .mode("append")
       .format("hive")
       .insertInto(target_table))

    try:
        spark.sql("MSCK REPAIR TABLE {0}".format(target_table))
    except Exception as e:
        logger.warning("MSCK REPAIR TABLE warning for %s : %s", target_table, str(e))


def build_numbers_df(spark):
    return spark.range(0, HISTORY_LEN).select(F.col("id").cast("int").alias("idx"))


# ============================================================
#                  COMMON TRANSFORM HELPERS
# ============================================================

def build_common_columns(df, date_col, hist_col, acct_col, id_col, id_alias):
    df = df.withColumn("EventDate", F.to_date(F.col(date_col)))

    # Spark 2.4 compatible month arithmetic
    df = df.withColumn(
        "PHPDate",
        F.expr("add_months(trunc(to_date({0}), 'MM'), -(idx + 1))".format(date_col))
    )

    df = df.withColumn(
        "PHPValue",
        F.expr("substring({0}, idx + 1, 1)".format(hist_col))
    )

    df = df.withColumn("AccountNumber", F.trim(F.col(acct_col)).cast("bigint"))
    df = df.withColumn(id_alias, F.col(id_col).cast("string"))

    return df


def apply_common_history_filters(df, raw_date_col):
    df = df.where(F.col(raw_date_col).isNotNull())
    df = df.where(F.to_date(F.col(raw_date_col)) > F.add_months(F.current_date(), -int(MONTHS_BACK)))
    df = df.where(~F.coalesce(F.col("PHPValue"), F.lit("")).isin(["", "-"]))
    df = df.where(F.col("PHPDate") > F.add_months(F.current_date(), -int(MONTHS_BACK)))
    return df


# ============================================================
#                        AUD JOB
# ============================================================

def run_aud_job(spark, logger):
    logger.info("========== START AUD JOB ==========")

    ensure_external_partitioned_table(spark, TGT_AUD_TABLE, TGT_AUD_LOCATION, logger, "AUD_ID")

    numbers_df = build_numbers_df(spark)
    log_df_info(logger, numbers_df, "AUD - cteNumbers", do_count=True, show_n=SAMPLE_SHOW)

    src = spark.table(SRC_AUD_TABLE)
    base = src

    if USE_AUD_ID_FILTER:
        logger.info("AUD - Applying AUD_ID_LIST filter")
        base = base.where(F.col("aud_id").cast("string").isin(AUD_ID_LIST))
    else:
        logger.info("AUD - Skipping AUD_ID_LIST filter")

    if USE_AUD_ACCOUNT_FILTER and AUD_TEST_ACCOUNT:
        logger.info("AUD - Applying acct_num filter: %s", AUD_TEST_ACCOUNT)
        base = base.where(F.trim(F.col("acct_num")) == F.lit(AUD_TEST_ACCOUNT))
    else:
        logger.info("AUD - Skipping acct_num test filter")

    base = (base
            .where(F.col("date_created").isNotNull())
            .where(F.col("date_opened").isNotNull())
            .where(F.trim(F.col("acct_num")).rlike("^[0-9]+$"))
            .where(F.col("seven_year_payment_history").isNotNull())
            .where(F.length(F.trim(F.col("seven_year_payment_history"))) > 0)
            .where(F.regexp_replace(F.col("seven_year_payment_history"), "-", "") != "")
            .where(F.to_date(F.col("date_created")) > F.add_months(F.current_date(), -int(MONTHS_BACK)))
            )

    log_df_info(
        logger,
        base.select("acct_num", "aud_id", "date_created", "seven_year_payment_history"),
        "AUD - Source after base filters",
        do_count=True,
        show_n=SAMPLE_SHOW
    )

    joined = base.crossJoin(numbers_df)

    tr = build_common_columns(
        joined,
        date_col="date_created",
        hist_col="seven_year_payment_history",
        acct_col="acct_num",
        id_col="aud_id",
        id_alias="AUD_ID"
    )

    tr = apply_common_history_filters(tr, "date_created")

    log_df_info(
        logger,
        tr.select("AccountNumber", "AUD_ID", "EventDate", "PHPDate", "PHPValue"),
        "AUD - After explode + filters",
        do_count=True,
        show_n=SAMPLE_SHOW
    )

    # Updated RowNum logic as per new SQL:
    # PARTITION BY acct_num, PHPDate
    # ORDER BY EventDate DESC
    aud_window = (
        Window.partitionBy(F.col("acct_num"), F.col("PHPDate"))
              .orderBy(F.col("EventDate").desc())
    )

    cte_aud = tr.withColumn("RowNum", F.row_number().over(aud_window))

    log_df_info(
        logger,
        cte_aud.select("AccountNumber", "AUD_ID", "EventDate", "PHPDate", "PHPValue", "RowNum"),
        "AUD - cteAUDData with RowNum",
        do_count=False,
        show_n=SAMPLE_SHOW
    )

    # New SQL logic:
    # CASE WHEN EXISTS (
    #   SELECT 1 FROM cteAUDData CHK
    #   WHERE CHK.AccountNumber = cteAUDData.AccountNumber
    #     AND CHK.PHPDate = cteAUDData.PHPDate
    #     AND CHK.RowNum > 1
    #     AND CHK.PHPValue <> cteAUDData.PHPValue
    # ) THEN 'C'
    # ELSE cteAUDData.PHPValue
    # END AS PHPValue
    base_r1 = cte_aud.where(F.col("RowNum") == 1).alias("base")
    chk = cte_aud.where(F.col("RowNum") > 1).alias("chk")

    diff_rows = (
        base_r1.join(
            chk,
            (F.col("base.AccountNumber") == F.col("chk.AccountNumber")) &
            (F.col("base.PHPDate") == F.col("chk.PHPDate")) &
            (F.col("base.PHPValue") != F.col("chk.PHPValue")),
            "left"
        )
        .select(
            F.col("base.AccountNumber").alias("AccountNumber"),
            F.col("base.AUD_ID").alias("AUD_ID"),
            F.col("base.EventDate").alias("EventDate"),
            F.col("base.PHPDate").alias("PHPDate"),
            F.when(F.col("chk.AccountNumber").isNotNull(), F.lit("C"))
             .otherwise(F.col("base.PHPValue")).alias("PHPValue")
        )
        .dropDuplicates(["AccountNumber", "AUD_ID", "EventDate", "PHPDate"])
    )

    final_df = diff_rows.where(
        F.col("PHPDate") > F.add_months(F.current_date(), -int(MONTHS_BACK))
    ).orderBy(F.col("PHPDate").desc(), F.col("EventDate").desc())

    log_df_info(logger, final_df, "AUD - Final transformed data", do_count=True, show_n=SAMPLE_SHOW)

    out = (
        final_df
        .withColumn("php_month", F.date_format(F.col("PHPDate"), "yyyy-MM"))
        .select("AccountNumber", "AUD_ID", "EventDate", "PHPDate", "PHPValue", "php_month")
    )

    log_df_info(logger, out, "AUD - Final with partition", do_count=True, show_n=SAMPLE_SHOW)

    write_partitioned(logger, spark, out, TGT_AUD_TABLE)

    logger.info("========== END AUD JOB ==========")


# ============================================================
#                       ACDV JOB
# ============================================================

def run_acdv_job(spark, logger):
    logger.info("========== START ACDV JOB ==========")

    ensure_external_partitioned_table(spark, TGT_ACDV_TABLE, TGT_ACDV_LOCATION, logger, "ACDV_ID")

    numbers_df = build_numbers_df(spark)
    log_df_info(logger, numbers_df, "ACDV - cteNumbers", do_count=True, show_n=SAMPLE_SHOW)

    src = spark.table(SRC_ACDV_TABLE)
    base = src

    if USE_ACDV_CONTROL_FILTER:
        logger.info("ACDV - Applying ACDV_CONTROL_LIST filter")
        base = base.where(F.col("ACDVControlNumber").cast("string").isin(ACDV_CONTROL_LIST))
    else:
        logger.info("ACDV - Skipping ACDV_CONTROL_LIST filter")

    if USE_ACDV_ACCOUNT_FILTER and ACDV_TEST_ACCOUNT:
        logger.info("ACDV - Applying account filter: %s", ACDV_TEST_ACCOUNT)
        base = base.where(F.trim(F.col("ACDVREQ_ACCT_NUM")) == F.lit(ACDV_TEST_ACCOUNT))
    else:
        logger.info("ACDV - Skipping account test filter")

    base = (base
            .where(F.col("ACDVRESP_RESPONSE_DATE_TIME").isNotNull())
            .where(F.col("date_opened").isNotNull())
            .where(F.trim(F.col("ACDVREQ_ACCT_NUM")).rlike("^[0-9]+$"))
            .where(F.col("ACDVRESP_SEVEN_YEAR_PAY_HIST").isNotNull())
            .where(F.length(F.trim(F.col("ACDVRESP_SEVEN_YEAR_PAY_HIST"))) > 0)
            .where(F.regexp_replace(F.col("ACDVRESP_SEVEN_YEAR_PAY_HIST"), "-", "") != "")
            .where(F.to_date(F.col("ACDVRESP_RESPONSE_DATE_TIME")) > F.add_months(F.current_date(), -int(MONTHS_BACK)))
            )

    log_df_info(
        logger,
        base.select("ACDVREQ_ACCT_NUM", "ACDVControlNumber", "ACDVRESP_RESPONSE_DATE_TIME", "ACDVRESP_SEVEN_YEAR_PAY_HIST"),
        "ACDV - Source after base filters",
        do_count=True,
        show_n=SAMPLE_SHOW
    )

    joined = base.crossJoin(numbers_df)

    tr = build_common_columns(
        joined,
        date_col="ACDVRESP_RESPONSE_DATE_TIME",
        hist_col="ACDVRESP_SEVEN_YEAR_PAY_HIST",
        acct_col="ACDVREQ_ACCT_NUM",
        id_col="ACDVControlNumber",
        id_alias="ACDV_ID"
    )

    tr = apply_common_history_filters(tr, "ACDVRESP_RESPONSE_DATE_TIME")

    log_df_info(
        logger,
        tr.select("AccountNumber", "ACDV_ID", "EventDate", "PHPDate", "PHPValue"),
        "ACDV - After explode + filters",
        do_count=True,
        show_n=SAMPLE_SHOW
    )

    # Applied same updated pattern to ACDV
    acdv_window = (
        Window.partitionBy(F.col("ACDVREQ_ACCT_NUM"), F.col("PHPDate"))
              .orderBy(F.col("EventDate").desc())
    )

    cte_acdv = tr.withColumn("RowNum", F.row_number().over(acdv_window))

    log_df_info(
        logger,
        cte_acdv.select("AccountNumber", "ACDV_ID", "EventDate", "PHPDate", "PHPValue", "RowNum"),
        "ACDV - cteACDVData with RowNum",
        do_count=False,
        show_n=SAMPLE_SHOW
    )

    base_r1 = cte_acdv.where(F.col("RowNum") == 1).alias("base")
    chk = cte_acdv.where(F.col("RowNum") > 1).alias("chk")

    diff_rows = (
        base_r1.join(
            chk,
            (F.col("base.AccountNumber") == F.col("chk.AccountNumber")) &
            (F.col("base.PHPDate") == F.col("chk.PHPDate")) &
            (F.col("base.PHPValue") != F.col("chk.PHPValue")),
            "left"
        )
        .select(
            F.col("base.AccountNumber").alias("AccountNumber"),
            F.col("base.ACDV_ID").alias("ACDV_ID"),
            F.col("base.EventDate").alias("EventDate"),
            F.col("base.PHPDate").alias("PHPDate"),
            F.when(F.col("chk.AccountNumber").isNotNull(), F.lit("C"))
             .otherwise(F.col("base.PHPValue")).alias("PHPValue")
        )
        .dropDuplicates(["AccountNumber", "ACDV_ID", "EventDate", "PHPDate"])
    )

    final_df = diff_rows.where(
        F.col("PHPDate") > F.add_months(F.current_date(), -int(MONTHS_BACK))
    ).orderBy(F.col("PHPDate").desc(), F.col("EventDate").desc())

    log_df_info(logger, final_df, "ACDV - Final transformed data", do_count=True, show_n=SAMPLE_SHOW)

    out = (
        final_df
        .withColumn("php_month", F.date_format(F.col("PHPDate"), "yyyy-MM"))
        .select("AccountNumber", "ACDV_ID", "EventDate", "PHPDate", "PHPValue", "php_month")
    )

    log_df_info(logger, out, "ACDV - Final with partition", do_count=True, show_n=SAMPLE_SHOW)

    write_partitioned(logger, spark, out, TGT_ACDV_TABLE)

    logger.info("========== END ACDV JOB ==========")


# ============================================================
#                          MAIN
# ============================================================

def main():
    logger = setup_logging(LOG_LEVEL)
    start_time = time.time()

    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .enableHiveSupport()
        .getOrCreate()
    )

    configure_spark(spark)

    logger.info("Spark version: %s", spark.version)
    logger.info("Python version: %s", sys.version.replace("\n", " "))

    run_aud_job(spark, logger)
    run_acdv_job(spark, logger)

    logger.info("ALL JOBS COMPLETED in %s seconds", int(time.time() - start_time))
    spark.stop()


if __name__ == "__main__":
    main()
