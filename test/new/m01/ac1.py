# -*- coding: utf-8 -*-
from __future__ import print_function

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

###########################################################
# LOGGING
###########################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s acdv_php_transform - %(message)s"
)

logger = logging.getLogger("acdv_php_transform")

###########################################################
# CONSTANTS
###########################################################

APP_NAME = "dsas_crcoe_non_cx6_acdv_process"

SRC_ACDV_TABLE = "hive_dsas_fnsh_sanitized.eoscar_acdvarchive_hist"

# Final target is EXTERNAL + PARQUET + PARTITIONED
TARGET_ACDV_TABLE = "dsas_conformed.crcoe_non_cx6_php_acdv_table"
TARGET_ACDV_LOCATION = "/apps/hive/external/dsas_conformed/crcoe_non_cx6_php_acdv_table"

MAX_MONTHS = 84
MONTHS_BACK = 85
WRITE_REPARTITION = 200
SHUFFLE_PARTITIONS = 400

# Debug indicator
DEBUG_MODE = False

# If DEBUG_MODE = True, only these accounts will be processed
DEBUG_ACCOUNTS = [
    "515836146209",
    "123456789012"
]

# Optional control number filter
USE_ACDV_CONTROL_FILTER = False
ACDV_CONTROL_LIST = [
    "3978214565004",
    "99999097001872017",
    "396894994013003",
    "0789145041001"
]

###########################################################
# SPARK SESSION
###########################################################

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .enableHiveSupport()
    .getOrCreate()
)

###########################################################
# PERFORMANCE / HIVE CONFIG
###########################################################

spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.hadoop.hive.exec.max.dynamic.partitions", "5000")
spark.conf.set("spark.hadoop.hive.exec.max.dynamic.partitions.pernode", "5000")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

###########################################################
# CREATE EXTERNAL TABLE
###########################################################

def ensure_target_table():
    logger.info("Ensuring ACDV external target table exists: %s", TARGET_ACDV_TABLE)

    ddl = """
    CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
        AccountNumber BIGINT,
        ACDV_ID       STRING,
        EventDate     DATE,
        PHPDate       DATE,
        PHPValue      STRING
    )
    PARTITIONED BY (php_month STRING)
    STORED AS PARQUET
    LOCATION '{location}'
    """.format(
        table_name=TARGET_ACDV_TABLE,
        location=TARGET_ACDV_LOCATION
    )

    spark.sql(ddl)

###########################################################
# COMMON LOGIC
###########################################################

def build_php_logic(df, acct_col, id_col, date_col, hist_col, output_id_col):
    logger.info("Starting posexplode transformation")

    df = df.withColumn(
        "hist_array",
        F.split(F.col(hist_col), "")
    )

    df = df.select(
        "*",
        F.posexplode("hist_array").alias("idx", "PHPValue")
    )

    df = (
        df
        .filter(F.col("idx") < MAX_MONTHS)
        .filter(F.col("PHPValue").isNotNull())
        .filter(F.col("PHPValue") != "")
        .filter(F.col("PHPValue") != "-")
    )

    df = df.withColumn(
        "EventDate",
        F.to_date(F.col(date_col))
    )

    df = df.withColumn(
        "PHPDate",
        F.expr("add_months(trunc(EventDate, 'MM'), -(idx + 1))")
    )

    df = df.withColumn(
        "AccountNumber",
        F.trim(F.col(acct_col)).cast("bigint")
    )

    df = df.withColumn(
        output_id_col,
        F.col(id_col).cast("string")
    )

    logger.info("Posexplode transformation completed")

    logger.info("Applying row_number logic")

    w = Window.partitionBy(
        "AccountNumber",
        "PHPDate"
    ).orderBy(
        F.col("EventDate").desc()
    )

    df = df.withColumn("RowNum", F.row_number().over(w))

    logger.info("Applying latest AUD logic to ACDV also")

    summary_df = (
        df.groupBy("AccountNumber", "PHPDate")
        .agg(
            F.count("*").alias("grp_cnt"),
            F.min("PHPValue").alias("min_php"),
            F.max("PHPValue").alias("max_php")
        )
    )

    base_df = df.filter(F.col("RowNum") == 1)

    final_df = (
        base_df.join(summary_df, ["AccountNumber", "PHPDate"], "left")
        .select(
            F.col("AccountNumber"),
            F.col(output_id_col),
            F.col("EventDate"),
            F.col("PHPDate"),
            F.when(
                (F.col("grp_cnt") > 1) & (F.col("min_php") != F.col("max_php")),
                F.lit("C")
            ).otherwise(F.col("PHPValue")).alias("PHPValue")
        )
    )

    return final_df

###########################################################
# READ + FILTER
###########################################################

def run_acdv_job():
    logger.info("========== START ACDV JOB ==========")

    src = (
        spark.table(SRC_ACDV_TABLE)
        .select(
            "ACDVREQ_ACCT_NUM",
            "ACDVControlNumber",
            "ACDVRESP_RESPONSE_DATE_TIME",
            "ACDVRESP_SEVEN_YEAR_PAY_HIST",
            "date_opened"
        )
    )

    df = (
        src
        .filter(F.col("ACDVRESP_RESPONSE_DATE_TIME").isNotNull())
        .filter(F.col("date_opened").isNotNull())
        .filter(F.trim(F.col("ACDVREQ_ACCT_NUM")).rlike("^[0-9]+$"))
        .filter(F.col("ACDVRESP_SEVEN_YEAR_PAY_HIST").isNotNull())
        .filter(F.length(F.trim(F.col("ACDVRESP_SEVEN_YEAR_PAY_HIST"))) > 0)
        .filter(F.regexp_replace(F.col("ACDVRESP_SEVEN_YEAR_PAY_HIST"), "-", "") != "")
        .filter(
            F.to_date(F.col("ACDVRESP_RESPONSE_DATE_TIME")) >
            F.add_months(F.current_date(), -MONTHS_BACK)
        )
    )

    if USE_ACDV_CONTROL_FILTER:
        logger.info("Applying ACDV control number filter")
        df = df.filter(F.col("ACDVControlNumber").cast("string").isin(ACDV_CONTROL_LIST))

    if DEBUG_MODE:
        logger.info("DEBUG_MODE is ON. Processing only debug accounts")
        df = df.filter(F.trim(F.col("ACDVREQ_ACCT_NUM")).isin(DEBUG_ACCOUNTS))

    df_php = build_php_logic(
        df=df,
        acct_col="ACDVREQ_ACCT_NUM",
        id_col="ACDVControlNumber",
        date_col="ACDVRESP_RESPONSE_DATE_TIME",
        hist_col="ACDVRESP_SEVEN_YEAR_PAY_HIST",
        output_id_col="ACDV_ID"
    )

    df_php = df_php.filter(
        F.col("PHPDate") > F.add_months(F.current_date(), -MONTHS_BACK)
    )

    logger.info("ACDV transformation completed")
    return df_php

###########################################################
# WRITE
###########################################################

def write_output(df):
    logger.info("Writing ACDV data to external Hive table: %s", TARGET_ACDV_TABLE)

    out_df = (
        df
        .withColumn("php_month", F.date_format(F.col("PHPDate"), "yyyy-MM"))
        .select(
            "AccountNumber",
            "ACDV_ID",
            "EventDate",
            "PHPDate",
            "PHPValue",
            "php_month"
        )
    )

    (
        out_df
        .repartition(WRITE_REPARTITION, "php_month")
        .write
        .mode("append")
        .format("hive")
        .insertInto(TARGET_ACDV_TABLE)
    )

    logger.info("ACDV write completed")

###########################################################
# MAIN
###########################################################

def main():
    logger.info("ACDV job started")
    ensure_target_table()
    acdv_df = run_acdv_job()
    write_output(acdv_df)
    logger.info("ACDV job completed successfully")

if __name__ == "__main__":
    main()
