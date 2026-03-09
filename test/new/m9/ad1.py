# -*- coding: utf-8 -*-

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


############################################
# CONFIGURATION
############################################

DEBUG_MODE = False
LOG_SCHEMA = False
LOG_COUNT = False
LOG_SHOW = False

OVERWRITE_TABLE = False

ENABLE_ACCOUNT_FILTER = False

TEST_ACCOUNTS = [
    "518536146209"
]

SRC_TABLE = "hive_dsas_fnsh_sanitized.eoscar_aud_hist"

TGT_TABLE = "dsas_conformed.crcoe_non_cx6_php_aud_table"
TGT_LOCATION = "/apps/hive/external/dsas_conformed/crcoe_non_cx6_php_aud_table"

MAX_MONTHS = 84
MONTHS_BACK = 85
WRITE_PARTITIONS = 200


############################################
# LOGGING
############################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s AUD_JOB - %(message)s"
)

logger = logging.getLogger("AUD_JOB")


############################################
# DEBUG FUNCTION
############################################

def debug_df(df, name, cols=None, limit=10):

    if not DEBUG_MODE:
        return

    logger.info("========== DEBUG DF : %s ==========", name)

    if LOG_SCHEMA:
        logger.info("Schema for %s", name)
        df.printSchema()

    if LOG_COUNT:
        try:
            cnt = df.count()
            logger.info("Count for %s = %s", name, cnt)
        except Exception as e:
            logger.error("Count failed for %s : %s", name, str(e))

    if LOG_SHOW:
        try:
            if cols:
                df.select(*cols).show(limit, False)
            else:
                df.show(limit, False)
        except Exception as e:
            logger.error("Show failed for %s : %s", name, str(e))


############################################
# TABLE RESET
############################################

def reset_table_if_required(spark, table_name, location):

    if not OVERWRITE_TABLE:
        logger.info("Overwrite disabled")
        return

    logger.info("Dropping table %s", table_name)

    spark.sql("DROP TABLE IF EXISTS {0} PURGE".format(table_name))

    logger.info("Deleting folder %s", location)

    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(location)

    if fs.exists(path):
        fs.delete(path, True)


############################################
# SPARK SESSION
############################################

spark = (
    SparkSession.builder
    .appName("AUD_PHP_JOB")
    .enableHiveSupport()
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "400")


############################################
# RESET TABLE
############################################

reset_table_if_required(spark, TGT_TABLE, TGT_LOCATION)


############################################
# CREATE TABLE
############################################

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS dsas_conformed.crcoe_non_cx6_php_aud_table(
AccountNumber BIGINT,
AUD_ID STRING,
EventDate DATE,
PHPDate DATE,
PHPValue STRING
)
PARTITIONED BY (php_month STRING)
STORED AS PARQUET
LOCATION '/apps/hive/external/dsas_conformed/crcoe_non_cx6_php_aud_table'
""")


############################################
# STEP1 NUMBERS TABLE
############################################

numbers_df = spark.range(0, MAX_MONTHS).select(F.col("id").alias("idx"))

debug_df(numbers_df,"numbers_df")


############################################
# STEP2 SOURCE
############################################

src_df = spark.table(SRC_TABLE)

debug_df(src_df,"src_df",["acct_num","aud_id"])


############################################
# OPTIONAL FILTER
############################################

if ENABLE_ACCOUNT_FILTER:

    logger.info("Applying test account filter")

    src_df = src_df.filter(
        F.col("acct_num").isin(TEST_ACCOUNTS)
    )


############################################
# STEP3 BASE TRANSFORMATION
############################################

base_df = (
    src_df.crossJoin(numbers_df)
    .withColumn("EventDate",F.to_date("date_created"))
    .withColumn(
        "PHPDate",
        F.expr("add_months(trunc(to_date(date_created),'MM'),-(idx+1))")
    )
    .withColumn(
        "PHPValue",
        F.expr("substr(seven_year_payment_history,idx+1,1)")
    )
    .withColumn(
        "AccountNumber",
        F.trim(F.col("acct_num")).cast("bigint")
    )
)

debug_df(base_df,"base_df",["AccountNumber","PHPDate","PHPValue"])


############################################
# STEP4 FILTERS
############################################

filtered_df = (
    base_df
    .filter(~F.coalesce(F.col("PHPValue"),F.lit("")).isin("","-"))
    .filter(F.col("date_created").isNotNull())
    .filter(F.col("date_opened").isNotNull())
    .filter(F.col("AccountNumber").isNotNull())
    .filter(
        F.to_date("date_created") >
        F.add_months(F.current_date(),-MONTHS_BACK)
    )
)

debug_df(filtered_df,"filtered_df")


############################################
# STEP5 WINDOW
############################################

window = Window.partitionBy("acct_num","PHPDate").orderBy(F.col("EventDate").desc())

cte_df = filtered_df.withColumn("RowNum",F.row_number().over(window))

debug_df(cte_df,"cte_df")


############################################
# STEP6 'C' LOGIC
############################################

base = cte_df.filter("RowNum=1").alias("base")
chk = cte_df.filter("RowNum>1").alias("chk")

final_df = (
    base.join(
        chk,
        (F.col("base.AccountNumber")==F.col("chk.AccountNumber")) &
        (F.col("base.PHPDate")==F.col("chk.PHPDate")) &
        (F.col("base.PHPValue")!=F.col("chk.PHPValue")),
        "left"
    )
    .select(
        F.col("base.AccountNumber").alias("AccountNumber"),
        F.col("base.aud_id").alias("AUD_ID"),
        F.col("base.EventDate"),
        F.col("base.PHPDate"),
        F.when(
            F.col("chk.AccountNumber").isNotNull(),
            F.lit("C")
        ).otherwise(F.col("base.PHPValue")).alias("PHPValue")
    )
)

debug_df(final_df,"final_df")


############################################
# STEP7 WRITE
############################################

out_df = final_df.select(
    "AccountNumber",
    "AUD_ID",
    "EventDate",
    "PHPDate",
    "PHPValue",
    F.date_format("PHPDate","yyyy-MM").alias("php_month")
)

debug_df(out_df,"out_df")


(
    out_df
    .repartition(WRITE_PARTITIONS,"php_month")
    .write
    .mode("append")
    .format("hive")
    .insertInto(TGT_TABLE)
)

logger.info("AUD Job Completed")
