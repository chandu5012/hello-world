# -*- coding: utf-8 -*-
from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =========================================================
# CONSTANTS
# =========================================================
APP_NAME = "acdv_php_load"

SRC_TABLE = "hive_dsas_fnsh_sanitized.eoscar_acdvarchive_hist"

TGT_TABLE = "dsas_conformed.crcoe_non_cx6_php_acdv_table"
TGT_LOCATION = "/apps/hive/external/dsas_conformed/crcoe_non_cx6_php_acdv_table"

MAX_MONTHS = 84
MONTHS_BACK = 85
WRITE_REPARTITION = 200


# =========================================================
# SPARK
# =========================================================
spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .enableHiveSupport()
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")


# =========================================================
# CREATE TARGET EXTERNAL TABLE
# =========================================================
create_target_sql = """
CREATE EXTERNAL TABLE IF NOT EXISTS {0} (
    AccountNumber BIGINT,
    ACDV_ID       STRING,
    EventDate     DATE,
    PHPDate       DATE,
    PHPValue      STRING
)
PARTITIONED BY (php_month STRING)
STORED AS PARQUET
LOCATION '{1}'
""".format(TGT_TABLE, TGT_LOCATION)

spark.sql(create_target_sql)


# =========================================================
# STEP-1: cteNumbers (0 to 83)
# =========================================================
numbers_df = spark.range(0, MAX_MONTHS).select(
    F.col("id").cast("int").alias("idx")
)


# =========================================================
# STEP-2: Read source table from Hive
# =========================================================
src_df = spark.table(SRC_TABLE).select(
    "ACDVREQ_ACCT_NUM",
    "ACDVControlNumber",
    "ACDVRESP_RESPONSE_DATE_TIME",
    "ACDVRESP_SEVEN_YEAR_PAY_HIST",
    "date_opened"
)


# =========================================================
# STEP-3: Join with numbers and derive columns
# =========================================================
base_df = (
    src_df.crossJoin(numbers_df)
    .withColumn("EventDate", F.to_date(F.col("ACDVRESP_RESPONSE_DATE_TIME")))
    .withColumn(
        "PHPDate",
        F.add_months(
            F.trunc(F.to_date(F.col("ACDVRESP_RESPONSE_DATE_TIME")), "MM"),
            -(F.col("idx") + F.lit(1))
        )
    )
    .withColumn(
        "PHPValue",
        F.expr("substr(ACDVRESP_SEVEN_YEAR_PAY_HIST, idx + 1, 1)")
    )
    .withColumn(
        "AccountNumber",
        F.trim(F.col("ACDVREQ_ACCT_NUM")).cast("bigint")
    )
    .withColumn(
        "ACDV_ID",
        F.col("ACDVControlNumber").cast("string")
    )
)


# =========================================================
# STEP-4: Apply same SQL filters
# =========================================================
filtered_df = (
    base_df
    .filter(F.coalesce(F.col("PHPValue"), F.lit("")).isin("", "-") == False)
    .filter(F.coalesce(F.col("ACDVRESP_SEVEN_YEAR_PAY_HIST"), F.lit("")) != "")
    .filter(F.regexp_replace(F.col("ACDVRESP_SEVEN_YEAR_PAY_HIST"), "-", "") != "")
    .filter(F.col("ACDVRESP_RESPONSE_DATE_TIME").isNotNull())
    .filter(F.to_date(F.col("ACDVRESP_RESPONSE_DATE_TIME")) > F.add_months(F.current_date(), -MONTHS_BACK))
    .filter(F.col("date_opened").isNotNull())
    .filter(F.col("AccountNumber").isNotNull())
)


# =========================================================
# STEP-5: Apply row_number()
# =========================================================
w = (
    Window.partitionBy("ACDVREQ_ACCT_NUM", "PHPDate")
    .orderBy(F.col("EventDate").desc())
)

cte_df = filtered_df.withColumn("RowNum", F.row_number().over(w))


# =========================================================
# STEP-6: Apply same CASE WHEN EXISTS logic
# =========================================================
base_r1_df = cte_df.filter(F.col("RowNum") == 1).alias("base")
chk_df = cte_df.filter(F.col("RowNum") > 1).alias("chk")

final_df = (
    base_r1_df.join(
        chk_df,
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
        F.when(
            F.col("chk.AccountNumber").isNotNull(),
            F.lit("C")
        ).otherwise(F.col("base.PHPValue")).alias("PHPValue")
    )
    .dropDuplicates(["AccountNumber", "ACDV_ID", "EventDate", "PHPDate"])
    .filter(F.col("PHPDate") > F.add_months(F.current_date(), -MONTHS_BACK))
)


# =========================================================
# STEP-7: Add partition column and write to external table
# =========================================================
out_df = final_df.select(
    "AccountNumber",
    "ACDV_ID",
    "EventDate",
    "PHPDate",
    "PHPValue",
    F.date_format(F.col("PHPDate"), "yyyy-MM").alias("php_month")
)

(
    out_df
    .repartition(WRITE_REPARTITION, "php_month")
    .write
    .mode("append")
    .format("hive")
    .insertInto(TGT_TABLE)
)

spark.stop()
