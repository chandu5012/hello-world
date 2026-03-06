#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# LOGGER
# ============================================================
def get_logger():
    logger = logging.getLogger("RID352_RECOMMENDED_PHP_DSAS")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


log = get_logger()


# ============================================================
# CONFIG
# ============================================================
CX_TABLE = "RID352_RecommendedPHP_CX8"
ACDV_TABLE = "RID352_RecommendedPHP_ACDV"
AUD_TABLE = "RID352_RecommendedPHP_AUD"
CRA_TABLE = "RID352_RecommendedPHP_CRA"

TARGET_TABLE = "RID352_RecommendedPHP"

REPARTITION_CNT = 400
DEBUG_MODE = "False"              # "True" or "False"
ACCOUNT_LIST_ENABLED = "False"    # "True" or "False"

TEST_ACCOUNT_LIST = [
    "1234567890",
    "9876543210"
]


# ============================================================
# SPARK SESSION
# ============================================================
spark = (
    SparkSession.builder
    .appName("RID352_RecommendedPHP_DSAS")
    .enableHiveSupport()
    .getOrCreate()
)

log.info("Spark session started")
log.info("Spark version: %s", spark.version)

# ------------------------------------------------------------
# SPARK PERFORMANCE SETTINGS
# ------------------------------------------------------------
spark.conf.set("spark.sql.shuffle.partitions", str(REPARTITION_CNT))
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.broadcastTimeout", "1200")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

if spark.version.startswith("3"):
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    log.info("Adaptive Query Execution enabled for Spark 3.x")

log.info("spark.sql.shuffle.partitions = %s", spark.conf.get("spark.sql.shuffle.partitions"))
log.info("spark.sql.autoBroadcastJoinThreshold = %s", spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))


# ============================================================
# HELPERS
# ============================================================
def is_true(val):
    return str(val).strip().lower() == "true"


def coalesce_str(col_name):
    return F.coalesce(F.col(col_name).cast("string"), F.lit(""))


def safe_to_date(col_name):
    return F.to_date(F.col(col_name))


def debug_show(df, name, cnt=10):
    if is_true(DEBUG_MODE):
        log.info("Showing sample records for %s", name)
        df.show(cnt, False)
        log.info("Schema for %s", name)
        df.printSchema()


def apply_account_filter(df, col_name, enabled, account_list, df_name):
    """
    Behavior:
    1. enabled = False  -> full data
    2. enabled = True and list present -> filter those accounts
    3. enabled = True and list empty   -> full data with warning
    """
    if is_true(enabled):
        acct_list = [str(x).strip() for x in account_list if str(x).strip() != ""]

        if len(acct_list) == 0:
            log.warning(
                "ACCOUNT_LIST_ENABLED is True but account list is empty for %s. Processing full dataset.",
                df_name
            )
            return df

        log.info("Applying account filter on %s", df_name)
        log.info("Account list: %s", ",".join(acct_list))

        return df.filter(F.col(col_name).cast("string").isin(acct_list))
    else:
        log.info("Account filter disabled for %s. Processing full data.", df_name)
        return df


# ============================================================
# STEP 1: READ SOURCE TABLES
# ============================================================
log.info("Reading DSAS source tables")

cx_df = spark.table(CX_TABLE)
acdv_df = spark.table(ACDV_TABLE)
aud_df = spark.table(AUD_TABLE)
cra_df = spark.table(CRA_TABLE)

debug_show(cx_df, "cx_df_raw", 5)
debug_show(acdv_df, "acdv_df_raw", 5)
debug_show(aud_df, "aud_df_raw", 5)
debug_show(cra_df, "cra_df_raw", 5)


# ============================================================
# STEP 2: OPTIONAL ACCOUNT FILTER
# ============================================================
log.info("Applying optional test account filter")

cx_df = apply_account_filter(
    cx_df,
    "consumer_account_number",
    ACCOUNT_LIST_ENABLED,
    TEST_ACCOUNT_LIST,
    "cx_df"
)

acdv_df = apply_account_filter(
    acdv_df,
    "AccountNumber",
    ACCOUNT_LIST_ENABLED,
    TEST_ACCOUNT_LIST,
    "acdv_df"
)

aud_df = apply_account_filter(
    aud_df,
    "ACCOUNTNUMBER",
    ACCOUNT_LIST_ENABLED,
    TEST_ACCOUNT_LIST,
    "aud_df"
)

cra_df = apply_account_filter(
    cra_df,
    "AccountNumber",
    ACCOUNT_LIST_ENABLED,
    TEST_ACCOUNT_LIST,
    "cra_df"
)


# ============================================================
# STEP 3: STANDARDIZE COLUMN NAMES
# ============================================================
log.info("Standardizing source tables")

# ---------------- CX8 ----------------
# consumer_account_number, date_opened, file_id, base_id,
# modification_id, date_of_account_information, transmitdate,
# phpdate, phpvalue, event_date
cx_std_df = cx_df.select(
    F.col("consumer_account_number").cast("string").alias("AccountNumber"),
    safe_to_date("date_opened").alias("DateOpened"),
    F.col("file_id").cast("string").alias("FileID"),
    F.col("base_id").cast("string").alias("BaseID"),
    F.col("modification_id").cast("string").alias("ModificationID"),
    safe_to_date("date_of_account_information").alias("DateOfAccountInformation"),
    safe_to_date("transmitdate").alias("TransmitDate"),
    safe_to_date("phpdate").alias("PHPDate"),
    F.col("phpvalue").cast("string").alias("CX_PHPValue"),
    safe_to_date("event_date").alias("CX_EventDate")
)

# ---------------- ACDV ----------------
# AccountNumber, ACDV_ID, EventDate, PHPDate, PHPValue
acdv_std_df = acdv_df.select(
    F.col("AccountNumber").cast("string").alias("ACDV_AccountNumber"),
    F.col("ACDV_ID").cast("string").alias("ACDV_ID"),
    safe_to_date("EventDate").alias("ACDV_EventDate"),
    safe_to_date("PHPDate").alias("ACDV_PHPDate"),
    F.when(F.col("PHPValue").cast("string") == F.lit("C"), F.lit("D"))
     .otherwise(F.col("PHPValue").cast("string")).alias("ACDV_PHPValue"),
    F.when(F.col("PHPValue").cast("string") == F.lit("C"), F.lit(1))
     .otherwise(F.lit(0)).alias("CValue_ACDV")
)

# ---------------- AUD ----------------
# ACCOUNTNUMBER, AUD_ID, EVENTDATE, PHPDATE, PHPVALUE
aud_std_df = aud_df.select(
    F.col("ACCOUNTNUMBER").cast("string").alias("AUD_AccountNumber"),
    F.col("AUD_ID").cast("string").alias("AUD_ID"),
    safe_to_date("EVENTDATE").alias("AUD_EventDate"),
    safe_to_date("PHPDATE").alias("AUD_PHPDate"),
    F.when(F.col("PHPVALUE").cast("string") == F.lit("C"), F.lit("D"))
     .otherwise(F.col("PHPVALUE").cast("string")).alias("AUD_PHPValue"),
    F.when(F.col("PHPVALUE").cast("string") == F.lit("C"), F.lit(1))
     .otherwise(F.lit(0)).alias("CValue_AUD")
)

# ---------------- CRA ----------------
# AccountNumber, CRA_ID, EventDate, PHPDate, PHPValue
cra_std_df = cra_df.select(
    F.col("AccountNumber").cast("string").alias("CRA_AccountNumber"),
    F.col("CRA_ID").cast("string").alias("CRA_ID"),
    safe_to_date("EventDate").alias("CRA_EventDate"),
    safe_to_date("PHPDate").alias("CRA_PHPDate"),
    F.when(F.col("PHPValue").cast("string") == F.lit("C"), F.lit("D"))
     .otherwise(F.col("PHPValue").cast("string")).alias("CRA_PHPValue"),
    F.when(F.col("PHPValue").cast("string") == F.lit("C"), F.lit(1))
     .otherwise(F.lit(0)).alias("CValue_CRA")
)

debug_show(cx_std_df, "cx_std_df", 5)
debug_show(acdv_std_df, "acdv_std_df", 5)
debug_show(aud_std_df, "aud_std_df", 5)
debug_show(cra_std_df, "cra_std_df", 5)


# ============================================================
# STEP 4: REPARTITION ONLY LARGE CX TABLE
# ============================================================
log.info("Repartitioning only CX table because CX is very large")

cx_std_df = cx_std_df.repartition(REPARTITION_CNT, "AccountNumber", "PHPDate")


# ============================================================
# STEP 5: JOIN 4 SOURCE TABLES
# No broadcast because ACDV/AUD/CRA are medium-sized
# ============================================================
log.info("Joining CX8 with ACDV / AUD / CRA")

joined_df = (
    cx_std_df.alias("CX")
    .join(
        acdv_std_df.alias("ACDV"),
        (F.col("CX.AccountNumber") == F.col("ACDV.ACDV_AccountNumber")) &
        (F.col("CX.PHPDate") == F.col("ACDV.ACDV_PHPDate")),
        "left"
    )
    .join(
        aud_std_df.alias("AUD"),
        (F.col("CX.AccountNumber") == F.col("AUD.AUD_AccountNumber")) &
        (F.col("CX.PHPDate") == F.col("AUD.AUD_PHPDate")),
        "left"
    )
    .join(
        cra_std_df.alias("CRA"),
        (F.col("CX.AccountNumber") == F.col("CRA.CRA_AccountNumber")) &
        (F.col("CX.PHPDate") == F.col("CRA.CRA_PHPDate")),
        "left"
    )
    .select(
        F.col("CX.AccountNumber"),
        F.col("CX.DateOpened"),
        F.col("CX.FileID"),
        F.col("CX.BaseID"),
        F.col("CX.ModificationID"),
        F.col("CX.DateOfAccountInformation"),
        F.col("CX.TransmitDate"),
        F.col("CX.PHPDate"),
        F.col("CX.CX_PHPValue"),
        F.col("CX.CX_EventDate"),

        F.col("ACDV.ACDV_ID"),
        F.col("ACDV.ACDV_EventDate"),
        F.col("ACDV.ACDV_PHPValue"),
        F.col("ACDV.CValue_ACDV"),

        F.col("AUD.AUD_ID"),
        F.col("AUD.AUD_EventDate"),
        F.col("AUD.AUD_PHPValue"),
        F.col("AUD.CValue_AUD"),

        F.col("CRA.CRA_ID"),
        F.col("CRA.CRA_EventDate"),
        F.col("CRA.CRA_PHPValue"),
        F.col("CRA.CValue_CRA")
    )
)

debug_show(joined_df, "joined_df", 10)


# ============================================================
# STEP 6: DERIVED COLUMNS
# ============================================================
log.info("Creating derived columns")

derived_df = (
    joined_df
    .withColumn(
        "LastEventDate",
        F.greatest(
            F.col("CX_EventDate"),
            F.col("ACDV_EventDate"),
            F.col("AUD_EventDate"),
            F.col("CRA_EventDate")
        )
    )
    .withColumn(
        "AllPHPValues",
        F.concat(
            coalesce_str("CX_PHPValue"),
            coalesce_str("ACDV_PHPValue"),
            coalesce_str("AUD_PHPValue"),
            coalesce_str("CRA_PHPValue")
        )
    )
    .withColumn(
        "LastEventPHPValues",
        F.concat(
            F.when(F.col("CX_EventDate") == F.col("LastEventDate"), coalesce_str("CX_PHPValue")).otherwise(F.lit("")),
            F.when(F.col("ACDV_EventDate") == F.col("LastEventDate"), coalesce_str("ACDV_PHPValue")).otherwise(F.lit("")),
            F.when(F.col("AUD_EventDate") == F.col("LastEventDate"), coalesce_str("AUD_PHPValue")).otherwise(F.lit("")),
            F.when(F.col("CRA_EventDate") == F.col("LastEventDate"), coalesce_str("CRA_PHPValue")).otherwise(F.lit(""))
        )
    )
)

debug_show(derived_df, "derived_df", 10)


# ============================================================
# STEP 7: RULE ENGINE
# ============================================================
log.info("Applying rule engine")

all_vals = F.coalesce(F.col("AllPHPValues"), F.lit(""))
last_vals = F.coalesce(F.col("LastEventPHPValues"), F.lit(""))

all_first_char = F.substring(all_vals, 1, 1)
last_first_char = F.substring(last_vals, 1, 1)

all_same_expr = (F.regexp_replace(all_vals, all_first_char, "") == "")
last_same_expr = (F.regexp_replace(last_vals, last_first_char, "") == "")

final_df = (
    derived_df
    .withColumn(
        "HasConflicts",
        F.when(F.length(all_vals) > 1, F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "CalculatedPHP",
        F.when(all_vals == "", F.lit(""))
         .when(all_same_expr, all_first_char)
         .when(F.col("CRA_PHPValue").isNotNull(), F.col("CRA_PHPValue"))
         .when(
             F.length(last_vals) > 0,
             F.when(last_same_expr, last_first_char).otherwise(F.lit("D"))
         )
         .otherwise(F.lit("?"))
    )
    .withColumn(
        "RuleAppliedDescription",
        F.when(all_vals == "", F.lit("1 - No Data"))
         .when(all_same_expr, F.lit("2 - Agreement"))
         .when(F.col("CRA_PHPValue").isNotNull(), F.lit("3 - CRA Value"))
         .when(
             F.length(last_vals) > 0,
             F.when(last_same_expr, F.lit("4 - Latest Event (No Conflicts)"))
              .otherwise(F.lit("5 - Latest Event (With Conflicts)"))
         )
         .otherwise(F.lit("?"))
    )
)

debug_show(final_df, "final_df", 20)


# ============================================================
# STEP 8: FINAL OUTPUT
# ============================================================
log.info("Preparing final output DataFrame")

output_df = final_df.select(
    "AccountNumber",
    "DateOpened",
    "FileID",
    "BaseID",
    "ModificationID",
    "DateOfAccountInformation",
    "TransmitDate",
    "PHPDate",

    "CX_PHPValue",
    "CX_EventDate",

    F.when(F.col("CValue_ACDV") == 1,
           F.concat(F.col("ACDV_PHPValue"), F.lit("**")))
     .otherwise(F.col("ACDV_PHPValue")).alias("ACDV_PHPValue"),
    "ACDV_EventDate",
    "ACDV_ID",

    F.when(F.col("CValue_AUD") == 1,
           F.concat(F.col("AUD_PHPValue"), F.lit("**")))
     .otherwise(F.col("AUD_PHPValue")).alias("AUD_PHPValue"),
    "AUD_EventDate",
    "AUD_ID",

    F.when(F.col("CValue_CRA") == 1,
           F.concat(F.col("CRA_PHPValue"), F.lit("**")))
     .otherwise(F.col("CRA_PHPValue")).alias("CRA_PHPValue"),
    "CRA_EventDate",
    "CRA_ID",

    "HasConflicts",
    "CalculatedPHP",
    "RuleAppliedDescription"
)

debug_show(output_df, "output_df", 20)


# ============================================================
# STEP 9: CREATE TARGET TABLE
# ============================================================
log.info("Creating target table if not exists: %s", TARGET_TABLE)

spark.sql("""
CREATE TABLE IF NOT EXISTS {0} (
    AccountNumber STRING,
    DateOpened DATE,
    FileID STRING,
    BaseID STRING,
    ModificationID STRING,
    DateOfAccountInformation DATE,
    TransmitDate DATE,
    PHPDate DATE,
    CX_PHPValue STRING,
    CX_EventDate DATE,
    ACDV_PHPValue STRING,
    ACDV_EventDate DATE,
    ACDV_ID STRING,
    AUD_PHPValue STRING,
    AUD_EventDate DATE,
    AUD_ID STRING,
    CRA_PHPValue STRING,
    CRA_EventDate DATE,
    CRA_ID STRING,
    HasConflicts INT,
    CalculatedPHP STRING,
    RuleAppliedDescription STRING
)
STORED AS PARQUET
""".format(TARGET_TABLE))


# ============================================================
# STEP 10: WRITE TARGET TABLE
# ============================================================
log.info("Writing data into target table")

output_df.write.mode("overwrite").insertInto(TARGET_TABLE)

log.info("Write completed successfully")


# ============================================================
# STEP 11: VALIDATION
# ============================================================
target_count = spark.table(TARGET_TABLE).count()
log.info("Target table count: %s", target_count)

log.info("RID352 DSAS job completed successfully")

spark.stop()
