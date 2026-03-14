# -*- coding: utf-8 -*-
from __future__ import print_function

import os
import sys
import time
import argparse
import logging
import subprocess
from datetime import datetime
from pyspark.sql import SparkSession


# ============================================================
# ARGUMENTS
# ============================================================
parser = argparse.ArgumentParser(description="AUD / ACDV HQL Driver")

parser.add_argument("--app_id", required=True, help="Application ID")
parser.add_argument("--app_name", required=True, help="Application Name")
parser.add_argument("--mail_id", required=True, help="Mail ID")
parser.add_argument("--process_type", required=True, help="daily / adhoc")
parser.add_argument("--entity_name", required=True, help="AUD / ACDV")

args = parser.parse_args()

app_id = args.app_id
app_name = args.app_name
mail_id = args.mail_id
process_type = args.process_type.strip().lower()
entity_name = args.entity_name.strip().upper()

if entity_name not in ["AUD", "ACDV"]:
    raise Exception("Invalid entity_name: {0}. Use AUD / ACDV".format(entity_name))

if process_type not in ["daily", "adhoc"]:
    raise Exception("Invalid process_type: {0}. Use daily / adhoc".format(process_type))


# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s PHP_DRIVER - %(message)s"
)
log = logging.getLogger("php_driver")


# ============================================================
# CONFIG
# ============================================================
CLEAR_EXISTING_DATA_ON_LOAD = True
REBUILD_INDEX = False

AUD_HQL_FILE = "/apps/hql/aud_php_select.hql"
ACDV_HQL_FILE = "/apps/hql/acdv_php_select.hql"

ADHOC_WINDOW_FILE = "/apps/config/adhoc_window.txt"
FIRST_RUN_FOLDER = "/apps/control/first_run"

TABLE_CONFIGS = [
    {
        "name": "AUD",
        "hql_file": AUD_HQL_FILE,
        "src_table": "hive_dsas_fnsh_sanitized.eoscar_aud_hist",
        "src_process_col": "process_date",
        "tgt_table": "dsas_conformed.crcoe_non_cx6_php_aud_table",
        "tgt_location": "/apps/hive/external/dsas_conformed/crcoe_non_cx6_php_aud_table",
        "target_id_col": "AUD_ID",
        "index_name": "idx_accountnumber_aud",
        "expected_columns": [
            "AccountNumber",
            "AUD_ID",
            "EventDate",
            "PHPDate",
            "PHPValue",
            "report_date",
            "php_month"
        ]
    },
    {
        "name": "ACDV",
        "hql_file": ACDV_HQL_FILE,
        "src_table": "hive_dsas_fnsh_sanitized.eoscar_acdvarchive_hist",
        "src_process_col": "process_date",
        "tgt_table": "dsas_conformed.crcoe_non_cx6_php_acdv_table",
        "tgt_location": "/apps/hive/external/dsas_conformed/crcoe_non_cx6_php_acdv_table",
        "target_id_col": "ACDV_ID",
        "index_name": "idx_accountnumber_acdv",
        "expected_columns": [
            "AccountNumber",
            "ACDV_ID",
            "EventDate",
            "PHPDate",
            "PHPValue",
            "report_date",
            "php_month"
        ]
    }
]


# ============================================================
# SPARK SESSION
# ============================================================
spark = (
    SparkSession.builder
    .appName("CRCOE_PHP_DRIVER_{0}_{1}_{2}".format(app_id, app_name, entity_name))
    .enableHiveSupport()
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")


# ============================================================
# DRIVER START TIME
# ============================================================
driver_start_dt = datetime.now()
driver_start_ts = time.time()

log.info("#################### PHP DRIVER START ######################")
log.info("Driver start time = %s", driver_start_dt.strftime("%Y-%m-%d %H:%M:%S"))
log.info("app_id = %s", app_id)
log.info("app_name = %s", app_name)
log.info("mail_id = %s", mail_id)
log.info("process_type = %s", process_type)
log.info("entity_name = %s", entity_name)
log.info("CLEAR_EXISTING_DATA_ON_LOAD = %s", CLEAR_EXISTING_DATA_ON_LOAD)
log.info("REBUILD_INDEX = %s", REBUILD_INDEX)


# ============================================================
# PICK ONLY ONE ENTITY
# ============================================================
selected_cfg = None

for cfg in TABLE_CONFIGS:
    if cfg["name"] == entity_name:
        selected_cfg = cfg
        break

if selected_cfg is None:
    raise Exception("Configuration not found for entity_name = {0}".format(entity_name))


# ============================================================
# ENTITY START TIME
# ============================================================
entity_start_dt = datetime.now()
entity_start_ts = time.time()

log.info("====================================================")
log.info("Starting process for %s", selected_cfg["name"])
log.info("Process start time for %s = %s", selected_cfg["name"], entity_start_dt.strftime("%Y-%m-%d %H:%M:%S"))
log.info("====================================================")

hql_file = selected_cfg["hql_file"]
src_table = selected_cfg["src_table"]
src_process_col = selected_cfg["src_process_col"]
tgt_table = selected_cfg["tgt_table"]
tgt_location = selected_cfg["tgt_location"]
target_id_col = selected_cfg["target_id_col"]
index_name = selected_cfg["index_name"]
expected_columns = selected_cfg["expected_columns"]


# ============================================================
# CREATE TARGET TABLE IF NOT EXISTS
# ============================================================
log.info("Ensuring external table exists: %s", tgt_table)

create_sql = """
CREATE EXTERNAL TABLE IF NOT EXISTS {0}(
    AccountNumber BIGINT,
    {1} STRING,
    EventDate DATE,
    PHPDate DATE,
    PHPValue STRING,
    report_date TIMESTAMP
)
PARTITIONED BY (php_month STRING)
STORED AS PARQUET
LOCATION '{2}'
""".format(tgt_table, target_id_col, tgt_location)

spark.sql(create_sql)

log.info("External table validation completed: %s", tgt_table)


# ============================================================
# DAILY MODE - FIRST RUN / BAU CHECK
# ============================================================
skip_process = False
first_run_timestamp = None

if process_type == "daily":
    first_run_file = os.path.join(FIRST_RUN_FOLDER, tgt_table)

    log.info("Checking first-run file path: %s", first_run_file)

    if os.path.exists(first_run_file):
        log.info("First-run file exists for %s", entity_name)

        with open(first_run_file, "r") as f:
            first_run_timestamp = f.read().strip()

        if not first_run_timestamp:
            raise Exception("First-run file is empty for {0}".format(tgt_table))

        log.info("First-run timestamp = %s", first_run_timestamp)
        log.info("Skipping max(source) vs max(target) check because first-run file is available")

    else:
        log.info("First-run file not found. Proceeding with BAU max timestamp check")

        src_max = None
        tgt_max = None

        src_sql = "select max({0}) as max_val from {1}".format(src_process_col, src_table)
        log.info("Executing source max query: %s", src_sql)
        src_max = spark.sql(src_sql).collect()[0]["max_val"]

        if spark.catalog.tableExists(tgt_table):
            tgt_sql = "select max(report_date) as max_val from {0}".format(tgt_table)
            log.info("Executing target max query: %s", tgt_sql)
            tgt_max = spark.sql(tgt_sql).collect()[0]["max_val"]
        else:
            log.info("Target table does not exist for max(report_date) check: %s", tgt_table)
            tgt_max = None

        log.info("Source max(%s) = %s", src_process_col, src_max)
        log.info("Target max(report_date) = %s", tgt_max)

        if src_max is None:
            log.info("Source max is null. No data available. Skipping process.")
            skip_process = True

        elif tgt_max is not None and str(src_max) == str(tgt_max):
            log.info("Source max and target max are same. No new data available. Skipping process.")
            skip_process = True

        else:
            log.info("New data detected. Process will continue.")


# ============================================================
# PROCESS CONTINUE
# ============================================================
if not skip_process:

    # --------------------------------------------------------
    # READ HQL FILE
    # --------------------------------------------------------
    log.info("Reading HQL file: %s", hql_file)

    with open(hql_file, "r") as f:
        hql_text = f.read()

    if not hql_text or not hql_text.strip():
        raise Exception("HQL file is empty: {0}".format(hql_file))

    log.info("HQL file read completed for %s", entity_name)

    # --------------------------------------------------------
    # DELTA REPLACEMENT
    # --------------------------------------------------------
    log.info("Applying delta replacement for %s", entity_name)

    if process_type == "adhoc":
        log.info("ADHOC mode detected. Reading adhoc window file: %s", ADHOC_WINDOW_FILE)

        start_dt = None
        end_dt = None

        with open(ADHOC_WINDOW_FILE, "r") as f:
            for line in f:
                line = line.strip()

                if not line:
                    continue

                if line.lower().startswith("start_dt="):
                    start_dt = line.split("=", 1)[1].strip()

                if line.lower().startswith("end_dt="):
                    end_dt = line.split("=", 1)[1].strip()

        if not start_dt or not end_dt:
            raise Exception("Invalid adhoc file. start_dt or end_dt missing in {0}".format(ADHOC_WINDOW_FILE))

        replacement = "between '{0}' and '{1}'".format(start_dt, end_dt)

        log.info("ADHOC start_dt = %s", start_dt)
        log.info("ADHOC end_dt = %s", end_dt)
        log.info("Replacement string = %s", replacement)

        hql_text = hql_text.replace("'INPUT_DELTA_COL'", replacement)

    else:
        log.info("DAILY mode detected for %s", entity_name)

        if first_run_timestamp:
            replacement = "> '{0}'".format(first_run_timestamp)
            log.info("Daily first-run replacement string = %s", replacement)
            hql_text = hql_text.replace("'INPUT_DELTA_COL'", replacement)
        else:
            replacement = "> (select max(report_date) from {0})".format(tgt_table)
            log.info("Daily BAU replacement string = %s", replacement)
            hql_text = hql_text.replace("'INPUT_DELTA_COL'", replacement)

    # --------------------------------------------------------
    # DISPLAY FINAL HQL AFTER REPLACEMENT
    # --------------------------------------------------------
    log.info("Final HQL after replacement for %s:\n%s", entity_name, hql_text)

    # --------------------------------------------------------
    # EXECUTE HQL
    # --------------------------------------------------------
    log.info("Executing spark.sql() for %s", entity_name)

    df = spark.sql(hql_text)

    log.info("Counting output dataframe for %s", entity_name)
    df_count = df.count()
    log.info("Output dataframe count for %s = %s", entity_name, df_count)

    # --------------------------------------------------------
    # IF NO DATA
    # --------------------------------------------------------
    if df_count == 0:
        log.info("No records returned from HQL for %s. Skipping cleanup and load.", entity_name)

    else:
        # ----------------------------------------------------
        # CLEAN EXISTING DATA
        # ----------------------------------------------------
        if CLEAR_EXISTING_DATA_ON_LOAD:
            log.info("Cleanup enabled. Dropping Hive partitions for %s", tgt_table)

            if spark.catalog.tableExists(tgt_table):
                try:
                    parts = spark.sql("SHOW PARTITIONS {0}".format(tgt_table)).collect()

                    if parts:
                        log.info("Total partitions found in %s = %s", tgt_table, len(parts))

                        for row in parts:
                            part_spec = row[0]
                            spark.sql(
                                "ALTER TABLE {0} DROP IF EXISTS PARTITION ({1})".format(tgt_table, part_spec)
                            )

                        log.info("All Hive partitions dropped successfully for %s", tgt_table)

                    else:
                        log.info("No partitions found to drop in %s", tgt_table)

                except Exception as e:
                    log.info("Hive partition drop skipped/failed for %s : %s", tgt_table, str(e))
            else:
                log.info("Target table does not exist during partition drop step: %s", tgt_table)

            log.info("Deleting HDFS folder if exists: %s", tgt_location)

            cmd_test = "hdfs dfs -test -e {0}".format(tgt_location)
            rc_test = subprocess.call(cmd_test, shell=True)

            if rc_test == 0:
                cmd_rm = "hdfs dfs -rm -r -skipTrash {0}".format(tgt_location)
                rc_rm = subprocess.call(cmd_rm, shell=True)

                if rc_rm == 0:
                    log.info("HDFS folder deleted successfully: %s", tgt_location)
                else:
                    log.info("HDFS folder delete command returned non-zero for: %s", tgt_location)
            else:
                log.info("HDFS folder does not exist. Skipping delete for: %s", tgt_location)

            log.info("Recreating external table metadata after cleanup for %s", tgt_table)
            spark.sql(create_sql)

        else:
            log.info("Cleanup disabled. Existing data will be retained for %s", tgt_table)

        # ----------------------------------------------------
        # VALIDATE OUTPUT COLUMNS
        # ----------------------------------------------------
        log.info("Validating output columns for %s", entity_name)

        actual_columns = df.columns

        if actual_columns != expected_columns:
            raise Exception(
                "{0} output columns mismatch.\nExpected: {1}\nActual  : {2}".format(
                    entity_name, expected_columns, actual_columns
                )
            )

        log.info("Output columns validated successfully for %s", entity_name)

        # ----------------------------------------------------
        # LOAD DATA
        # ----------------------------------------------------
        log.info("Loading data into target table: %s", tgt_table)

        (
            df
            .write
            .mode("append")
            .format("hive")
            .insertInto(tgt_table)
        )

        log.info("Data load completed successfully for %s", entity_name)

        # ----------------------------------------------------
        # OPTIONAL INDEX REBUILD
        # ----------------------------------------------------
        if REBUILD_INDEX:
            log.info("Index rebuild enabled for %s", entity_name)

            try:
                spark.sql("""
                CREATE INDEX IF NOT EXISTS {0}
                ON TABLE {1}(AccountNumber)
                AS 'COMPACT'
                """.format(index_name, tgt_table))

                spark.sql("""
                ALTER INDEX {0} ON {1} REBUILD
                """.format(index_name, tgt_table))

                log.info("Index rebuild completed for %s", entity_name)

            except Exception as e:
                log.info("Index rebuild skipped/failed for %s : %s", entity_name, str(e))
        else:
            log.info("Index rebuild disabled for %s", entity_name)

else:
    log.info("Process skipped for %s", entity_name)


# ============================================================
# ENTITY END TIME
# ============================================================
entity_end_dt = datetime.now()
entity_end_ts = time.time()

entity_elapsed_sec = int(entity_end_ts - entity_start_ts)
entity_hh = entity_elapsed_sec // 3600
entity_mm = (entity_elapsed_sec % 3600) // 60
entity_ss = entity_elapsed_sec % 60

log.info("Process end time for %s = %s", entity_name, entity_end_dt.strftime("%Y-%m-%d %H:%M:%S"))
log.info(
    "Total execution time for %s = %02d:%02d:%02d",
    entity_name, entity_hh, entity_mm, entity_ss
)


# ============================================================
# DRIVER END TIME
# ============================================================
driver_end_dt = datetime.now()
driver_end_ts = time.time()

driver_elapsed_sec = int(driver_end_ts - driver_start_ts)
driver_hh = driver_elapsed_sec // 3600
driver_mm = (driver_elapsed_sec % 3600) // 60
driver_ss = driver_elapsed_sec % 60

log.info("Driver end time = %s", driver_end_dt.strftime("%Y-%m-%d %H:%M:%S"))
log.info("Overall execution time = %02d:%02d:%02d", driver_hh, driver_mm, driver_ss)
log.info("#################### PHP DRIVER END ######################")
