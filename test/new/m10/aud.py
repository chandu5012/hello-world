# -*- coding: utf-8 -*-
from __future__ import print_function

import os
import sys
import argparse
import logging
import subprocess
from pyspark.sql import SparkSession


############################################################
# LOGGING
############################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s PHP_DRIVER - %(message)s"
)

log = logging.getLogger("php_driver")


############################################################
# ARGUMENTS
############################################################

def parse_args():
    parser = argparse.ArgumentParser(description="AUD/ACDV HQL driver")

    parser.add_argument("--app_id", required=True, help="Application ID")
    parser.add_argument("--app_name", required=True, help="Application Name")
    parser.add_argument("--mail_id", required=True, help="Notification mail id")
    parser.add_argument("--process_type", required=True, help="daily / adhoc")
    parser.add_argument("--entity_name", required=True, help="AUD / ACDV / BOTH")

    return parser.parse_args()


ARGS = parse_args()

APP_ID = ARGS.app_id
APP_NAME = ARGS.app_name
MAIL_ID = ARGS.mail_id
PROCESS_TYPE = ARGS.process_type.strip().lower()
ENTITY_NAME = ARGS.entity_name.strip().upper()


############################################################
# CONFIG
############################################################

# Control flags
CLEAR_EXISTING_DATA_ON_LOAD = True
REBUILD_INDEX = False

# Files
AUD_HQL_FILE = "/apps/hql/aud_php_select.hql"
ACDV_HQL_FILE = "/apps/hql/acdv_php_select.hql"
ADHOC_WINDOW_FILE = "/apps/config/adhoc_window.txt"

# First-run control folder
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


############################################################
# SPARK SESSION
############################################################

spark = (
    SparkSession.builder
    .appName("CRCOE_PHP_DRIVER_{0}_{1}".format(APP_ID, APP_NAME))
    .enableHiveSupport()
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")


############################################################
# HELPER FUNCTIONS
############################################################

def read_text_file(path):
    log.info("Reading file: %s", path)
    with open(path, "r") as f:
        return f.read()


def read_adhoc_window(path):
    """
    File format:
    start_dt=2025-01-01 00:00:00
    end_dt=2025-01-10 23:59:59
    """
    log.info("Reading adhoc window file: %s", path)

    start_dt = None
    end_dt = None

    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            if line.lower().startswith("start_dt="):
                start_dt = line.split("=", 1)[1].strip()

            if line.lower().startswith("end_dt="):
                end_dt = line.split("=", 1)[1].strip()

    if not start_dt or not end_dt:
        raise Exception("Invalid adhoc window file. start_dt or end_dt missing.")

    log.info("Adhoc start_dt=%s, end_dt=%s", start_dt, end_dt)
    return start_dt, end_dt


def get_first_run_timestamp(table_name):
    """
    If file exists under FIRST_RUN_FOLDER/<table_name>, use that timestamp.
    """
    path = os.path.join(FIRST_RUN_FOLDER, table_name)

    if os.path.exists(path):
        log.info("First-run file exists for table: %s", table_name)
        ts = read_text_file(path).strip()
        log.info("First-run timestamp from file: %s", ts)
        return ts

    log.info("First-run file not found for table: %s", table_name)
    return None


def table_exists(table_name):
    return spark.catalog.tableExists(table_name)


def get_max_value(table_name, col_name):
    if not table_exists(table_name):
        log.info("Table does not exist: %s", table_name)
        return None

    sql_text = "select max({0}) as max_val from {1}".format(col_name, table_name)
    row = spark.sql(sql_text).collect()[0]
    return row["max_val"]


def create_external_table_if_not_exists(cfg):
    log.info("Ensuring external table exists: %s", cfg["tgt_table"])

    ddl = """
    CREATE EXTERNAL TABLE IF NOT EXISTS {table_name}(
        AccountNumber BIGINT,
        {id_col} STRING,
        EventDate DATE,
        PHPDate DATE,
        PHPValue STRING,
        report_date TIMESTAMP
    )
    PARTITIONED BY (php_month STRING)
    STORED AS PARQUET
    LOCATION '{location}'
    """.format(
        table_name=cfg["tgt_table"],
        id_col=cfg["target_id_col"],
        location=cfg["tgt_location"]
    )

    spark.sql(ddl)
    log.info("External table check completed: %s", cfg["tgt_table"])


def should_process_bau_daily(cfg):
    """
    Daily BAU:
    compare max(source.process_date) vs max(target.report_date)
    """
    log.info("Checking BAU daily max timestamp logic for %s", cfg["name"])

    src_max = get_max_value(cfg["src_table"], cfg["src_process_col"])
    tgt_max = get_max_value(cfg["tgt_table"], "report_date")

    log.info("%s source max(%s) = %s", cfg["name"], cfg["src_process_col"], src_max)
    log.info("%s target max(report_date) = %s", cfg["name"], tgt_max)

    if src_max is None:
        log.info("%s source max is null. Skipping process.", cfg["name"])
        return False

    if tgt_max is None:
        log.info("%s target is empty. Processing required.", cfg["name"])
        return True

    if str(src_max) == str(tgt_max):
        log.info("%s no new data found. Skipping process.", cfg["name"])
        return False

    log.info("%s new data found. Processing required.", cfg["name"])
    return True


def apply_delta_filter(hql_text, cfg):
    """
    Expected placeholder in HQL:
        > 'INPUT_DELTA_COL'

    process_type = adhoc
        -> between 'start_dt' and 'end_dt'

    process_type = daily and first-run file exists
        -> > 'first_run_timestamp'

    process_type = daily and no first-run file
        -> > (select max(report_date) from target_table)
    """
    log.info("Applying delta replacement for %s", cfg["name"])

    if PROCESS_TYPE == "adhoc":
        start_dt, end_dt = read_adhoc_window(ADHOC_WINDOW_FILE)
        replacement = "between '{0}' and '{1}'".format(start_dt, end_dt)
        log.info("%s adhoc replacement = %s", cfg["name"], replacement)

    else:
        first_run_ts = get_first_run_timestamp(cfg["tgt_table"])

        if first_run_ts:
            replacement = "> '{0}'".format(first_run_ts)
            log.info("%s daily first-run replacement = %s", cfg["name"], replacement)
        else:
            replacement = "> (select max(report_date) from {0})".format(cfg["tgt_table"])
            log.info("%s daily BAU replacement = %s", cfg["name"], replacement)

    hql_text = hql_text.replace("> 'INPUT_DELTA_COL'", replacement)
    hql_text = hql_text.replace("'INPUT_DELTA_COL'", replacement)

    return hql_text


def drop_all_hive_partitions(table_name):
    log.info("Dropping Hive partitions for table: %s", table_name)

    if not table_exists(table_name):
        log.info("Table does not exist. No partitions to drop: %s", table_name)
        return

    try:
        parts = spark.sql("SHOW PARTITIONS {0}".format(table_name)).collect()

        if not parts:
            log.info("No partitions found for table: %s", table_name)
            return

        for row in parts:
            part_spec = row[0]
            spark.sql(
                "ALTER TABLE {0} DROP IF EXISTS PARTITION ({1})".format(table_name, part_spec)
            )

        log.info("Dropped %s partitions from %s", len(parts), table_name)

    except Exception as e:
        log.info("Partition drop skipped/failed for %s : %s", table_name, str(e))


def delete_hdfs_path(path):
    log.info("Deleting target folder if exists: %s", path)

    cmd_test = "hdfs dfs -test -e {0}".format(path)
    rc = subprocess.call(cmd_test, shell=True)

    if rc == 0:
        cmd_rm = "hdfs dfs -rm -r -skipTrash {0}".format(path)
        rc_rm = subprocess.call(cmd_rm, shell=True)

        if rc_rm == 0:
            log.info("Deleted folder successfully: %s", path)
        else:
            log.info("Folder delete returned non-zero for: %s", path)
    else:
        log.info("Folder does not exist, skipping delete: %s", path)


def rebuild_index_if_enabled(cfg):
    if not REBUILD_INDEX:
        log.info("Index rebuild disabled for %s", cfg["name"])
        return

    log.info("Attempting index create/rebuild for %s", cfg["name"])

    try:
        spark.sql("""
        CREATE INDEX IF NOT EXISTS {idx_name}
        ON TABLE {table_name}(AccountNumber)
        AS 'COMPACT'
        """.format(
            idx_name=cfg["index_name"],
            table_name=cfg["tgt_table"]
        ))

        spark.sql("""
        ALTER INDEX {idx_name} ON {table_name} REBUILD
        """.format(
            idx_name=cfg["index_name"],
            table_name=cfg["tgt_table"]
        ))

        log.info("Index rebuild completed for %s", cfg["name"])

    except Exception as e:
        log.info("Index rebuild skipped/failed for %s : %s", cfg["name"], str(e))


def execute_select_hql(hql_text, cfg):
    log.info("Executing HQL for %s", cfg["name"])

    df = spark.sql(hql_text)

    log.info("%s HQL executed. Counting result dataframe...", cfg["name"])
    cnt = df.count()
    log.info("%s result count = %s", cfg["name"], cnt)

    return df, cnt


def validate_output_columns(df, cfg):
    actual = df.columns
    expected = cfg["expected_columns"]

    if actual != expected:
        raise Exception(
            "{0} output columns mismatch.\nExpected: {1}\nActual  : {2}".format(
                cfg["name"], expected, actual
            )
        )

    log.info("%s output columns validated successfully", cfg["name"])


def load_to_target(df, cfg):
    log.info("Loading data into target table: %s", cfg["tgt_table"])

    validate_output_columns(df, cfg)

    (
        df
        .write
        .mode("append")
        .format("hive")
        .insertInto(cfg["tgt_table"])
    )

    log.info("Load completed for %s", cfg["name"])


def select_configs():
    if ENTITY_NAME == "AUD":
        return [cfg for cfg in TABLE_CONFIGS if cfg["name"] == "AUD"]

    if ENTITY_NAME == "ACDV":
        return [cfg for cfg in TABLE_CONFIGS if cfg["name"] == "ACDV"]

    if ENTITY_NAME == "BOTH":
        return TABLE_CONFIGS

    raise Exception("Invalid entity_name: {0}. Use AUD / ACDV / BOTH".format(ENTITY_NAME))


############################################################
# PROCESS ONE ENTITY
############################################################

def process_one(cfg):
    log.info("====================================================")
    log.info("Starting process for %s", cfg["name"])
    log.info("app_id=%s, app_name=%s, mail_id=%s, process_type=%s, entity_name=%s",
             APP_ID, APP_NAME, MAIL_ID, PROCESS_TYPE, ENTITY_NAME)
    log.info("====================================================")

    # Ensure target exists first
    create_external_table_if_not_exists(cfg)

    # Daily BAU skip check only if:
    # process_type = daily
    # and first-run file does NOT exist
    if PROCESS_TYPE == "daily":
        first_run_ts = get_first_run_timestamp(cfg["tgt_table"])

        if not first_run_ts:
            if not should_process_bau_daily(cfg):
                log.info("%s skipped based on BAU daily max timestamp check", cfg["name"])
                return
        else:
            log.info("%s first-run file found. Max timestamp check skipped.", cfg["name"])

    # Read HQL
    hql_text = read_text_file(cfg["hql_file"])

    # Replace delta
    hql_text = apply_delta_filter(hql_text, cfg)

    # Execute HQL
    df, cnt = execute_select_hql(hql_text, cfg)

    # If no data, skip cleanup + load
    if cnt == 0:
        log.info("%s returned zero records. Skipping cleanup and load.", cfg["name"])
        return

    # Cleanup existing data only when result exists
    if CLEAR_EXISTING_DATA_ON_LOAD:
        log.info("%s cleanup enabled. Dropping partitions and deleting folder.", cfg["name"])
        drop_all_hive_partitions(cfg["tgt_table"])
        delete_hdfs_path(cfg["tgt_location"])
    else:
        log.info("%s cleanup disabled. Existing target data retained.", cfg["name"])

    # Recreate table after cleanup
    create_external_table_if_not_exists(cfg)

    # Load
    load_to_target(df, cfg)

    # Optional index rebuild
    rebuild_index_if_enabled(cfg)

    log.info("Completed process for %s", cfg["name"])


############################################################
# MAIN
############################################################

def main():
    log.info("#################### PHP DRIVER START ####################")
    log.info("app_id=%s", APP_ID)
    log.info("app_name=%s", APP_NAME)
    log.info("mail_id=%s", MAIL_ID)
    log.info("process_type=%s", PROCESS_TYPE)
    log.info("entity_name=%s", ENTITY_NAME)
    log.info("CLEAR_EXISTING_DATA_ON_LOAD=%s", CLEAR_EXISTING_DATA_ON_LOAD)
    log.info("REBUILD_INDEX=%s", REBUILD_INDEX)

    for cfg in select_configs():
        process_one(cfg)

    log.info("#################### PHP DRIVER END ######################")


if __name__ == "__main__":
    main()
