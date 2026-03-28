# -*- coding: utf-8 -*-
from __future__ import print_function

import time
from pyspark.sql import SparkSession

# ============================================================
# FLAGS
# ============================================================
CLEAR_EXISTING_DATA_ON_LOAD = True
APPLY_TABLE_STATS = True
APPLY_COLUMN_STATS = True
APPLY_PARTITION_STATS = True

# Optional config:
# If columns are given here, only those valid columns will be used.
# Else all non-partition columns will be taken dynamically.
TABLE_STATS_CONFIG = {
    "dsas_conformed.rid352_recommendedphp": {
        "columns": ["AccountNumber", "report_date"]
    },
    "dsas_conformed.rid352_recommendedphp_cx6": {
        "columns": ["AccountNumber", "report_date", "bucket_id"]
    },
    "dsas_conformed.rid352_recommendedphp_acdv": {
        "columns": ["AccountNumber", "report_date"]
    },
    "dsas_conformed.rid352_recommendedphp_aud": {
        "columns": ["AccountNumber", "report_date"]
    },
    "dsas_conformed.rid352_recommendedphp_cra": {
        "columns": ["AccountNumber", "report_date"]
    }
}


# ============================================================
# LOGGER
# Replace with your existing logger if already available
# ============================================================
class SimpleLogger(object):
    def info(self, msg, *args):
        if args:
            print("INFO - " + (msg % args))
        else:
            print("INFO - " + str(msg))

    def error(self, msg, *args):
        if args:
            print("ERROR - " + (msg % args))
        else:
            print("ERROR - " + str(msg))

    def warning(self, msg, *args):
        if args:
            print("WARN - " + (msg % args))
        else:
            print("WARN - " + str(msg))


log = SimpleLogger()


# ============================================================
# COMMON HELPERS
# ============================================================
def split_table_name(full_table_name):
    """
    Splits db.table into (db_name, tbl_name)
    """
    if "." in full_table_name:
        db_name, tbl_name = full_table_name.split(".", 1)
        return db_name.strip(), tbl_name.strip()
    return None, full_table_name.strip()


def get_qualified_table_name(full_table_name):
    db_name, tbl_name = split_table_name(full_table_name)
    if db_name:
        return "{0}.{1}".format(db_name, tbl_name)
    return tbl_name


def table_exists(spark, full_table_name):
    db_name, tbl_name = split_table_name(full_table_name)
    try:
        if db_name:
            return spark.catalog.tableExists(tbl_name, db_name)
        else:
            return spark.catalog.tableExists(tbl_name)
    except Exception:
        try:
            return spark._jsparkSession.catalog().tableExists(full_table_name)
        except Exception:
            return False


def get_table_columns_metadata(spark, full_table_name):
    """
    Spark 2.4 safe version
    """
    db_name, tbl_name = split_table_name(full_table_name)
    if db_name:
        return spark.catalog.listColumns(tbl_name, db_name)
    return spark.catalog.listColumns(tbl_name)


def get_partition_columns(spark, full_table_name):
    cols = get_table_columns_metadata(spark, full_table_name)
    return [c.name for c in cols if getattr(c, "isPartition", False)]


def get_non_partition_columns(spark, full_table_name):
    cols = get_table_columns_metadata(spark, full_table_name)
    return [c.name for c in cols if not getattr(c, "isPartition", False)]


def get_stats_columns_for_table(spark, full_table_name):
    cfg = TABLE_STATS_CONFIG.get(full_table_name, {})
    cfg_cols = cfg.get("columns")

    actual_cols = set([c.lower() for c in get_non_partition_columns(spark, full_table_name)])

    if cfg_cols:
        valid_cols = [c for c in cfg_cols if c.lower() in actual_cols]
        if valid_cols:
            return valid_cols
        else:
            log.warning("Configured columns not found for %s. Falling back to dynamic columns.", full_table_name)

    return get_non_partition_columns(spark, full_table_name)


def get_partition_specs(spark, full_table_name):
    qualified_name = get_qualified_table_name(full_table_name)
    try:
        rows = spark.sql("SHOW PARTITIONS {0}".format(qualified_name)).collect()
        return [r[0] for r in rows]
    except Exception as e:
        log.warning("Unable to fetch partitions for %s: %s", full_table_name, str(e))
        return []


def quote_partition_value(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "\\'") + "'"


def build_partition_spec_sql(part_spec):
    """
    Converts:
      php_month=202503
      year=2025/month=03/day=28
    into:
      php_month='202503'
      year='2025',month='03',day='28'
    """
    items = []
    for token in str(part_spec).split("/"):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        items.append("{0}={1}".format(key.strip(), quote_partition_value(value.strip())))
    return ",".join(items)


# ============================================================
# STATS FUNCTIONS
# ============================================================
def apply_table_stats(spark, full_table_name):
    qualified_name = get_qualified_table_name(full_table_name)
    log.info("Applying table stats for %s", qualified_name)
    spark.sql("ANALYZE TABLE {0} COMPUTE STATISTICS".format(qualified_name))
    log.info("Table stats applied successfully for %s", qualified_name)


def apply_column_stats(spark, full_table_name, columns):
    qualified_name = get_qualified_table_name(full_table_name)

    if not columns:
        log.info("No eligible columns found for column stats in %s", qualified_name)
        return

    col_str = ",".join(columns)
    log.info("Applying column stats for %s on columns: %s", qualified_name, col_str)
    spark.sql(
        "ANALYZE TABLE {0} COMPUTE STATISTICS FOR COLUMNS {1}".format(
            qualified_name, col_str
        )
    )
    log.info("Column stats applied successfully for %s", qualified_name)


def apply_partition_stats(spark, full_table_name):
    qualified_name = get_qualified_table_name(full_table_name)
    partition_cols = get_partition_columns(spark, full_table_name)

    if not partition_cols:
        log.info("Table %s is not partitioned. Skipping partition stats.", qualified_name)
        return

    parts = get_partition_specs(spark, full_table_name)

    if not parts:
        log.info("No partitions found in %s", qualified_name)
        return

    log.info("Total partitions found in %s = %s", qualified_name, len(parts))

    for part_spec in parts:
        part_sql = build_partition_spec_sql(part_spec)
        log.info("Applying partition stats for %s partition (%s)", qualified_name, part_sql)
        spark.sql(
            "ANALYZE TABLE {0} PARTITION ({1}) COMPUTE STATISTICS".format(
                qualified_name, part_sql
            )
        )

    log.info("Partition stats applied successfully for %s", qualified_name)


def apply_stats_dynamically(spark, full_table_name):
    qualified_name = get_qualified_table_name(full_table_name)

    if not table_exists(spark, full_table_name):
        log.warning("Table does not exist: %s", qualified_name)
        return

    start_ts = time.time()

    log.info("==========================================================")
    log.info("STATS STARTED FOR TABLE: %s", qualified_name)
    log.info("==========================================================")

    try:
        if APPLY_TABLE_STATS:
            apply_table_stats(spark, full_table_name)

        if APPLY_COLUMN_STATS:
            stats_columns = get_stats_columns_for_table(spark, full_table_name)
            apply_column_stats(spark, full_table_name, stats_columns)

        if APPLY_PARTITION_STATS:
            apply_partition_stats(spark, full_table_name)

        total_secs = int(time.time() - start_ts)

        log.info("==========================================================")
        log.info("STATS COMPLETED FOR TABLE: %s", qualified_name)
        log.info("TOTAL TIME TAKEN: %s seconds", total_secs)
        log.info("==========================================================")

    except Exception as e:
        log.error("Error while applying stats for %s: %s", qualified_name, str(e))
        raise


# ============================================================
# MAIN WRAPPER
# ============================================================
def cleanup_and_apply_stats(spark, tgt_table):
    qualified_name = get_qualified_table_name(tgt_table)

    log.info("==========================================================")
    log.info("PROCESS STARTED FOR TABLE: %s", qualified_name)
    log.info("==========================================================")

    if CLEAR_EXISTING_DATA_ON_LOAD:
        log.info("Cleanup enabled for %s", qualified_name)
    else:
        log.info("Cleanup not enabled for %s", qualified_name)

    if table_exists(spark, tgt_table):
        apply_stats_dynamically(spark, tgt_table)
    else:
        log.warning("Skipping stats because table not found: %s", qualified_name)

    log.info("==========================================================")
    log.info("PROCESS COMPLETED FOR TABLE: %s", qualified_name)
    log.info("==========================================================")


# ============================================================
# USAGE
# ============================================================
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("dynamic_stats_spark24") \
        .enableHiveSupport() \
        .getOrCreate()

    # Single table
    tgt_table = "dsas_conformed.rid352_recommendedphp"
    cleanup_and_apply_stats(spark, tgt_table)

    # Multiple tables
    table_list = [
        "dsas_conformed.rid352_recommendedphp",
        "dsas_conformed.rid352_recommendedphp_cx6",
        "dsas_conformed.rid352_recommendedphp_acdv",
        "dsas_conformed.rid352_recommendedphp_aud",
        "dsas_conformed.rid352_recommendedphp_cra"
    ]

    for full_table_name in table_list:
        cleanup_and_apply_stats(spark, full_table_name)

    spark.stop()
