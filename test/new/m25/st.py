# -*- coding: utf-8 -*-
from __future__ import print_function

import time
from pyspark.sql import SparkSession


# ============================================================
# CONFIG / FLAGS
# ============================================================
CLEAR_EXISTING_DATA_ON_LOAD = True
APPLY_TABLE_STATS = True
APPLY_COLUMN_STATS = True
APPLY_PARTITION_STATS = True

# Optional:
# If table is present here, only these columns will be analyzed.
# If table not present, all non-partition columns will be analyzed dynamically.
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
# Replace this with your project logger if already available.
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
# UTILITY FUNCTIONS
# ============================================================
def table_exists(spark, table_name):
    try:
        return spark._jsparkSession.catalog().tableExists(table_name)
    except Exception:
        try:
            db_name, tbl_name = table_name.split(".", 1)
            return spark.catalog.tableExists(db_name, tbl_name)
        except Exception:
            return False


def get_table_columns_metadata(spark, table_name):
    """
    Returns list of column metadata objects from Spark catalog.
    """
    return spark.catalog.listColumns(table_name)


def get_partition_columns(spark, table_name):
    cols = get_table_columns_metadata(spark, table_name)
    return [c.name for c in cols if getattr(c, "isPartition", False)]


def get_non_partition_columns(spark, table_name):
    cols = get_table_columns_metadata(spark, table_name)
    return [c.name for c in cols if not getattr(c, "isPartition", False)]


def get_stats_columns_for_table(spark, table_name):
    """
    Priority:
    1. Take columns from TABLE_STATS_CONFIG if available
    2. Else dynamically take all non-partition columns
    """
    cfg = TABLE_STATS_CONFIG.get(table_name, {})
    cfg_cols = cfg.get("columns")

    if cfg_cols:
        actual_cols = set([c.lower() for c in get_non_partition_columns(spark, table_name)])
        valid_cols = [c for c in cfg_cols if c.lower() in actual_cols]

        if valid_cols:
            return valid_cols
        else:
            log.warning("Configured columns not found for %s. Falling back to dynamic non-partition columns.", table_name)

    return get_non_partition_columns(spark, table_name)


def get_partition_specs(spark, table_name):
    """
    Returns partition specs like:
      ['php_month=202503', 'php_month=202504']
    or for multi partitions:
      ['year=2025/month=03/day=28']
    """
    try:
        rows = spark.sql("SHOW PARTITIONS {0}".format(table_name)).collect()
        return [r[0] for r in rows]
    except Exception as e:
        log.warning("Unable to fetch partitions for %s: %s", table_name, str(e))
        return []


def quote_partition_value(value):
    """
    Safe quote for string partition values.
    """
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "\\'") + "'"


def build_partition_spec_sql(part_spec):
    """
    Converts SHOW PARTITIONS output into SQL-safe partition clause.

    Input examples:
      php_month=202503
      php_month=2025-03
      year=2025/month=03/day=28

    Output examples:
      php_month='202503'
      php_month='2025-03'
      year='2025',month='03',day='28'
    """
    items = []
    for token in str(part_spec).split("/"):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        items.append("{0}={1}".format(key.strip(), quote_partition_value(value.strip())))
    return ",".join(items)


def apply_table_stats(spark, table_name):
    log.info("Applying table stats for %s", table_name)
    spark.sql("ANALYZE TABLE {0} COMPUTE STATISTICS".format(table_name))
    log.info("Table stats applied successfully for %s", table_name)


def apply_column_stats(spark, table_name, columns):
    if not columns:
        log.info("No eligible columns found for column stats in %s", table_name)
        return

    col_str = ",".join(columns)
    log.info("Applying column stats for %s on columns: %s", table_name, col_str)
    spark.sql("ANALYZE TABLE {0} COMPUTE STATISTICS FOR COLUMNS {1}".format(table_name, col_str))
    log.info("Column stats applied successfully for %s", table_name)


def apply_partition_stats(spark, table_name):
    partition_cols = get_partition_columns(spark, table_name)

    if not partition_cols:
        log.info("Table %s is not partitioned. Skipping partition stats.", table_name)
        return

    parts = get_partition_specs(spark, table_name)

    if not parts:
        log.info("No partitions found in %s", table_name)
        return

    log.info("Total partitions found in %s = %s", table_name, len(parts))

    for part_spec in parts:
        part_sql = build_partition_spec_sql(part_spec)
        log.info("Applying partition stats for %s partition (%s)", table_name, part_sql)
        spark.sql(
            "ANALYZE TABLE {0} PARTITION ({1}) COMPUTE STATISTICS".format(
                table_name, part_sql
            )
        )

    log.info("Partition stats applied successfully for %s", table_name)


def apply_stats_dynamically(spark, table_name):
    """
    Applies:
      1. table stats
      2. column stats
      3. partition stats
    without hardcoding table / columns / partition names.
    """
    if not table_exists(spark, table_name):
        log.warning("Table does not exist: %s", table_name)
        return

    log.info("==========================================================")
    log.info("STATS STARTED FOR TABLE: %s", table_name)
    log.info("==========================================================")

    start_ts = time.time()

    try:
        if APPLY_TABLE_STATS:
            apply_table_stats(spark, table_name)

        if APPLY_COLUMN_STATS:
            stats_columns = get_stats_columns_for_table(spark, table_name)
            apply_column_stats(spark, table_name, stats_columns)

        if APPLY_PARTITION_STATS:
            apply_partition_stats(spark, table_name)

        end_ts = time.time()
        total_secs = int(end_ts - start_ts)

        log.info("==========================================================")
        log.info("STATS COMPLETED FOR TABLE: %s", table_name)
        log.info("TOTAL TIME TAKEN: %s seconds", total_secs)
        log.info("==========================================================")

    except Exception as e:
        log.error("Error while applying stats for %s: %s", table_name, str(e))
        raise


def cleanup_and_apply_stats(spark, tgt_table):
    """
    Main wrapper for one target table.
    """
    log.info("==========================================================")
    log.info("PROCESS STARTED FOR TABLE: %s", tgt_table)
    log.info("==========================================================")

    if CLEAR_EXISTING_DATA_ON_LOAD:
        log.info("Cleanup enabled for %s", tgt_table)
    else:
        log.info("Cleanup not enabled for %s", tgt_table)

    if table_exists(spark, tgt_table):
        apply_stats_dynamically(spark, tgt_table)
    else:
        log.warning("Skipping stats because table not found: %s", tgt_table)

    log.info("==========================================================")
    log.info("PROCESS COMPLETED FOR TABLE: %s", tgt_table)
    log.info("==========================================================")


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    spark = SparkSession.builder.enableHiveSupport().appName("dynamic_table_stats").getOrCreate()

    # Example 1: single target table from your process
    tgt_table = "dsas_conformed.rid352_recommendedphp"
    cleanup_and_apply_stats(spark, tgt_table)

    # Example 2: multiple tables
    table_list = [
        "dsas_conformed.rid352_recommendedphp",
        "dsas_conformed.rid352_recommendedphp_cx6",
        "dsas_conformed.rid352_recommendedphp_acdv",
        "dsas_conformed.rid352_recommendedphp_aud",
        "dsas_conformed.rid352_recommendedphp_cra"
    ]

    for table_name in table_list:
        cleanup_and_apply_stats(spark, table_name)

    spark.stop()
