# ============================================================
# FINAL UPDATED PYSPARK SCRIPT (Spark 2.4/Python2.7 + Spark 3.3)
#
# Fixes:
# 1) Column order independent:
#    - add missing cols on both sides as NULL
#    - reorder both dataframes in SAME sorted column order
# 2) PK / NO-PK deterministic pairing (prevents duplicate records):
#    - if PK present: add __pk_row_num per PK group (stable pairing)
#    - if NO-PK: build __row_hash + __row_num using sorted compare cols
# 3) mismatchCount correct (won't become 110 for 10 fields)
# 4) NULL values appear in JSON as real null (not dropped / not empty)
# 5) Supports: trim_spaces, ignore_case, null_equals_empty, numeric_tolerance
# ============================================================

import os
import glob
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import types as T

spark = SparkSession.builder.appName("MismatchExplorer_Final_JSON_Compat").getOrCreate()

# -----------------------------
# INPUTS (from UI)
# -----------------------------
run_id = "mismatch_268ac90a56e2"

# User enters either "N/A" or CSV PK list: "client_id,account_id,batch_id"
pk_columns_input = "N/A"   # example: "client_id,account_id,batch_id"

trim_spaces = True
ignore_case = False
null_equals_empty = True
numeric_tolerance = 0.0    # set > 0 for tolerance numeric compare
join_type = "full_outer"

# Output (client mode local path)
ui_base_dir = "/tmp/ui_output"
spark_json_folder = os.path.join(ui_base_dir, "run_id=" + run_id)
ui_single_json_file = os.path.join(ui_base_dir, run_id + ".json")

# -----------------------------
# SOURCE/TARGET DF
# Replace these with your real loads
# -----------------------------
src_schema = T.StructType([
    T.StructField("client_id", T.StringType(), True),
    T.StructField("account_id", T.StringType(), True),
    T.StructField("batch_id", T.StringType(), True),
    T.StructField("STATUS", T.StringType(), True),
    T.StructField("EMAIL", T.StringType(), True),
    T.StructField("PINCODE", T.StringType(), True),
    T.StructField("ADDRESS", T.StringType(), True),
])

tgt_schema = T.StructType([
    T.StructField("client_id", T.StringType(), True),
    T.StructField("batch_id", T.StringType(), True),
    T.StructField("STATUS", T.StringType(), True),
    T.StructField("EMAIL", T.StringType(), True),
    T.StructField("PINCODE", T.StringType(), True),
    T.StructField("ADDRESS", T.StringType(), True),
])

source_df = spark.createDataFrame(
    [("10", "A123", "B55", " SrcVal_881 ", "SrcVal_693", "500001", "500001")],
    schema=src_schema
)

target_df = spark.createDataFrame(
    [("10", "B55", "TgtVal_678", "TgtVal_131", "50001", "500001")],
    schema=tgt_schema
)

# -----------------------------
# 1) Parse PK input
# -----------------------------
pk_requested = []
if pk_columns_input is not None:
    pk_str = str(pk_columns_input).strip()
    if pk_str and pk_str.upper() != "N/A":
        for p in pk_str.split(","):
            c = p.strip()
            if c:
                pk_requested.append(c)

# -----------------------------
# 2) Schema alignment + reorder columns
#    (THIS FIXES "order mismatch causes bad output")
# -----------------------------
src_cols = set(source_df.columns)
tgt_cols = set(target_df.columns)

# union of all columns present on either side OR user requested pk
union_cols = sorted(list(src_cols.union(tgt_cols).union(set(pk_requested))))

# Add missing columns (safe cast to string for compatibility)
for c in union_cols:
    if c not in src_cols:
        source_df = source_df.withColumn(c, F.lit(None).cast("string"))
for c in union_cols:
    if c not in tgt_cols:
        target_df = target_df.withColumn(c, F.lit(None).cast("string"))

# Reorder both dataframes in the SAME order
source_df = source_df.select(*union_cols)
target_df = target_df.select(*union_cols)

# Recompute cols after alignment
src_cols = set(source_df.columns)
tgt_cols = set(target_df.columns)

# Resolved PK = PK columns that exist after alignment (they will, because we added)
resolved_pk = []
for c in pk_requested:
    if c in src_cols and c in tgt_cols:
        resolved_pk.append(c)

# compare_cols in stable sorted order
compare_cols = [c for c in union_cols if c not in resolved_pk]

# -----------------------------
# 3) Normalization rules (no functions requirement: kept inline style)
# -----------------------------
tol = float(numeric_tolerance or 0.0)

# helper expressions (kept minimal and Spark 2.4 compatible)
def normalize_expr(col_expr):
    c = col_expr.cast("string")
    if null_equals_empty:
        c = F.coalesce(c, F.lit(""))
    if trim_spaces:
        c = F.trim(c)
    if ignore_case:
        c = F.upper(c)
    return c

# numeric check based on Spark schema types (safer than guessing)
type_map_src = {}
for f in source_df.schema.fields:
    type_map_src[f.name] = f.dataType.simpleString()

type_map_tgt = {}
for f in target_df.schema.fields:
    type_map_tgt[f.name] = f.dataType.simpleString()

def is_numeric_type(t):
    dt = (t or "").lower()
    return ("int" in dt) or ("bigint" in dt) or ("double" in dt) or ("float" in dt) or ("decimal" in dt) or ("smallint" in dt) or ("tinyint" in dt) or ("long" in dt) or ("short" in dt)

# -----------------------------
# 4) Build deterministic pairing keys
#    A) If PK present: add __pk_row_num to avoid many-to-many join duplicates
#    B) If NO-PK: build __row_hash + __row_num (stable) using sorted compare cols
# -----------------------------
# Build stable "sort key" using compare_cols (sorted) for BOTH cases
src_norm_list = []
tgt_norm_list = []
for c in compare_cols:
    src_norm_list.append(F.coalesce(normalize_expr(F.col(c)), F.lit("__NULL__")))
    tgt_norm_list.append(F.coalesce(normalize_expr(F.col(c)), F.lit("__NULL__")))

source_df = source_df.withColumn("__cmp_sort_key", F.concat_ws("||", *src_norm_list))
target_df = target_df.withColumn("__cmp_sort_key", F.concat_ws("||", *tgt_norm_list))

if len(resolved_pk) > 0:
    # PK case: stable row_number per PK group
    wpk = Window.partitionBy(*[F.col(k) for k in resolved_pk]).orderBy(F.col("__cmp_sort_key"))
    source_df = source_df.withColumn("__pk_row_num", F.row_number().over(wpk))
    target_df = target_df.withColumn("__pk_row_num", F.row_number().over(wpk))
    join_cols = list(resolved_pk) + ["__pk_row_num"]
else:
    # NO-PK case: hash + row_number per hash bucket
    source_df = source_df.withColumn("__row_hash", F.sha2(F.col("__cmp_sort_key"), 256))
    target_df = target_df.withColumn("__row_hash", F.sha2(F.col("__cmp_sort_key"), 256))

    wh = Window.partitionBy(F.col("__row_hash")).orderBy(F.col("__cmp_sort_key"))
    source_df = source_df.withColumn("__row_num", F.row_number().over(wh))
    target_df = target_df.withColumn("__row_num", F.row_number().over(wh))

    join_cols = ["__row_hash", "__row_num"]

# -----------------------------
# 5) Join
# -----------------------------
j = source_df.alias("s").join(target_df.alias("t"), on=join_cols, how=join_type)

# -----------------------------
# 6) Output PK columns
# -----------------------------
if len(resolved_pk) == 0:
    # use short hash + row num for UI-friendly pkDisplay
    j = j.withColumn("__pk_hash_short", F.substring(F.col("__row_hash"), 1, 12))
    j = j.withColumn("__pk_row_num", F.col("__row_num"))
    out_pk_cols = ["__pk_hash_short", "__pk_row_num"]
else:
    out_pk_cols = list(resolved_pk)

# -----------------------------
# 7) Build per-field comparison structs (explode later)
#    IMPORTANT: keep NULLs as real NULL so JSON shows null
# -----------------------------
pairs = []
for c in compare_cols:
    src_c = F.col("s.`%s`" % c)
    tgt_c = F.col("t.`%s`" % c)

    t_str = type_map_src.get(c)
    if not t_str:
        t_str = type_map_tgt.get(c)
    if not t_str:
        t_str = "string"

    # match expression
    if is_numeric_type(t_str) and tol > 0.0:
        s_num = src_c.cast("double")
        t_num = tgt_c.cast("double")
        match_expr = (
            (s_num.isNull() & t_num.isNull()) |
            (s_num.isNotNull() & t_num.isNotNull() & (F.abs(s_num - t_num) <= F.lit(tol)))
        )
    else:
        match_expr = normalize_expr(src_c).eqNullSafe(normalize_expr(tgt_c))

    pairs.append(
        F.struct(
            F.lit(c).alias("FIELD_NAME"),
            src_c.cast("string").alias("SRC_STATUS"),   # NULL stays NULL
            tgt_c.cast("string").alias("TGT_STATUS"),   # NULL stays NULL
            F.when(match_expr, F.lit("Y")).otherwise(F.lit("N")).alias("MATCH_IND")
        )
    )

# -----------------------------
# 8) Create long_df: one row per field per record
# -----------------------------
select_pk_cols = [F.col(k) for k in out_pk_cols]

tmp_df = j.select(*(select_pk_cols + [F.explode(F.array(*pairs)).alias("x")]))

long_df = tmp_df.select(
    *(select_pk_cols + [
        F.col("x.FIELD_NAME").alias("FIELD_NAME"),
        F.col("x.SRC_STATUS").alias("SRC_STATUS"),
        F.col("x.TGT_STATUS").alias("TGT_STATUS"),
        F.col("x.MATCH_IND").alias("MATCH_IND")
    ])
)

# -----------------------------
# 9) Build final JSON structure
# -----------------------------
df_fields = long_df.withColumn(
    "field_struct",
    F.struct(
        F.col("FIELD_NAME").cast("string").alias("fieldName"),
        F.col("SRC_STATUS").alias("sourceValue"),   # NULL remains NULL => JSON null
        F.col("TGT_STATUS").alias("targetValue"),   # NULL remains NULL => JSON null
        F.when(F.col("MATCH_IND") == "Y", F.lit("MATCH")).otherwise(F.lit("MISMATCH")).alias("status")
    )
)

final_df = (
    df_fields.groupBy(*out_pk_cols)
    .agg(
        F.collect_list("field_struct").alias("fields"),
        F.sum(F.when(F.col("MATCH_IND") == "N", F.lit(1)).otherwise(F.lit(0))).cast("int").alias("mismatchCount")
    )
    .withColumn("status", F.when(F.col("mismatchCount") > 0, F.lit("MISMATCH")).otherwise(F.lit("MATCH")))
)

# pk struct
pk_struct_cols = []
for c in out_pk_cols:
    pk_struct_cols.append(F.col(c).cast("string").alias(c))
final_df = final_df.withColumn("pk", F.struct(*pk_struct_cols))

# pkDisplay
pk_disp_parts = []
for c in out_pk_cols:
    pk_disp_parts.append(F.concat(F.lit(c + "="), F.col(c).cast("string")))
final_df = final_df.withColumn("pkDisplay", F.concat_ws(" | ", *pk_disp_parts))

# stable ordering of fields (Spark sorts structs by field order)
final_df = final_df.withColumn("fields", F.sort_array(F.col("fields")))

final_df = final_df.select("pk", "pkDisplay", "mismatchCount", "status", "fields")

# -----------------------------
# 10) Write output for UI (client mode local)
# -----------------------------
if not os.path.exists(ui_base_dir):
    os.makedirs(ui_base_dir)

final_df.coalesce(1).write.mode("overwrite").json(spark_json_folder)

part_files = glob.glob(os.path.join(spark_json_folder, "part-*.json"))
if (part_files is None) or (len(part_files) == 0):
    raise RuntimeError("No part-*.json found in: " + spark_json_folder)

shutil.copy(part_files[0], ui_single_json_file)

print("OK Spark JSON folder : " + spark_json_folder)
print("OK UI JSON file      : " + ui_single_json_file)

spark.stop()
