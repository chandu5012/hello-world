# ============================================================
# COMPLETE SINGLE CONTINUOUS PYSPARK SCRIPT (NO CUSTOM FUNCTIONS)
# Compatible with:
#   - Spark 2.4.x + Python 2.7
#   - Spark 3.3.x + Python 3.x
#
# Features:
# 1) PK input: user enters "N/A" OR comma-separated PK columns.
#    - PK cols may exist in source OR target; if exists in either side -> consider.
# 2) Schema alignment:
#    - If source has cols not in target -> add to target as NULL
#    - If target has cols not in source -> add to source as NULL
#    - Missing PK columns also get added as NULL on other side.
# 3) If no usable PK -> fallback surrogate key using:
#      __row_hash (sha2 of normalized row) + __row_num
#    - UI-friendly short key: __row_hash_short (only on source to avoid ambiguity)
# 4) Comparison rules applied only when flags True:
#      trim_spaces, ignore_case, null_equals_empty, numeric_tolerance (>0)
# 5) Outputs:
#    A) long_df: PK..., FIELD_NAME, SRC_STATUS, TGT_STATUS, MATCH_IND
#    B) final_df: nested JSON structure for UI
# 6) JSON NULL visibility:
#    - Null values are converted to string "null" so JSON always has keys
# 7) Client mode local output:
#    - Writes Spark JSON folder + copies single part file to {run_id}.json
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

# User enters either "N/A" or PK list like: "client_id,account_id,batch_id"
pk_columns_input = "client_id,account_id,batch_id"   # change to "N/A" when no PK

# Comparison options (apply only if True / tol > 0)
trim_spaces = True
ignore_case = False
null_equals_empty = True
numeric_tolerance = 0.0   # set to > 0 (ex: 0.01) for numeric tolerance
join_type = "full_outer"  # "inner" / "left" / "right" / "full_outer"

# Local output path for UI (client mode)
ui_base_dir = "/tmp/ui_output"
spark_json_folder = os.path.join(ui_base_dir, "run_id=" + run_id)
ui_single_json_file = os.path.join(ui_base_dir, run_id + ".json")

# -----------------------------
# SOURCE/TARGET DF (REPLACE WITH YOUR REAL LOADS)
# -----------------------------
# NOTE: This is just sample data. Replace source_df and target_df with your JDBC/Mongo/File reads.

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
    # simulate missing account_id on target side
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
        parts = pk_str.split(",")
        for p in parts:
            c = p.strip()
            if c:
                pk_requested.append(c)

# -----------------------------
# 2) Resolve PK columns that exist in SOURCE OR TARGET
# -----------------------------
src_cols = set(source_df.columns)
tgt_cols = set(target_df.columns)

resolved_pk = []
for c in pk_requested:
    if (c in src_cols) or (c in tgt_cols):
        resolved_pk.append(c)

# -----------------------------
# 3) Schema alignment (union of columns + resolved_pk)
# -----------------------------
src_type_map = {}
for f in source_df.schema.fields:
    src_type_map[f.name] = f.dataType.simpleString()

tgt_type_map = {}
for f in target_df.schema.fields:
    tgt_type_map[f.name] = f.dataType.simpleString()

union_cols = list(set(source_df.columns).union(set(target_df.columns)).union(set(resolved_pk)))
union_cols.sort()

# add missing columns to source
for c in union_cols:
    if c not in src_cols:
        dt = tgt_type_map.get(c)
        if dt:
            source_df = source_df.withColumn(c, F.lit(None).cast(dt))
        else:
            source_df = source_df.withColumn(c, F.lit(None).cast("string"))

# add missing columns to target
for c in union_cols:
    if c not in tgt_cols:
        dt = src_type_map.get(c)
        if dt:
            target_df = target_df.withColumn(c, F.lit(None).cast(dt))
        else:
            target_df = target_df.withColumn(c, F.lit(None).cast("string"))

# refresh after alignment
src_cols = set(source_df.columns)
tgt_cols = set(target_df.columns)

src_type_map2 = {}
for f in source_df.schema.fields:
    src_type_map2[f.name] = f.dataType.simpleString()

tgt_type_map2 = {}
for f in target_df.schema.fields:
    tgt_type_map2[f.name] = f.dataType.simpleString()

compare_cols = []
for c in union_cols:
    if c not in resolved_pk:
        compare_cols.append(c)

# -----------------------------
# 4) Normalization helper (inline)
# -----------------------------
def _normalize_expr(col_expr):
    c = col_expr.cast("string")
    if null_equals_empty:
        c = F.coalesce(c, F.lit(""))
    if trim_spaces:
        c = F.trim(c)
    if ignore_case:
        c = F.upper(c)
    return c

def _is_numeric_type(t):
    dt = (t or "").lower()
    return ("int" in dt) or ("bigint" in dt) or ("double" in dt) or ("float" in dt) or ("decimal" in dt) or ("smallint" in dt) or ("tinyint" in dt) or ("long" in dt) or ("short" in dt)

tol = float(numeric_tolerance or 0.0)

# -----------------------------
# 5) Choose join cols (PK if available else surrogate)
# -----------------------------
join_cols = list(resolved_pk)
out_pk_cols = list(resolved_pk)

if len(join_cols) == 0:
    # Build row hash using normalized compare columns
    src_norm_cols = []
    for c in compare_cols:
        src_norm_cols.append(F.coalesce(_normalize_expr(F.col(c)), F.lit("∅")))

    tgt_norm_cols = []
    for c in compare_cols:
        tgt_norm_cols.append(F.coalesce(_normalize_expr(F.col(c)), F.lit("∅")))

    source_df = source_df.withColumn("__row_hash", F.sha2(F.concat_ws("||", *src_norm_cols), 256))
    target_df = target_df.withColumn("__row_hash", F.sha2(F.concat_ws("||", *tgt_norm_cols), 256))

    w = Window.partitionBy("__row_hash").orderBy(F.lit(1))
    source_df = source_df.withColumn("__row_num", F.row_number().over(w))
    target_df = target_df.withColumn("__row_num", F.row_number().over(w))

    # UI-friendly short hash ONLY on source to avoid ambiguity after join
    source_df = source_df.withColumn("__row_hash_short", F.substring(F.col("__row_hash"), 1, 12))

    # Join on full hash + row_num, display short hash + row_num
    join_cols = ["__row_hash", "__row_num"]
    out_pk_cols = ["__row_hash_short", "__row_num"]

# -----------------------------
# 6) Join
# -----------------------------
j = source_df.alias("s").join(target_df.alias("t"), on=join_cols, how=join_type)

# -----------------------------
# 7) Build comparison structs (row per field)
#    Ensure null values appear in JSON by converting null -> "null"
# -----------------------------
pairs = []

for c in compare_cols:
    src_c = F.col("s.`%s`" % c)
    tgt_c = F.col("t.`%s`" % c)

    t_str = src_type_map2.get(c)
    if not t_str:
        t_str = tgt_type_map2.get(c)
    if not t_str:
        t_str = "string"

    if (_is_numeric_type(t_str)) and (tol > 0.0):
        s_num = src_c.cast("double")
        t_num = tgt_c.cast("double")
        match_expr = (
            (s_num.isNull() & t_num.isNull()) |
            (s_num.isNotNull() & t_num.isNotNull() & (F.abs(s_num - t_num) <= F.lit(tol)))
        )
    else:
        match_expr = _normalize_expr(src_c).eqNullSafe(_normalize_expr(tgt_c))

    pairs.append(
        F.struct(
            F.lit(c).alias("FIELD_NAME"),
            F.when(src_c.isNull(), F.lit("null")).otherwise(src_c.cast("string")).alias("SRC_STATUS"),
            F.when(tgt_c.isNull(), F.lit("null")).otherwise(tgt_c.cast("string")).alias("TGT_STATUS"),
            F.when(match_expr, F.lit("Y")).otherwise(F.lit("N")).alias("MATCH_IND")
        )
    )

# -----------------------------
# 8) Create long_df
#    IMPORTANT: out_pk_cols might include __row_hash_short which exists ONLY on source alias "s"
# -----------------------------
select_pk_cols = []
for k in out_pk_cols:
    # select from source alias to avoid ambiguity (works for both normal PK and short-hash case)
    select_pk_cols.append(F.col("s.`%s`" % k).alias(k))

long_df = (
    j.select(
        *(select_pk_cols + [F.explode(F.array(*pairs)).alias("x")])
    )
    .select(
        *select_pk_cols,
        F.col("x.FIELD_NAME"),
        F.col("x.SRC_STATUS"),
        F.col("x.TGT_STATUS"),
        F.col("x.MATCH_IND")
    )
)

# -----------------------------
# 9) Build final_df (nested JSON structure)
# -----------------------------
df_fields = long_df.withColumn(
    "field_struct",
    F.struct(
        F.col("FIELD_NAME").cast("string").alias("fieldName"),
        # keep "null" string if present, else value
        F.when(F.col("SRC_STATUS").isNull(), F.lit("null")).otherwise(F.col("SRC_STATUS")).alias("sourceValue"),
        F.when(F.col("TGT_STATUS").isNull(), F.lit("null")).otherwise(F.col("TGT_STATUS")).alias("targetValue"),
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

# sort fields for stable UI view
final_df = final_df.withColumn("fields", F.sort_array(F.col("fields")))

final_df = final_df.select("pk", "pkDisplay", "mismatchCount", "status", "fields")

# -----------------------------
# 10) Write output to LOCAL for UI (client mode)
# -----------------------------
if not os.path.exists(ui_base_dir):
    os.makedirs(ui_base_dir)

# coalesce(1) -> write single part file
final_df.coalesce(1).write.mode("overwrite").json(spark_json_folder)

part_files = glob.glob(os.path.join(spark_json_folder, "part-*.json"))
if (part_files is None) or (len(part_files) == 0):
    raise RuntimeError("No part-*.json found in: " + spark_json_folder)

shutil.copy(part_files[0], ui_single_json_file)

print("✅ Spark JSON folder :", spark_json_folder)
print("✅ UI JSON file      :", ui_single_json_file)

# Optional debug:
# long_df.show(truncate=False)
# final_df.show(truncate=False)

spark.stop()
