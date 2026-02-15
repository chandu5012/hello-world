# ============================================================
# FINAL UPDATED PYSPARK SCRIPT (Spark 2.4/Python2.7 + Spark 3.3)
#
# Fixes:
# ✅ NO-PK FULL OUTER JOIN: pk/ pkDisplay never empty now
# ✅ mismatchCount correct (won’t become 110 for 10 fields)
# ✅ deterministic matching for duplicates using __row_sort_key
# ✅ null values appear in JSON as "null"
# ✅ schema alignment source vs target (missing cols -> null)
# ✅ supports options: trim, ignore_case, null_equals_empty, numeric_tolerance
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
pk_columns_input = "N/A"   # <-- set to N/A to test NO-PK case

trim_spaces = True
ignore_case = False
null_equals_empty = True
numeric_tolerance = 0.0   # set to > 0 for tolerance
join_type = "full_outer"  # full_outer is common for mismatch explorer

ui_base_dir = "/tmp/ui_output"
spark_json_folder = os.path.join(ui_base_dir, "run_id=" + run_id)
ui_single_json_file = os.path.join(ui_base_dir, run_id + ".json")

# -----------------------------
# SOURCE/TARGET DF (REPLACE WITH REAL LOADS)
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
        for p in pk_str.split(","):
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
# 3) Schema alignment (source vs target)
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

# compare columns = all except resolved_pk
compare_cols = []
for c in union_cols:
    if c not in resolved_pk:
        compare_cols.append(c)

# -----------------------------
# 4) Normalization rules
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
# 5) Join keys: PK if exists else fallback
# -----------------------------
join_cols = list(resolved_pk)

if len(join_cols) == 0:
    # Build normalized string list for hash and deterministic ordering
    src_norm_cols = []
    for c in compare_cols:
        src_norm_cols.append(F.coalesce(_normalize_expr(F.col(c)), F.lit("__NULL__")))

    tgt_norm_cols = []
    for c in compare_cols:
        tgt_norm_cols.append(F.coalesce(_normalize_expr(F.col(c)), F.lit("__NULL__")))

    source_df = source_df.withColumn("__row_hash", F.sha2(F.concat_ws("||", *src_norm_cols), 256))
    target_df = target_df.withColumn("__row_hash", F.sha2(F.concat_ws("||", *tgt_norm_cols), 256))

    # deterministic ordering within same hash bucket
    source_df = source_df.withColumn("__row_sort_key", F.concat_ws("||", *src_norm_cols))
    target_df = target_df.withColumn("__row_sort_key", F.concat_ws("||", *tgt_norm_cols))

    w = Window.partitionBy("__row_hash").orderBy(F.col("__row_sort_key"))

    source_df = source_df.withColumn("__row_num", F.row_number().over(w))
    target_df = target_df.withColumn("__row_num", F.row_number().over(w))

    join_cols = ["__row_hash", "__row_num"]

# -----------------------------
# 6) Join (USING join_cols => output join columns are top-level coalesced)
# -----------------------------
j = source_df.alias("s").join(target_df.alias("t"), on=join_cols, how=join_type)

# -----------------------------
# 7) Decide output PK columns (IMPORTANT FIX)
#    - If PK exists: use top-level PK cols from join output
#    - If NO PK: use top-level __row_hash + __row_num to build pk (works for target-only rows too)
# -----------------------------
if len(resolved_pk) == 0:
    # join output contains __row_hash and __row_num at top-level
    j = j.withColumn("__pk_hash_short", F.substring(F.col("__row_hash"), 1, 12))
    j = j.withColumn("__pk_row_num", F.col("__row_num"))
    out_pk_cols = ["__pk_hash_short", "__pk_row_num"]
else:
    out_pk_cols = list(resolved_pk)

# -----------------------------
# 8) Build comparison structs (explode fields)
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
# 9) long_df (PK cols are TOP-LEVEL now, not s.)
# -----------------------------
select_pk_cols = []
for k in out_pk_cols:
    select_pk_cols.append(F.col(k))

tmp_df = j.select(*(select_pk_cols + [F.explode(F.array(*pairs)).alias("x")]))

long_df = tmp_df.select(
    *(select_pk_cols + [
        F.col("x.FIELD_NAME"),
        F.col("x.SRC_STATUS"),
        F.col("x.TGT_STATUS"),
        F.col("x.MATCH_IND")
    ])
)

# -----------------------------
# 10) final_df JSON
# -----------------------------
df_fields = long_df.withColumn(
    "field_struct",
    F.struct(
        F.col("FIELD_NAME").cast("string").alias("fieldName"),
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

# pk struct (top-level pk columns)
pk_struct_cols = []
for c in out_pk_cols:
    pk_struct_cols.append(F.col(c).cast("string").alias(c))
final_df = final_df.withColumn("pk", F.struct(*pk_struct_cols))

# pkDisplay
pk_disp_parts = []
for c in out_pk_cols:
    pk_disp_parts.append(F.concat(F.lit(c + "="), F.col(c).cast("string")))
final_df = final_df.withColumn("pkDisplay", F.concat_ws(" | ", *pk_disp_parts))

# stable ordering of fields
final_df = final_df.withColumn("fields", F.sort_array(F.col("fields")))

final_df = final_df.select("pk", "pkDisplay", "mismatchCount", "status", "fields")

# -----------------------------
# 11) Write output for UI (client mode local)
# -----------------------------
if not os.path.exists(ui_base_dir):
    os.makedirs(ui_base_dir)

final_df.coalesce(1).write.mode("overwrite").json(spark_json_folder)

part_files = glob.glob(os.path.join(spark_json_folder, "part-*.json"))
if (part_files is None) or (len(part_files) == 0):
    raise RuntimeError("No part-*.json found in: " + spark_json_folder)

shutil.copy(part_files[0], ui_single_json_file)

print("✅ Spark JSON folder :", spark_json_folder)
print("✅ UI JSON file      :", ui_single_json_file)

spark.stop()
