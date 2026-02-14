# ============================================================
# SINGLE CONTINUOUS SCRIPT (NO CUSTOM FUNCTIONS)
# Spark 2.4 + 3.3 compatible
#
# PK RULE (UPDATED AS YOU SAID):
# - User either enters "N/A" OR provides ONE OR MORE candidate PK columns.
# - Those PK columns might exist only in Source OR only in Target.
# - If a given PK column exists in either side -> we consider it as PK.
# - We add missing PK columns on the other side as NULL so join works.
# - If user enters N/A OR none of the PK columns exist in both sides -> NO PK fallback
#   using surrogate (__row_hash, __row_num).
# ============================================================

import os, glob, shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import types as T

spark = SparkSession.builder.appName("MismatchExplorer_Final_JSON").getOrCreate()

# -----------------------------
# INPUTS (from UI)
# -----------------------------
run_id = "mismatch_268ac90a56e2"

# User enters either "N/A" or a PK list (single or multi, comma-separated)
pk_columns_input = "client_id,account_id,batch_id"   # examples: "id" OR "id,acct" OR "N/A"

trim_spaces = True
ignore_case = False
null_equals_empty = True
numeric_tolerance = 0.0
join_type = "full_outer"

ui_base_dir = "/tmp/ui_output"
spark_json_folder = os.path.join(ui_base_dir, f"run_id={run_id}")
ui_single_json_file = os.path.join(ui_base_dir, f"{run_id}.json")

# -----------------------------
# SOURCE/TARGET DF (REPLACE WITH YOUR LOADS)
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
# 1) Parse PK input (user either gives N/A or list)
# -----------------------------
pk_requested = []
if pk_columns_input is not None:
    pk_str = str(pk_columns_input).strip()
    if pk_str and pk_str.upper() != "N/A":
        pk_requested = [c.strip() for c in pk_str.split(",") if c.strip()]

# -----------------------------
# 2) Consider PK columns if they exist in SOURCE OR TARGET
# -----------------------------
src_cols = set(source_df.columns)
tgt_cols = set(target_df.columns)

resolved_pk = []
for c in pk_requested:
    if c in src_cols or c in tgt_cols:
        resolved_pk.append(c)

# If user gave PK but none exist, treat as NO PK
# If user gave N/A => pk_requested empty => resolved_pk empty => NO PK
# (this matches your requirement)
# resolved_pk is final PK list

# -----------------------------
# 3) Align schemas (add missing cols both sides as NULL)
#    Includes resolved_pk too (so PK exists on both after alignment)
# -----------------------------
src_type_map = {f.name: f.dataType.simpleString() for f in source_df.schema.fields}
tgt_type_map = {f.name: f.dataType.simpleString() for f in target_df.schema.fields}

union_cols = sorted(set(source_df.columns).union(set(target_df.columns)).union(set(resolved_pk)))

# add missing columns to source
for c in union_cols:
    if c not in src_cols:
        dt = tgt_type_map.get(c)
        source_df = source_df.withColumn(c, F.lit(None).cast(dt if dt else "string"))

# add missing columns to target
for c in union_cols:
    if c not in tgt_cols:
        dt = src_type_map.get(c)
        target_df = target_df.withColumn(c, F.lit(None).cast(dt if dt else "string"))

# refresh sets + type maps after alignment
src_cols = set(source_df.columns)
tgt_cols = set(target_df.columns)
src_type_map2 = {f.name: f.dataType.simpleString() for f in source_df.schema.fields}
tgt_type_map2 = {f.name: f.dataType.simpleString() for f in target_df.schema.fields}

compare_cols = [c for c in union_cols if c not in resolved_pk]

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
    return any(x in dt for x in ["int","bigint","double","float","decimal","smallint","tinyint","long","short"])

# -----------------------------
# 5) Decide JOIN columns:
#    - If resolved_pk exists -> join on it
#    - Else fallback to surrogate join key (__row_hash, __row_num)
# -----------------------------
join_cols = list(resolved_pk)
out_pk_cols = list(resolved_pk)

if len(join_cols) == 0:
    # Surrogate key based on ALL compare columns
    src_norm_cols = [F.coalesce(_normalize_expr(F.col(c)), F.lit("∅")) for c in compare_cols]
    tgt_norm_cols = [F.coalesce(_normalize_expr(F.col(c)), F.lit("∅")) for c in compare_cols]

    source_df = source_df.withColumn("__row_hash", F.sha2(F.concat_ws("||", *src_norm_cols), 256))
    target_df = target_df.withColumn("__row_hash", F.sha2(F.concat_ws("||", *tgt_norm_cols), 256))

    w = Window.partitionBy("__row_hash").orderBy(F.lit(1))
    source_df = source_df.withColumn("__row_num", F.row_number().over(w))
    target_df = target_df.withColumn("__row_num", F.row_number().over(w))

    join_cols = ["__row_hash", "__row_num"]
    out_pk_cols = ["__row_hash", "__row_num"]

# -----------------------------
# 6) Join
# -----------------------------
j = source_df.alias("s").join(target_df.alias("t"), on=join_cols, how=join_type)

# -----------------------------
# 7) Field comparison logic (inline)
# -----------------------------
tol = float(numeric_tolerance or 0.0)

pairs = []
for c in compare_cols:
    src_c = F.col(f"s.`{c}`")
    tgt_c = F.col(f"t.`{c}`")
    t_str = src_type_map2.get(c) or tgt_type_map2.get(c) or "string"

    if _is_numeric_type(t_str) and tol > 0:
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
            src_c.cast("string").alias("SRC_STATUS"),
            tgt_c.cast("string").alias("TGT_STATUS"),
            F.when(match_expr, F.lit("Y")).otherwise(F.lit("N")).alias("MATCH_IND")
        )
    )

# -----------------------------
# 8) long_df (row per field)
# -----------------------------
long_df = (
    j.select(*[F.col(k) for k in out_pk_cols], F.explode(F.array(*pairs)).alias("x"))
     .select(
         *[F.col(k) for k in out_pk_cols],
         F.col("x.FIELD_NAME"),
         F.col("x.SRC_STATUS"),
         F.col("x.TGT_STATUS"),
         F.col("x.MATCH_IND")
     )
)

# -----------------------------
# 9) final_df (nested JSON structure)
# -----------------------------
df_fields = long_df.withColumn(
    "field_struct",
    F.struct(
        F.col("FIELD_NAME").cast("string").alias("fieldName"),
        F.col("SRC_STATUS").cast("string").alias("sourceValue"),
        F.col("TGT_STATUS").cast("string").alias("targetValue"),
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

final_df = final_df.withColumn("pk", F.struct(*[F.col(c).cast("string").alias(c) for c in out_pk_cols]))
pk_disp_parts = [F.concat(F.lit(f"{c}="), F.col(c).cast("string")) for c in out_pk_cols]
final_df = final_df.withColumn("pkDisplay", F.concat_ws(" | ", *pk_disp_parts))
final_df = final_df.withColumn("fields", F.sort_array(F.col("fields")))
final_df = final_df.select("pk", "pkDisplay", "mismatchCount", "status", "fields")

# -----------------------------
# 10) Write output to LOCAL for UI (client mode)
#     - coalesce(1) -> single part file
#     - copy to fixed file {run_id}.json
# -----------------------------
if not os.path.exists(ui_base_dir):
    os.makedirs(ui_base_dir)

final_df.coalesce(1).write.mode("overwrite").json(spark_json_folder)

part_files = glob.glob(os.path.join(spark_json_folder, "part-*.json"))
if not part_files:
    raise RuntimeError("No part-*.json found in: " + spark_json_folder)

shutil.copy(part_files[0], ui_single_json_file)

print("✅ Spark JSON folder :", spark_json_folder)
print("✅ UI JSON file      :", ui_single_json_file)

# Optional debug
# long_df.show(truncate=False)
# final_df.show(truncate=False)

spark.stop()
