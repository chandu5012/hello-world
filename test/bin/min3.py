# ============================================================
# FINAL MISMATCH EXPLORER JSON GENERATOR
# Spark 2.4 (Py2.7) + Spark 3.3 compatible
#
# Fixes:
# - Column order differences (source vs target) won't break output
# - Missing columns handled by adding nulls on both sides
# - PK provided -> join on PK
# - PK=N/A -> deterministic row_number join (NO HASH JOIN)
# - No duplicate FIELD_NAME rows
# - mismatchCount correct
# - null values appear as "null" string in JSON
# - trim/ignore_case/null_equals_empty/numeric_tolerance supported
# ============================================================

import os
import glob
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession.builder.appName("MismatchExplorer_JSON_Final").getOrCreate()

# -----------------------------
# INPUTS (from UI)
# -----------------------------
run_id = "mismatch_268ac90a56e2"

# User enters either "N/A" OR "col1,col2"
pk_columns_input = "N/A"

trim_spaces = True
ignore_case = False
null_equals_empty = True
numeric_tolerance = 0.0  # set >0 for tolerance

join_type = "full_outer"

ui_base_dir = "/tmp/ui_output"
spark_json_folder = os.path.join(ui_base_dir, "run_id=" + run_id)
ui_single_json_file = os.path.join(ui_base_dir, run_id + ".json")

# -----------------------------
# SOURCE/TARGET DF (REPLACE WITH REAL LOADS)
# -----------------------------
# source_df = ...
# target_df = ...

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
src_cols0 = set(source_df.columns)
tgt_cols0 = set(target_df.columns)

resolved_pk = []
for c in pk_requested:
    if (c in src_cols0) or (c in tgt_cols0):
        resolved_pk.append(c)

# -----------------------------
# 3) Schema alignment (add missing columns as nulls)
#     + enforce SAME COLUMN ORDER by selecting sorted union cols
# -----------------------------
src_type_map = {}
for f in source_df.schema.fields:
    src_type_map[f.name] = f.dataType.simpleString()

tgt_type_map = {}
for f in target_df.schema.fields:
    tgt_type_map[f.name] = f.dataType.simpleString()

# union of all cols (including any pk col user gave)
union_cols = list(set(source_df.columns).union(set(target_df.columns)).union(set(resolved_pk)))
union_cols = sorted(union_cols)

# add missing columns to source
for c in union_cols:
    if c not in source_df.columns:
        dt = tgt_type_map.get(c)
        if dt:
            source_df = source_df.withColumn(c, F.lit(None).cast(dt))
        else:
            source_df = source_df.withColumn(c, F.lit(None).cast("string"))

# add missing columns to target
for c in union_cols:
    if c not in target_df.columns:
        dt = src_type_map.get(c)
        if dt:
            target_df = target_df.withColumn(c, F.lit(None).cast(dt))
        else:
            target_df = target_df.withColumn(c, F.lit(None).cast("string"))

# IMPORTANT: reorder both dataframes to SAME ORDER
source_df = source_df.select(*union_cols)
target_df = target_df.select(*union_cols)

# rebuild type maps after alignment
src_type_map2 = {}
for f in source_df.schema.fields:
    src_type_map2[f.name] = f.dataType.simpleString()

tgt_type_map2 = {}
for f in target_df.schema.fields:
    tgt_type_map2[f.name] = f.dataType.simpleString()

# columns to compare (exclude pk if pk exists)
compare_cols = []
for c in union_cols:
    if c not in resolved_pk:
        compare_cols.append(c)

# -----------------------------
# 4) Normalization rules (INLINE)
# -----------------------------
tol = float(numeric_tolerance or 0.0)

def is_numeric_type(t):
    dt = (t or "").lower()
    return ("int" in dt) or ("bigint" in dt) or ("double" in dt) or ("float" in dt) or ("decimal" in dt) or ("smallint" in dt) or ("tinyint" in dt) or ("long" in dt) or ("short" in dt)

# for normalization we will build expressions each time (no custom unicode chars)
def norm_expr(col_expr):
    c = col_expr.cast("string")
    if null_equals_empty:
        c = F.coalesce(c, F.lit(""))
    if trim_spaces:
        c = F.trim(c)
    if ignore_case:
        c = F.upper(c)
    return c

# -----------------------------
# 5) Join strategy
#    PK present => join on PK
#    PK=N/A     => deterministic row_number join (fixes duplicates)
# -----------------------------
if len(resolved_pk) > 0:
    # Use PK join
    j = source_df.alias("s").join(target_df.alias("t"), on=resolved_pk, how=join_type)
    out_pk_cols = list(resolved_pk)

else:
    # NO PK => Create deterministic __row_num using SAME ORDER expressions

    # build ordering expressions from compare columns (already sorted)
    order_exprs = []
    for c in compare_cols:
        order_exprs.append(norm_expr(F.col(c)))

    # If compare_cols empty (rare), fallback to monotonically_increasing_id ordering
    if len(order_exprs) == 0:
        source_df = source_df.withColumn("__row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
        target_df = target_df.withColumn("__row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    else:
        source_df = source_df.withColumn("__row_num", F.row_number().over(Window.orderBy(*order_exprs)))
        target_df = target_df.withColumn("__row_num", F.row_number().over(Window.orderBy(*order_exprs)))

    # Join on __row_num (THIS is the key fix)
    j = source_df.alias("s").join(target_df.alias("t"), on=["__row_num"], how=join_type)

    # Create a short pk for UI (ROW=<num>)
    j = j.withColumn("__pk_row", F.concat(F.lit("ROW_"), F.col("__row_num").cast("string")))
    out_pk_cols = ["__pk_row"]

# -----------------------------
# 6) Build comparison pairs (explode)
# -----------------------------
pairs = []
for c in compare_cols:
    src_c = F.col("s.`%s`" % c)
    tgt_c = F.col("t.`%s`" % c)

    t_str = src_type_map2.get(c) or tgt_type_map2.get(c) or "string"

    if is_numeric_type(t_str) and (tol > 0.0):
        s_num = src_c.cast("double")
        t_num = tgt_c.cast("double")
        match_expr = (
            (s_num.isNull() & t_num.isNull()) |
            (s_num.isNotNull() & t_num.isNotNull() & (F.abs(s_num - t_num) <= F.lit(tol)))
        )
    else:
        match_expr = norm_expr(src_c).eqNullSafe(norm_expr(tgt_c))

    pairs.append(
        F.struct(
            F.lit(c).alias("FIELD_NAME"),
            F.when(src_c.isNull(), F.lit("null")).otherwise(src_c.cast("string")).alias("SRC_STATUS"),
            F.when(tgt_c.isNull(), F.lit("null")).otherwise(tgt_c.cast("string")).alias("TGT_STATUS"),
            F.when(match_expr, F.lit("Y")).otherwise(F.lit("N")).alias("MATCH_IND")
        )
    )

# pk select
pk_select_exprs = []
if len(resolved_pk) > 0:
    for k in out_pk_cols:
        pk_select_exprs.append(F.col(k))
else:
    pk_select_exprs.append(F.col("__pk_row"))

tmp_df = j.select(*(pk_select_exprs + [F.explode(F.array(*pairs)).alias("x")]))

long_df = tmp_df.select(
    *(pk_select_exprs + [
        F.col("x.FIELD_NAME"),
        F.col("x.SRC_STATUS"),
        F.col("x.TGT_STATUS"),
        F.col("x.MATCH_IND")
    ])
)

# -----------------------------
# 7) DEDUPLICATE (prevents repeated FIELD_NAME rows)
# -----------------------------
group_keys = list(out_pk_cols) + ["FIELD_NAME"]
long_df = (
    long_df.groupBy(*group_keys)
    .agg(
        F.first("SRC_STATUS", ignorenulls=False).alias("SRC_STATUS"),
        F.first("TGT_STATUS", ignorenulls=False).alias("TGT_STATUS"),
        F.first("MATCH_IND", ignorenulls=False).alias("MATCH_IND")
    )
)

# -----------------------------
# 8) Build final JSON dataframe
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

# keep fields stable order by fieldName
final_df = final_df.withColumn("fields", F.sort_array(F.col("fields")))
final_df = final_df.select("pk", "pkDisplay", "mismatchCount", "status", "fields")

# -----------------------------
# 9) Write output (client mode local path)
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
