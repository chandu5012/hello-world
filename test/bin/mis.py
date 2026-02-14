# ============================================================
# Spark 2.4 + 3.3 compatible end-to-end code
# Enhancements added per your requirement:
#
# 1) PK columns can be provided from source side OR target side.
#    -> We validate against both schemas and take whichever exists.
#    -> If some PK cols exist only on one side, we add missing PK col(s)
#       on the other side as NULL so join works.
#
# 2) Schema alignment for comparison:
#    -> If source has columns not in target, add those columns to target as NULL
#    -> If target has columns not in source, add those columns to source as NULL
#    -> Then compare on UNION of columns (excluding PK)
#
# 3) Optional comparison rules applied ONLY if their flags True:
#    - trim_spaces
#    - ignore_case
#    - null_equals_empty
#    - numeric_tolerance (only if > 0 and numeric type)
#
# 4) If PK is still empty/not usable => fallback to NO-PK surrogate join key
#    (__row_hash, __row_num) based on ALL compare columns.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import types as T


# -----------------------------
# PK parsing + validation
# -----------------------------
def parse_pk(pk_columns):
    if pk_columns is None:
        return []
    if isinstance(pk_columns, list):
        return [c.strip() for c in pk_columns if c and c.strip() and c.strip().upper() != "N/A"]
    s = str(pk_columns).strip()
    if not s or s.upper() == "N/A":
        return []
    return [c.strip() for c in s.split(",") if c.strip()]


def resolve_pk_columns(pk_requested, src_cols, tgt_cols):
    """
    User can provide pk columns; they might exist in source OR target.
    Rule:
      - Keep only requested PK columns that exist in source or target.
      - We'll ensure both sides have those columns by adding missing as NULL.
    """
    if not pk_requested:
        return []

    resolved = []
    for c in pk_requested:
        if c in src_cols or c in tgt_cols:
            resolved.append(c)
        # else: ignore invalid PK col (exists in neither)
    return resolved


# -----------------------------
# Schema alignment helpers
# -----------------------------
def add_missing_columns_as_null(df, desired_cols, type_map=None):
    """
    Ensure df has all desired_cols. If missing, add as NULL.
    If type_map provided, cast null to that type, else string.
    """
    existing = set(df.columns)
    out = df
    for c in desired_cols:
        if c not in existing:
            dt = None
            if type_map and c in type_map:
                dt = type_map[c]
            if dt:
                out = out.withColumn(c, F.lit(None).cast(dt))
            else:
                out = out.withColumn(c, F.lit(None).cast("string"))
    return out


def align_source_target_schemas(src_df, tgt_df, pk_cols):
    """
    - Add columns present in source but not target -> to target as NULL
    - Add columns present in target but not source -> to source as NULL
    - Return aligned dataframes + compare columns (union - pk)
    """
    src_cols = src_df.columns
    tgt_cols = tgt_df.columns

    # capture types so nulls get correct types where possible
    src_type_map = {f.name: f.dataType.simpleString() for f in src_df.schema.fields}
    tgt_type_map = {f.name: f.dataType.simpleString() for f in tgt_df.schema.fields}

    union_cols = sorted(set(src_cols).union(set(tgt_cols)))

    # Ensure PK cols exist on both sides too (even if only one side had them)
    union_with_pk = sorted(set(union_cols).union(set(pk_cols)))

    src_aligned = add_missing_columns_as_null(src_df, union_with_pk, type_map=src_type_map)
    tgt_aligned = add_missing_columns_as_null(tgt_df, union_with_pk, type_map=tgt_type_map)

    compare_cols = [c for c in union_with_pk if c not in pk_cols]

    return src_aligned, tgt_aligned, compare_cols


# -----------------------------
# Normalization (only when flags True)
# -----------------------------
def normalize_for_compare(col, trim_spaces=False, ignore_case=False, null_equals_empty=False):
    c = col.cast("string")
    if null_equals_empty:
        c = F.coalesce(c, F.lit(""))
    if trim_spaces:
        c = F.trim(c)
    if ignore_case:
        c = F.upper(c)
    return c


def is_numeric_type(simple_str):
    dt = (simple_str or "").lower()
    return any(x in dt for x in ["int", "bigint", "double", "float", "decimal", "smallint", "tinyint", "long", "short"])


def build_match_expr(src_col, tgt_col, src_type_str,
                     trim_spaces=False, ignore_case=False, null_equals_empty=False,
                     numeric_tolerance=0.0):
    tol = float(numeric_tolerance or 0.0)

    if is_numeric_type(src_type_str) and tol > 0:
        s = src_col.cast("double")
        t = tgt_col.cast("double")
        return (
            (s.isNull() & t.isNull()) |
            (s.isNotNull() & t.isNotNull() & (F.abs(s - t) <= F.lit(tol)))
        )

    s_norm = normalize_for_compare(src_col, trim_spaces, ignore_case, null_equals_empty)
    t_norm = normalize_for_compare(tgt_col, trim_spaces, ignore_case, null_equals_empty)
    return s_norm.eqNullSafe(t_norm)


# -----------------------------
# NO-PK surrogate join key
# -----------------------------
def add_surrogate_join_key(df, compare_cols,
                           trim_spaces=False, ignore_case=False, null_equals_empty=False):
    norm_cols = [
        normalize_for_compare(F.col(c), trim_spaces, ignore_case, null_equals_empty)
        for c in compare_cols
    ]
    norm_cols_safe = [F.coalesce(x, F.lit("∅")) for x in norm_cols]
    df2 = df.withColumn("__row_hash", F.sha2(F.concat_ws("||", *norm_cols_safe), 256))

    w = Window.partitionBy("__row_hash").orderBy(F.lit(1))
    df2 = df2.withColumn("__row_num", F.row_number().over(w))
    return df2, ["__row_hash", "__row_num"]


# -----------------------------
# Step 1: create long mismatch rows
# -----------------------------
def compare_source_target_long(
    src_df,
    tgt_df,
    pk_columns,                 # "id" or "id,acct" or list or None/"N/A"
    trim_spaces=False,
    ignore_case=False,
    null_equals_empty=False,
    numeric_tolerance=0.0,
    join_type="full_outer"
):
    pk_requested = parse_pk(pk_columns)

    # Resolve PK against both sides
    resolved_pk = resolve_pk_columns(pk_requested, src_df.columns, tgt_df.columns)

    # Align schemas (also ensures resolved PK exists on both sides)
    src_aligned, tgt_aligned, compare_cols = align_source_target_schemas(src_df, tgt_df, resolved_pk)

    # datatype map for match (prefer source type; fallback to target; else string)
    src_type_map = {f.name: f.dataType.simpleString() for f in src_aligned.schema.fields}
    tgt_type_map = {f.name: f.dataType.simpleString() for f in tgt_aligned.schema.fields}

    def get_type(colname):
        return src_type_map.get(colname) or tgt_type_map.get(colname) or "string"

    # Determine join columns
    s = src_aligned
    t = tgt_aligned
    join_cols = resolved_pk[:]
    output_pk_cols = resolved_pk[:]

    # If no usable PK, use surrogate
    if len(join_cols) == 0:
        s, join_cols = add_surrogate_join_key(s, compare_cols, trim_spaces, ignore_case, null_equals_empty)
        t, _ = add_surrogate_join_key(t, compare_cols, trim_spaces, ignore_case, null_equals_empty)
        output_pk_cols = join_cols[:]

    j = s.alias("s").join(t.alias("t"), on=join_cols, how=join_type)

    pairs = []
    for c in compare_cols:
        src_c = F.col(f"s.`{c}`")
        tgt_c = F.col(f"t.`{c}`")
        dtype = get_type(c)

        match_expr = build_match_expr(
            src_c, tgt_c, dtype,
            trim_spaces=trim_spaces,
            ignore_case=ignore_case,
            null_equals_empty=null_equals_empty,
            numeric_tolerance=numeric_tolerance
        )

        pairs.append(
            F.struct(
                F.lit(c).alias("FIELD_NAME"),
                src_c.cast("string").alias("SRC_STATUS"),
                tgt_c.cast("string").alias("TGT_STATUS"),
                F.when(match_expr, F.lit("Y")).otherwise(F.lit("N")).alias("MATCH_IND")
            )
        )

    long_df = (
        j.select(*[F.col(k) for k in output_pk_cols], F.explode(F.array(*pairs)).alias("x"))
         .select(
             *[F.col(k) for k in output_pk_cols],
             F.col("x.FIELD_NAME"),
             F.col("x.SRC_STATUS"),
             F.col("x.TGT_STATUS"),
             F.col("x.MATCH_IND")
         )
    )

    return long_df, output_pk_cols


# -----------------------------
# Step 2: aggregate to final JSON structure
# -----------------------------
def build_final_json_df(long_df, pk_cols):
    df_fields = long_df.withColumn(
        "field_struct",
        F.struct(
            F.col("FIELD_NAME").cast("string").alias("fieldName"),
            F.col("SRC_STATUS").cast("string").alias("sourceValue"),
            F.col("TGT_STATUS").cast("string").alias("targetValue"),
            F.when(F.col("MATCH_IND") == "Y", F.lit("MATCH")).otherwise(F.lit("MISMATCH")).alias("status")
        )
    )

    grouped = (
        df_fields.groupBy(*pk_cols)
        .agg(
            F.collect_list("field_struct").alias("fields"),
            F.sum(F.when(F.col("MATCH_IND") == "N", F.lit(1)).otherwise(F.lit(0))).cast("int").alias("mismatchCount")
        )
        .withColumn("status", F.when(F.col("mismatchCount") > 0, F.lit("MISMATCH")).otherwise(F.lit("MATCH")))
    )

    pk_struct_cols = [F.col(c).cast("string").alias(c) for c in pk_cols]
    grouped = grouped.withColumn("pk", F.struct(*pk_struct_cols))

    pk_disp_parts = [F.concat(F.lit(f"{c}="), F.col(c).cast("string")) for c in pk_cols]
    grouped = grouped.withColumn("pkDisplay", F.concat_ws(" | ", *pk_disp_parts))

    grouped = grouped.withColumn("fields", F.sort_array(F.col("fields")))

    return grouped.select("pk", "pkDisplay", "mismatchCount", "status", "fields")


# -----------------------------
# End-to-end API
# -----------------------------
def source_target_to_final_json(
    src_df,
    tgt_df,
    pk_columns,
    trim_spaces=False,
    ignore_case=False,
    null_equals_empty=False,
    numeric_tolerance=0.0,
    join_type="full_outer"
):
    long_df, out_pk_cols = compare_source_target_long(
        src_df=src_df,
        tgt_df=tgt_df,
        pk_columns=pk_columns,
        trim_spaces=trim_spaces,
        ignore_case=ignore_case,
        null_equals_empty=null_equals_empty,
        numeric_tolerance=numeric_tolerance,
        join_type=join_type
    )
    final_df = build_final_json_df(long_df, out_pk_cols)
    return long_df, final_df


# -----------------------------
# Example MAIN (replace reads in prod)
# -----------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("FinalJSON_With_PK_From_Either_Side").getOrCreate()

    # Example: Source has client_id/account_id/batch_id, Target missing account_id (simulate)
    src_rows = [
        ("10", "A123", "B55", " SrcVal_881 ", "SrcVal_693", "500001", "500001"),
    ]
    tgt_rows = [
        ("10", None,   "B55", "TgtVal_678", "TgtVal_131", "50001",  "500001"),
    ]

    src_schema = T.StructType([
        T.StructField("client_id", T.StringType(), True),
        T.StructField("account_id", T.StringType(), True),
        T.StructField("batch_id", T.StringType(), True),
        T.StructField("STATUS", T.StringType(), True),
        T.StructField("EMAIL", T.StringType(), True),
        T.StructField("PINCODE", T.StringType(), True),
        T.StructField("ADDRESS", T.StringType(), True),
        # Source-only column
        T.StructField("SRC_ONLY_COL", T.StringType(), True),
    ])

    tgt_schema = T.StructType([
        T.StructField("client_id", T.StringType(), True),
        # target missing account_id in schema (simulate by not defining it)
        T.StructField("batch_id", T.StringType(), True),
        T.StructField("STATUS", T.StringType(), True),
        T.StructField("EMAIL", T.StringType(), True),
        T.StructField("PINCODE", T.StringType(), True),
        T.StructField("ADDRESS", T.StringType(), True),
        # Target-only column
        T.StructField("TGT_ONLY_COL", T.StringType(), True),
    ])

    # Fix tgt rows to match schema (remove account_id field position)
    tgt_rows_fixed = [
        ("10", "B55", "TgtVal_678", "TgtVal_131", "50001", "500001", "TGTVALX"),
    ]
    src_rows_fixed = [
        ("10", "A123", "B55", " SrcVal_881 ", "SrcVal_693", "500001", "500001", "SRCVALX"),
    ]

    source_df = spark.createDataFrame(src_rows_fixed, schema=src_schema)
    target_df = spark.createDataFrame(tgt_rows_fixed, schema=tgt_schema)

    # User provided PK (may exist in one side only)
    pk_columns = "client_id,account_id,batch_id"

    # flags (apply only if True / tol > 0)
    trim_spaces = True
    ignore_case = False
    null_equals_empty = True
    numeric_tolerance = 0.0

    long_df, final_df = source_target_to_final_json(
        src_df=source_df,
        tgt_df=target_df,
        pk_columns=pk_columns,
        trim_spaces=trim_spaces,
        ignore_case=ignore_case,
        null_equals_empty=null_equals_empty,
        numeric_tolerance=numeric_tolerance,
        join_type="full_outer"
    )

    print("=== Long rows (row per field) ===")
    long_df.show(truncate=False)

    print("=== Final nested JSON DF ===")
    final_df.show(truncate=False)

    # Write output JSON (folder of part files)
    output_path = "/tmp/mismatch_final_json"
    final_df.write.mode("overwrite").json(output_path)
    print("✅ Written JSON output to:", output_path)

    spark.stop()
