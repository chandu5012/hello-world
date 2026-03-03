from pyspark.sql import SparkSession
import os
import subprocess

# --------------------------------------------------
# INPUTS
# --------------------------------------------------
file_location = "hadoop"            # unix / hadoop  (THIS decides copy or direct read)
file_path = "/tmp/sample.csv"       # unix: /tmp/... or /apps/run/...
                                   # hadoop: /tmp/... (HDFS) or /user/... etc.
file_type = "csv"                   # csv / json / excel
csv_delimiter = ""                  # if "|" give "|", if empty -> auto-detect (| ; \t ,)
sheet_name = "Sheet1"               # excel only
hdfs_base_dir = "/tmp"              # Hadoop directory is /tmp (HDFS)
multiline_json = "true"

# --------------------------------------------------
# Start Spark (to detect Scala for spark-excel)
# --------------------------------------------------
spark = SparkSession.builder.appName("ReadAnyFile").getOrCreate()

scala_bin = "2.12"
try:
    scala_full = spark.sparkContext._jvm.scala.util.Properties.versionNumberString()
    scala_bin = ".".join(scala_full.split(".")[0:2])  # "2.11" or "2.12"
except:
    pass

# Spark 2.4.8 -> Scala 2.11 -> use 0.12.2
# Spark 3.3   -> Scala 2.12 -> use 0.13.x
if scala_bin == "2.11":
    excel_pkg = "com.crealytics:spark-excel_2.11:0.12.2"
else:
    excel_pkg = "com.crealytics:spark-excel_2.12:0.13.7"

spark.stop()

spark = SparkSession.builder \
    .appName("ReadAnyFile") \
    .config("spark.jars.packages", excel_pkg) \
    .getOrCreate()

print("Scala:", scala_bin, "| Excel package:", excel_pkg)

# --------------------------------------------------
# HANDLE FILE LOCATION (unix vs hadoop)
# IMPORTANT: Hadoop path can also be /tmp, so NEVER detect by path.
# --------------------------------------------------
loc = file_location.strip().lower()

if loc == "unix":
    # Local Unix file (/tmp/... or /apps/run/...)
    if not os.path.exists(file_path):
        raise Exception("Unix file not found: {}".format(file_path))

    # Ensure HDFS /tmp exists (normally exists, but safe)
    subprocess.check_call(["hdfs", "dfs", "-mkdir", "-p", hdfs_base_dir])

    file_name = os.path.basename(file_path)
    hdfs_path = hdfs_base_dir.rstrip("/") + "/" + file_name

    # Copy local -> HDFS /tmp (overwrite)
    subprocess.check_call(["hdfs", "dfs", "-copyFromLocal", "-f", file_path, hdfs_path])

    print("Copied Unix file to HDFS:", hdfs_path)
    file_path_to_read = hdfs_path

elif loc == "hadoop":
    # Already on HDFS (even if it is /tmp)
    file_path_to_read = file_path
    print("Using Hadoop file path directly:", file_path_to_read)

else:
    raise ValueError("file_location must be 'unix' or 'hadoop'")

# --------------------------------------------------
# READ FILE
# --------------------------------------------------
ft = file_type.strip().lower()

if ft == "csv":
    # 1) If delimiter provided -> use it
    # 2) If not provided -> auto detect from first line in HDFS
    # 3) Fallback -> comma
    if csv_delimiter and csv_delimiter.strip() != "":
        delimiter_to_use = csv_delimiter
        print("Using provided delimiter:", delimiter_to_use)
    else:
        print("No delimiter provided. Auto detecting...")

        first_line = spark.sparkContext.textFile(file_path_to_read).first()

        if "|" in first_line:
            delimiter_to_use = "|"
        elif ";" in first_line:
            delimiter_to_use = ";"
        elif "\t" in first_line:
            delimiter_to_use = "\t"
        else:
            delimiter_to_use = ","  # fallback

        print("Auto detected delimiter:", delimiter_to_use)

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", delimiter_to_use) \
        .option("quote", '"') \
        .option("escape", '"') \
        .csv(file_path_to_read)

elif ft == "json":
    df = spark.read \
        .option("multiLine", multiline_json) \
        .json(file_path_to_read)

elif ft in ("excel", "xlsx", "xls"):
    df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("treatEmptyValuesAsNulls", "true") \
        .option("sheetName", sheet_name) \
        .load(file_path_to_read)

else:
    raise ValueError("Unsupported file_type. Use csv/json/excel")

# --------------------------------------------------
# VALIDATE
# --------------------------------------------------
print("Reading from:", file_path_to_read)
df.printSchema()
df.show(10, truncate=False)
print("Row count:", df.count())
