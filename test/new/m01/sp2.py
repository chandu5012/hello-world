from pyspark.sql import SparkSession
import os
import subprocess
import shutil

# --------------------------------------------------
# INPUTS (READ)
# --------------------------------------------------
file_location = "hadoop"              # unix / hadoop
file_path = "/tmp/sample.csv"         # unix: /tmp/... or /apps/run/...
                                     # hadoop: /tmp/... (HDFS) or /user/... etc.
file_type = "csv"                     # csv / json / excel
csv_delimiter = ""                    # if "|" give "|", if empty -> auto-detect (| ; \t ,)
sheet_name = "Sheet1"                 # excel only
multiline_json = "true"               # json pretty-print / multi-line json support

# --------------------------------------------------
# OUTPUTS (WRITE)
# --------------------------------------------------
output_type = "csv"                   # csv / json / parquet / orc
output_mode = "overwrite"             # overwrite / append
hdfs_output_base_dir = "/tmp"         # HDFS base folder (you said Hadoop dir is /tmp)
unix_output_dir = "/tmp"              # Unix folder to copy final output into

# For CSV output delimiter (can reuse input delimiter or set separately)
output_csv_delimiter = csv_delimiter if csv_delimiter else ""

# --------------------------------------------------
# Start Spark (to detect Scala for spark-excel)
# --------------------------------------------------
spark = SparkSession.builder.appName("ReadWriteAnyFile").getOrCreate()

scala_bin = "2.12"
try:
    scala_full = spark.sparkContext._jvm.scala.util.Properties.versionNumberString()
    scala_bin = ".".join(scala_full.split(".")[0:2])  # "2.11" or "2.12"
except:
    pass

# Spark 2.4.8 -> Scala 2.11 -> excel 0.12.2
# Spark 3.3   -> Scala 2.12 -> excel 0.13.x
if scala_bin == "2.11":
    excel_pkg = "com.crealytics:spark-excel_2.11:0.12.2"
else:
    excel_pkg = "com.crealytics:spark-excel_2.12:0.13.7"

spark.stop()

spark = SparkSession.builder \
    .appName("ReadWriteAnyFile") \
    .config("spark.jars.packages", excel_pkg) \
    .getOrCreate()

print("Scala:", scala_bin, "| Excel package:", excel_pkg)

# --------------------------------------------------
# HANDLE INPUT LOCATION (unix vs hadoop)
# IMPORTANT: Hadoop path can also be /tmp, so trust file_location arg only.
# --------------------------------------------------
loc = file_location.strip().lower()

if loc == "unix":
    if not os.path.exists(file_path):
        raise Exception("Unix file not found: {}".format(file_path))

    # Ensure HDFS base dir exists
    subprocess.check_call(["hdfs", "dfs", "-mkdir", "-p", hdfs_output_base_dir])

    in_name = os.path.basename(file_path)
    hdfs_in_path = hdfs_output_base_dir.rstrip("/") + "/" + in_name

    subprocess.check_call(["hdfs", "dfs", "-copyFromLocal", "-f", file_path, hdfs_in_path])

    print("Copied Unix file to HDFS:", hdfs_in_path)
    file_path_to_read = hdfs_in_path

elif loc == "hadoop":
    file_path_to_read = file_path
    print("Using Hadoop file path directly:", file_path_to_read)

else:
    raise ValueError("file_location must be 'unix' or 'hadoop'")

# --------------------------------------------------
# READ FILE
# --------------------------------------------------
ft = file_type.strip().lower()

if ft == "csv":
    # delimiter: use provided, else auto-detect from first line (in HDFS), else default ","
    if csv_delimiter and csv_delimiter.strip() != "":
        delimiter_to_use = csv_delimiter
        print("CSV: Using provided delimiter:", delimiter_to_use)
    else:
        print("CSV: No delimiter provided. Auto detecting...")
        first_line = spark.sparkContext.textFile(file_path_to_read).first()

        if "|" in first_line:
            delimiter_to_use = "|"
        elif ";" in first_line:
            delimiter_to_use = ";"
        elif "\t" in first_line:
            delimiter_to_use = "\t"
        else:
            delimiter_to_use = ","

        print("CSV: Auto detected delimiter:", delimiter_to_use)

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

print("Reading from:", file_path_to_read)
df.printSchema()
print("Source Row count:", df.count())

# --------------------------------------------------
# PREP OUTPUT NAMES (based on input file name)
# --------------------------------------------------
input_file_name = os.path.basename(file_path_to_read)       # sample.csv
input_base_name = input_file_name.split(".")[0]             # sample

out_t = output_type.strip().lower()

# HDFS output folder: /tmp/<input_base_name>
hdfs_out_dir = hdfs_output_base_dir.rstrip("/") + "/" + input_base_name

# For CSV/JSON, we will create a merged HDFS file: /tmp/<name>.<ext>
# Then copy to Unix: /tmp/<name>.<ext>
if out_t == "csv":
    merged_ext = "csv"
elif out_t == "json":
    merged_ext = "json"
else:
    merged_ext = None  # parquet/orc are folders

# --------------------------------------------------
# WRITE TO HDFS (folder)
# --------------------------------------------------
print("Writing to HDFS folder:", hdfs_out_dir)

# delete HDFS output folder if overwrite
if output_mode.lower() == "overwrite":
    subprocess.call(["hdfs", "dfs", "-rm", "-r", "-f", hdfs_out_dir])

write_df = df

# For text outputs (csv/json), make single part file so merge/copy is clean
if out_t in ("csv", "json"):
    write_df = df.coalesce(1)

if out_t == "csv":
    # output delimiter: if not given, use detected delimiter or default ","
    if output_csv_delimiter and output_csv_delimiter.strip() != "":
        out_delim = output_csv_delimiter
    else:
        # reuse delimiter_to_use if it exists, else comma
        try:
            out_delim = delimiter_to_use
        except:
            out_delim = ","

    (write_df.write
        .mode(output_mode)
        .option("header", "true")
        .option("delimiter", out_delim)
        .option("quote", '"')
        .option("escape", '"')
        .csv(hdfs_out_dir))

elif out_t == "json":
    (write_df.write
        .mode(output_mode)
        .json(hdfs_out_dir))

elif out_t == "parquet":
    (write_df.write
        .mode(output_mode)
        .parquet(hdfs_out_dir))

elif out_t == "orc":
    (write_df.write
        .mode(output_mode)
        .orc(hdfs_out_dir))

else:
    raise ValueError("Unsupported output_type. Use csv/json/parquet/orc")

print("Write completed to HDFS folder:", hdfs_out_dir)

# --------------------------------------------------
# COPY OUTPUT TO UNIX /tmp
# --------------------------------------------------
if out_t in ("csv", "json"):
    # 1) merge into single HDFS file
    hdfs_merged_file = hdfs_output_base_dir.rstrip("/") + "/" + input_base_name + "." + merged_ext
    subprocess.call(["hdfs", "dfs", "-rm", "-f", hdfs_merged_file])

    print("Merging HDFS part files into single file:", hdfs_merged_file)
    subprocess.check_call(["hdfs", "dfs", "-getmerge", hdfs_out_dir, hdfs_merged_file])

    # 2) copy merged file to Unix /tmp
    unix_final_file = unix_output_dir.rstrip("/") + "/" + input_base_name + "." + merged_ext
    print("Copying merged file from HDFS to Unix:", unix_final_file)

    # Ensure unix folder exists
    if not os.path.exists(unix_output_dir):
        os.makedirs(unix_output_dir)

    # Overwrite local file if exists
    if os.path.exists(unix_final_file):
        os.remove(unix_final_file)

    subprocess.check_call(["hdfs", "dfs", "-get", "-f", hdfs_merged_file, unix_final_file])

    print("✅ Final Unix file created:", unix_final_file)

elif out_t in ("parquet", "orc"):
    # Parquet/ORC are directories; copy directory to Unix /tmp
    unix_dir_target = unix_output_dir.rstrip("/") + "/" + input_base_name + "_" + out_t
    print("Copying HDFS directory to Unix folder:", unix_dir_target)

    # Clean local target folder if exists
    if os.path.exists(unix_dir_target):
        shutil.rmtree(unix_dir_target)

    os.makedirs(unix_dir_target)

    # Copy HDFS folder -> local folder (creates folder inside target; so use target parent)
    # We'll copy into unix_output_dir then rename if needed
    tmp_parent = unix_output_dir.rstrip("/")
    subprocess.check_call(["hdfs", "dfs", "-get", "-f", hdfs_out_dir, tmp_parent])

    # After get, local folder will be: /tmp/<input_base_name>
    local_downloaded = tmp_parent + "/" + input_base_name
    if os.path.exists(local_downloaded):
        # rename to include format suffix
        if os.path.exists(unix_dir_target):
            shutil.rmtree(unix_dir_target)
        os.rename(local_downloaded, unix_dir_target)

    print("✅ Final Unix folder created:", unix_dir_target)

else:
    raise ValueError("Unsupported output_type. Use csv/json/parquet/orc")

print("DONE.")
