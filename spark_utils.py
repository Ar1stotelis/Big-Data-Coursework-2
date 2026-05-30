from datetime import datetime
import os
import uuid

from pyspark.sql import SparkSession, functions as F, types as T
import config

def get_spark():
    spark = (
        SparkSession.builder
        .appName("Energy Data Pipeline")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
def new_run_id():
    return datetime.now().strftime("%Y%m%d%H%M%S") + '_' + uuid.uuid4().hex[:6]

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _read_control(spark, path, schema):
    if os.path.exists(path):
        return spark.read.parquet(path)
    return spark.createDataFrame([], schema)

def already_processed_files(spark):
    # Get list of already processed files
    df = _read_control(spark, config.CONTROL_FILES, FILE_SCHEMA)
    rows = df.filter(F.col("status") == "success").select("file_name").distinct().collect()
    return {r["file_name"] for r in rows}


RUN_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("started_at", T.StringType()),
    T.StructField("finished_at", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("files_processed", T.IntegerType()),
    T.StructField("rows_in", T.LongType()),
    T.StructField("rows_clean", T.LongType()),
    T.StructField("rows_rejected", T.LongType()),
])

FILE_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("file_name", T.StringType()),
    T.StructField("file_path", T.StringType()),
    T.StructField("processed_at", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("rows_in", T.LongType()),
])

def record_files(spark, run_id, file_infos):
    # file_infos: list of dicts {file_name, file_path, status, rows_in}
    if not file_infos:
        return
    rows = [(run_id, fi["file_name"], fi["file_path"], now_str(),
            fi["status"], int(fi["rows_in"])) for fi in file_infos]
    df = spark.createDataFrame(rows, FILE_SCHEMA)
    df.write.mode("append").parquet(config.CONTROL_FILES)


def record_run(spark, run):
    # run: dict matching RUN_SCHEMA fields
    rows = [(run["run_id"], run["started_at"], run["finished_at"], run["status"],
            int(run["files_processed"]), int(run["rows_in"]),
            int(run["rows_clean"]), int(run["rows_rejected"]))]
    df = spark.createDataFrame(rows, RUN_SCHEMA)
    df.write.mode("append").parquet(config.CONTROL_RUNS)
