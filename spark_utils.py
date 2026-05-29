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

