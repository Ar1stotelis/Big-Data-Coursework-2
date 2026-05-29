import os
from pyspark.sql import functions as F

import config
import spark_utils

'''
This file will do the following
- read raw CSV files for a run
- attach pipeline metadata such as source file name, ingestion, timestamp, run id
- write to Parquet files in the bronze layer

Here the purpose is to create an exact and queryable copy of the raw data in the bronze layer,
with the addition of metadata for traceability and auditing.

'''

def ingest_readings(spark, file_paths, run_id):
    if not file_paths:
        print("No files to process.")
        return None, []

    df = (
        spark.read.schema(config.READINGS_SCHEMA)
        .option("header", True)
        .csv(file_paths)

        # Metadata
        .withColumn("source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
        .withColumn("ingestion_timestamp", F.lit(spark_utils.now_str()))
        .withColumn("run_id", F.lit(run_id))
    )

    df = df.cache()  # Cache for reuse in validation and writing

    # per file row counts for control table
    counts = df.groupBy("source_file").count().rdd.collectAsMap()
    file_info = []
    for p in file_paths:
        name = os.path.basename(p)
        file_info.append({
            "file_name": name,
            "file_path": p,
            "status": "success",
            "rows_in": counts.get(name, 0),
        })
    #partition bronze by source_file and use overwrite so that re reading a file reaplce its partition
    # this makes bronze stay idempotent
    df.write.mode("overwrite").partitionBy("source_file").parquet(config.BRONZE_DIR+"/readings")

    return df, file_info

