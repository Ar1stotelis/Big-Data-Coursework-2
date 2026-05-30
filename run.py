

import argparse
import glob
import os
import config
import bronze_layer
import silver_layer
import spark_utils
import bronze_layer
def discover_reading_files():
    # Discover all meter_readings_*.csv files in the raw layer
    paths = sorted(glob.glob(os.path.join(config.RAW_READINGS_DIR, "meter_readings_*.csv")))
    return paths

def select_files(spark, args):
    all_files = discover_reading_files()
    if args.files:
        wanted_files = [f.strip() for f in args.files.split(",")]
        return [f for f in all_files if os.path.basename(f) in wanted_files]
    if args.reprocess:
        return all_files
    done = spark_utils.already_processed_files(spark)

    return [f for f in all_files if os.path.basename(f) not in done]

def main():
    ap = argparse.ArgumentParser(description="Energy Data Pipeline")
    ap.add_argument("--reprocess", action="store_true", help="Reprocess all files, ignoring control table")
    ap.add_argument("--files", type=str, help="Comma separated list of specific files to process (overrides --reprocess)")
    args = ap.parse_args()

    spark = spark_utils.get_spark()
    run_id = spark_utils.new_run_id()
    started_at = spark_utils.now_str()

    print(f"Starting run {run_id} at {started_at}")

    files = select_files(spark, args)
    if not files:
        print("No new files to process. Exiting.")
        spark.stop()
        return
    print("processing new files:")
    for f in files:
        print(f" - ", os.path.basename(f))


    # BRONZE LAYER
    print("Ingesting bronze layer...")
    batch_df, file_info = bronze_layer.ingest_readings(spark, files, run_id)
    rows_in = sum(i["rows_in"] for i in file_info)
    bronze_layer.ingest_events(spark, run_id)
    dimensions = bronze_layer.load_dimensions(spark)

    # SILVER LAYER
    print("Building silver layer...")
    clean, rejected, dup_counts = silver_layer.build(spark, dimensions)
    clean = clean.cache() # cache for counting and writing
    rejected = rejected.cache()
    rows_clean = clean.count()
    rows_rejected = rejected.count()
    silver_layer.write_clean(clean)
    silver_layer.write_rejected(rejected)

    # GOLD



if __name__ == "__main__":
    main()