

import argparse
import glob
import os
import config
import bronze_layer
import silver_layer
import spark_utils
import bronze_layer
import gold_layer
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
    print("Building gold layer...")
    gold_daily = gold_layer.daily_consumption(clean)
    gold_complete = gold_layer.meter_completeness(spark, clean)
    gold_peak = gold_layer.peak_demand(clean)
    gold_rejections = gold_layer.rejected_summary(rejected)
    gold_health = gold_layer.meter_health(spark, clean, rejected, dup_counts)
    gold_layer.write(gold_daily, "daily_consumption")
    gold_layer.write(gold_complete, "meter_completeness")
    gold_layer.write(gold_peak, "peak_demand")
    gold_layer.write(gold_rejections, "rejected_summary")
    gold_layer.write(gold_health, "meter_health")

        # Also export single clean CSVs (for Power BI / BI tools) alongside Parquet.
    for df_g, nm in [(gold_daily, "daily_consumption"),
                    (gold_complete, "meter_completeness"),
                    (gold_peak, "peak_demand"),
                    (gold_rejections, "rejected_summary"),
                    (gold_health, "meter_health")]:
        gold_layer.write_csv(df_g, nm)
    # control and tracking

    print("Recording control tables...")
    spark_utils.record_files(spark, run_id, file_info)
    finished_at = spark_utils.now_str()
    spark_utils.record_run(spark, {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": "success",
        "files_processed": len(files),
        "rows_in": rows_in,
        "rows_clean": rows_clean,
        "rows_rejected": rows_rejected,
    })

    # PREVIEWS
    runs_df = spark.read.parquet(config.CONTROL_RUNS)
    gold_layer.write_csv(runs_df, "pipeline_runs", out_dir=config.GOLD_DIR + "/_csv")
    print(f"\nrows_in (this run) = {rows_in} | clean (total) = {rows_clean} | "
            f"rejected (total) = {rows_rejected}")
    print(f"\n{'-'*10}  rejected_summary {'-'*10} ")
    gold_rejections.show(truncate=False)
    print(f"\n{'-'*10} daily_consumption (sample) {'-'*10} ")
    gold_daily.show(10, truncate=False)
    print(f"\n{'-'*10}  meter_health (least reliable 10) {'-'*10} ")
    gold_health.show(10, truncate=False)
    print(f"\n{'-'*10} pipeline_runs {'-'*10}  ")
    spark.read.parquet(config.CONTROL_RUNS).orderBy("started_at").show(truncate=False)

    print(f"\nRun {run_id} finished at {finished_at}")
    spark.stop()





if __name__ == "__main__":
    main()