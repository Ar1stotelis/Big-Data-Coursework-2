


# GOLD layer is going to be smaller and more focused on reporting and analytics needs, so we can afford to do more expensive operations here if needed, and we can also be less strict about performance optimizations like avoiding shuffles. This is where we would apply any final business rules, aggregations, or transformations needed for reporting.

# 1 daily_consumption: Daily kWh by region and building type
# 2 meter_completeness: per meter/day completencess vs meter expected count from expected_reading_frequency_minutes
# 3 peak_demand: by region and hour of the day demand with the peak hour flagged
# 4 rejected_summary: rejected record counts by reason
from pyspark.sql import functions as F, Window

import config

def daily_consumption(clean):
    return (
        clean.groupBy("reading_date", "region_name", "building_type")
        .agg(
            F.round(F.sum("energy_kwh"), 3).alias("total_kwh"),
            F.count("*").alias("n_readings"),
            F.count_distinct("meter_id").alias("n_meters"),
        )
        .orderBy("reading_date", "region_name", "building_type")
    )

def meter_completeness(spark, clean):
    # Compare per active meter/day readings against expected count derived from said meters reporting frequency
    # Active meters but zero reading on a day appear as no_data (the missing expected readings check at meter/day granularity)
    meters = (spark.read.schema(config.METERS_SCHEMA).option("header",True)).csv(config.RAW_DIMENSIONS_DIR + "/meters.csv")
    # Active and maintenance meters are kept in sliver and inactive meters are not expected to report anything
    active = (
        meters.filter(F.col("status").isin("active", "maintenance"))
        .withColumn("freq_min",
                    F.col("expected_reading_frequency_minutes").cast("int"))
        .withColumn("readings_expected",
                    (F.lit(config.MINUTES_IN_DAY) / F.col("freq_min")).cast("int"))
        .select("meter_id", F.col("status").alias("meter_status"),
                "readings_expected")
    )

    dates = clean.select("reading_date").distinct()
    grid = active.crossJoin(dates)

    actual = (clean.groupBy("meter_id", "reading_date")
              .agg(F.count("*").alias("readings_received")))

    return (
        grid.join(actual, ["meter_id", "reading_date"], "left")
        .withColumn("readings_received", F.coalesce(F.col("readings_received"), F.lit(0)))
        .withColumn("completeness_pct",
                    F.least(F.lit(100.0),
                            F.round(100.0 * F.col("readings_received")
                                    / F.col("readings_expected"), 1)))
        .withColumn("status",
                    F.when(F.col("readings_received") == 0, "no_data")
                    .when(F.col("completeness_pct") >= 95, "complete")
                    .otherwise("partial"))
        .orderBy("reading_date", "meter_id")
    )

def peak_demand(clean):
    by_hour = clean.groupBy("region_name", "reading_hour").agg(F.round(F.sum("energy_kwh"), 3).alias("total_kwh"))
    window = Window.partitionBy("region_name").orderBy(F.col("total_kwh").desc())
    return (
        by_hour.withColumn("rank_in_region", F.row_number().over(window))
        .withColumn("is_peak_hour", F.col("rank_in_region") == 1)
        .orderBy("region_name", "reading_hour")
    )

def rejected_summary(rejected):
    return rejected.groupBy("reject_reason").agg(F.count("*").alias("n_rejected")).orderBy(F.col("n_rejected").desc())

def meter_health(spark, clean, rejected, dup_counts):
    # one row per meter answering the question which meters report
    # complete and reliable data, and which have missing or invalid or duplicated or late readings

    meters = (spark.read.schema(config.METERS_SCHEMA).option("header", True)
            .csv(config.RAW_DIMENSIONS_DIR + "/meters.csv"))
    expected_meters = (
        meters.filter(F.col("status").isin("active", "maintenance"))
        .withColumn("freq_min", F.col("expected_reading_frequency_minutes").cast("int"))
        .select("meter_id", "building_id",
                F.col("status").alias("meter_status"), "freq_min")
    )

    n_days = clean.select("reading_date").distinct().count()
    n_days = n_days if n_days else 1

    # accepted readings + late counts per meter (from clean)
    clean_agg = (clean.groupBy("meter_id")
                .agg(F.count("*").alias("n_accepted"),
                    F.sum(F.col("is_late_arrival").cast("int")).alias("n_late")))

    # rejected readings per meter (a meter may be the source of several reasons)
    rej_agg = (rejected.filter(F.col("meter_id").isNotNull()
                            & (F.trim("meter_id") != ""))
            .groupBy("meter_id").agg(F.count("*").alias("n_rejected")))

    health = (
        expected_meters
        .withColumn("readings_expected_per_day",
                    (F.lit(config.MINUTES_IN_DAY) / F.col("freq_min")).cast("int"))
        .withColumn("readings_expected",
                    F.col("readings_expected_per_day") * F.lit(n_days))
        .join(clean_agg, "meter_id", "left")
        .join(rej_agg, "meter_id", "left")
        .join(dup_counts, "meter_id", "left")
        .fillna(0, ["n_accepted", "n_late", "n_rejected", "n_duplicate_readings"])
        .withColumn("completeness_pct",
                    F.least(F.lit(100.0),
                            F.round(100.0 * F.col("n_accepted")
                                    / F.col("readings_expected"), 1)))
        .withColumn("reject_rate_pct",
                    F.round(100.0 * F.col("n_rejected")
                            / F.greatest(F.col("n_accepted") + F.col("n_rejected"),
                                        F.lit(1)), 1))
        # work out if a meter is reliable, it needs most of its expected readings and a low reject rate
        .withColumn("reliability_flag",
                    F.when(F.col("n_accepted") == 0, "no_data")
                    .when((F.col("completeness_pct") >= 90)
                        & (F.col("reject_rate_pct") <= 5), "reliable")
                    .when(F.col("completeness_pct") >= 70, "watch")
                    .otherwise("unreliable"))
        .select("meter_id", "building_id", "meter_status",
                "readings_expected", "n_accepted", "completeness_pct",
                "n_rejected", "reject_rate_pct", "n_duplicate_readings", "n_late",
                "reliability_flag")
        .orderBy("completeness_pct", F.col("reject_rate_pct").desc())
    )
    return health


def write(df, name):
    df.write.mode("overwrite").parquet(config.GOLD_DIR + "/" + name)


def write_csv(df, name, out_dir=None):
    # put in single parquet file, then move
    # out to gold/_csv/<name>.csv that power bi can read directly
    import glob, os, shutil
    if out_dir is None:
        out_dir = os.path.join(config.GOLD_DIR, "_csv")
    tmp_dir = os.path.join(out_dir, name + "_tmp")
    os.makedirs(out_dir, exist_ok=True)

    (df.coalesce(1).write.mode("overwrite")
        .option("header", True).csv(tmp_dir))

    part = glob.glob(os.path.join(tmp_dir, "part-*.csv"))[0]
    final = os.path.join(out_dir, name + ".csv")
    if os.path.exists(final):
        os.remove(final)
    shutil.move(part, final)
    shutil.rmtree(tmp_dir)
    return final
