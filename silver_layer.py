
from pyspark.sql import functions as F, Window

import config
# This layer will handle preprocessing and cleaning of the data, such as:
# validation data
# De duplication
# Conforming data types

# Invalid record are not silently dropped, they are written to the rejected output
# Duplicates and corrections are resolved using a window function, for example
# a reading from source_system = "manual_correction" would supersede earlier values for the same, otherwise the latest reading by timestamp would be kept
# Dimension joins are broadcast joins which means no shuffle on readings
# Recomputing from bronze and dynamic partition overwrite by reading_date makes the silver layer idempotent and sorts late or corrected data into the right place



def _reject_reason():
    # Priority order of checks, the first one that matches is the reason for rejection. This is implemented as a when/otherwise chain in Spark.
    meter_id = F.col("meter_id")
    time_stamp = F.col("ts")
    energy = F.col("energy")
    reading_type = F.col("reading_type_norm")
    status = F.col("meter_status")
    return (
        F.when(meter_id.isNull() | (F.trim(meter_id) == ""), "missing_meter_id")
        # meter_status is null only when a (non-empty) meter_id is not in the
        # meters dimension meaning an unknown meter.
        .when(status.isNull(), "unknown_meter_id")
        .when(time_stamp.isNull(), "invalid_timestamp")
        .when(time_stamp > F.to_timestamp(F.lit(config.MAX_PLAUSIBLE_DATE), config.TIMESTAMP_FMT),
            "invalid_timestamp")
        .when(energy.isNull(), "null_energy")
        .when((energy < config.MIN_ENERGY_KWH) | (energy > config.MAX_ENERGY_KWH),
            "invalid_energy")
        .when(~reading_type.isin(config.ALLOWED_READING_TYPES), "invalid_reading_type")
        # the names 'inactive meters' specifically would reject only
        # inactive. Maintenance readings are kept (a meter under maintenance is
        # still a real reporting meter), they can be flagged in Gold layer instead.
        .when(status == "inactive", "inactive_meter")
        .otherwise(None)
    )

def build(spark, dims):
    bronze = spark.read.parquet(config.BRONZE_DIR + "/readings") # read all readings

    meters_dim = dims["meters"].select(
        "meter_id",
        F.col("status").alias("meter_status"),
        "building_id",
    )

    parsed = bronze.withColumn("ts", F.try_to_timestamp(F.col("reading_timestamp"), F.lit(config.TIMESTAMP_FMT))) \
        .withColumn("energy", F.col("energy_kwh").cast("double")) \
        .withColumn("reading_type_norm", F.lower(F.col("reading_type"))) \
        .join(F.broadcast(meters_dim), "meter_id", "left") # broadcast join to avoid shuffle on readings

    tagged = parsed.withColumn("reject_reason", _reject_reason())
    rejected =( tagged.filter(F.col("reject_reason").isNotNull())
            .select(
        "reading_id", "meter_id", "reading_timestamp", "energy_kwh",
        "reading_type", "source_system", "source_file", "ingestion_timestamp",
        "run_id", "reject_reason",
    ))

    valid = tagged.filter(F.col("reject_reason").isNull())

    # De dup and apply corrections, manual correction rows rank first
    # Then latest ingested and finally highest reading_id
    is_corr = F.col("source_system").isin(config.CORRECTION_SOURCE_SYSTEMS)
    window = Window.partitionBy(config.READING_KEY_FIELDS).orderBy(
        F.when(is_corr, 1).otherwise(0).desc(), # corrections first
        F.col("ingestion_timestamp").desc(), # then latest ingested
        F.col("reading_id").desc(), # then highest reading_id
    )
    deduped = valid.withColumn("rank", F.row_number().over(window)).filter(F.col("rank") == 1).drop("rank")

    buildings = dims["buildings"].select(
        "building_id","region_id", "building_type")
    regions = dims["regions"].select("region_id", "region_name")

    clean = (
        deduped
        .withColumn("reading_date", F.to_date("ts"))
        .withColumn("reading_hour", F.hour("ts"))
        .join(F.broadcast(buildings), "building_id", "left")
        .join(F.broadcast(regions), "region_id", "left")
        .select(
            "reading_id", "meter_id", "building_id", "region_id", "region_name",
            "building_type", "ts", "reading_date", "reading_hour",
            F.col("energy").alias("energy_kwh"),
            "voltage", "current", "power_factor",
            F.col("reading_type_norm").alias("reading_type"), "source_system",
            "meter_status",
            "source_file", "ingestion_timestamp", "run_id",
        )
    )
    return clean, rejected

def write_clean(clean):
    (clean.write.mode("overwrite").partitionBy("reading_date")
        .parquet(config.SILVER_DIR + "/readings_clean"))


def write_rejected(rejected):
    (rejected.write.mode("overwrite")
        .parquet(config.PROBLEMATIC_DIR + "/readings"))
