
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