import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

RAW_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'raw')
RAW_READINGS_DIR = os.path.join(RAW_DATA_DIR, 'meter_readings') # Readings directory
RAW_EVENTS_DIR = os.path.join(RAW_DATA_DIR, 'meter_events') # Events directory
RAW_DIMENSIONS_DIR = os.path.join(RAW_DATA_DIR, 'reference') # Dimensions directory

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "output")
BRONZE_DIR = os.path.join(OUTPUT_DIR, "bronze")
SILVER_DIR = os.path.join(OUTPUT_DIR, "silver")
GOLD_DIR = os.path.join(OUTPUT_DIR, "gold")
PROBLEMATIC_DIR = os.path.join(OUTPUT_DIR, "problematic")
CONTROL_DIR = os.path.join(OUTPUT_DIR, "control")
CONTROL_FILES = os.path.join(CONTROL_DIR, "processed_files")
CONTROL_RUNS = os.path.join(CONTROL_DIR, "runs")

# Explicit data schema definitions for raw data

READINGS_SCHEMA = ("reading_id STRING, meter_id STRING, reading_timestamp STRING, "
                    "energy_kwh STRING, voltage STRING, current STRING, "
                    "power_factor STRING, reading_type STRING, source_system STRING")
METERS_SCHEMA= ("meter_id STRING, building_id STRING, meter_type STRING, "
                    "installation_date STRING, status STRING, "
                    "expected_reading_frequency_minutes STRING")
BUILDINGS_SCHEMA = ("building_id STRING, region_id STRING, building_type STRING, "
                    "floor_area_sqm STRING, customer_type STRING")
REGIONS_SCHEMA = "region_id STRING, region_name STRING, country STRING, climate_zone STRING"
TARIFFS_SCHEMA = ("tariff_id STRING, region_id STRING, valid_from STRING, valid_to STRING, "
                    "tariff_type STRING, price_per_kwh DOUBLE, peak_period_flag STRING")
EVENTS_SCHEMA = ("event_id STRING, meter_id STRING, event_timestamp STRING, "
                    "event_type STRING, severity STRING, description STRING")

# data rules
TIMESTAMP_FMT = "yyyy-MM-dd HH:mm:ss"
ALLOWED_READING_TYPES = ["actual", "estimated", "corrected", "manual_estimate"]
# A reading is a correction and supersedes other values when it comes from
CORRECTION_SOURCE_SYSTEMS = "manual_correction"
MIN_ENERGY_KWH = 0.0 # no negatives
MAX_ENERGY_KWH = 118.0 # Need to further check data
METER_STATUS_ACTIVE = "active"
MAX_PLAUSIBLE_DATE = "2030-01-01 00:00:00" # no future or broken timestamps
MINUTES_IN_DAY = 24*60
READING_KEY_FIELDS = ["meter_id", "reading_timestamp"] # used for deduplication and corrections