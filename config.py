import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'raw')
RAW_READINGS_DIR = os.path.join(RAW_DATA_DIR, 'meter_readings') # Readings directory
RAW_EVENTS_DIR = os.path.join(RAW_DATA_DIR, 'meter_events') # Events directory
RAW_DIMENSIONS_DIR = os.path.join(RAW_DATA_DIR, 'dimensions') # Dimensions directory

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
BRONZE_DIR = os.path.join(OUTPUT_DIR, "bronze")
SILVER_DIR = os.path.join(OUTPUT_DIR, "silver")
GOLD_DIR = os.path.join(OUTPUT_DIR, "gold")
PROBLEMATIC_DIR = os.path.join(OUTPUT_DIR, "problematic")


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