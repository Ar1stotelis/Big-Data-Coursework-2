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
