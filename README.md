# Big-Data-Coursework-2

You need a python environment with pyspark installed, the project was developed on a conda environment so anything similar will work. The raw input lives under data/raw, with the meter readings in meter_readings, the events in meter_events and the dimension files like meters, buildings, regions and tariffs under reference. As long as those are in place you run everything from the project root through run.py.

If you run it the program will try to check if there are new files to go through.

python run.py

The below passed command line option will ensure the program will run on all files from the top and will save over the previous output. (meaning this will be like a fresh run)

python run.py --reprocess

If you only care about specific files you can name them and it will process just those, this overrides the reprocess behaviour.

python run.py --files "meter_readings_2026_01_01.csv,meter_readings_2026_01_02.csv"

Everything the pipeline produces ends up under data/output, with the bronze, silver and gold layers in their own folders, the rejected records under problematic and the run history under control. The final reporting tables are also written out as single csv files in data/output/gold/_csv so they can be opened directly in Power BI without having to deal with the parquet folders.
