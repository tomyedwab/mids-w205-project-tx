#!/usr/bin/python

import datetime
import subprocess

import secrets
from etl.weather.forecastio import ForecastIOImport

START_DATE = datetime.datetime(2013, 8, 29)
END_DATE = datetime.datetime(2015, 8, 31)
MAX_REQUESTS = 1
DATA_DIR = "/vagrant/data"

importer = ForecastIOImport(secrets, DATA_DIR)
importer.do_import(START_DATE, END_DATE, MAX_REQUESTS)
#importer.copy_to_hdfs()

# Copy the raw bikeshare data to HDFS as well
print "Copying bikeshare data to HDFS..."
subprocess.call(
    "/vagrant/hadoop/hadoop-hdfs.sh "
    "dfs -put %s/bikeshare_raw hdfs://hadoop:9000/" % DATA_DIR,
    shell=True
)
print "Done."
