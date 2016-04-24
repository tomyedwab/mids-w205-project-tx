#!/usr/bin/env python

import datetime
import os

import secrets
from etl.weather.forecastio import ForecastIOImport
from etl.bikeshare.bikeshare import BikeshareImport

START_DATE = datetime.datetime(2013, 8, 29)
END_DATE = datetime.datetime(2015, 8, 31)
MAX_REQUESTS = 1000
ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = "%s/data" % ROOT_DIR

importer_weather = ForecastIOImport(secrets, ROOT_DIR, DATA_DIR)
importer_weather.do_import(START_DATE, END_DATE, MAX_REQUESTS)
importer_weather.copy_to_hdfs()

importer_bikeshare = BikeshareImport(ROOT_DIR, DATA_DIR)
importer_bikeshare.do_import()

print "Done."
