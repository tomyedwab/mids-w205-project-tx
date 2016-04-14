#!/usr/bin/python

import datetime

import secrets
from etl.weather.forecastio import ForecastIOImport
from etl.bikeshare.bikeshare import BikeshareImport

START_DATE = datetime.datetime(2013, 8, 29)
END_DATE = datetime.datetime(2015, 8, 31)
MAX_REQUESTS = 1
DATA_DIR = "/vagrant/data"

importer_weather = ForecastIOImport(secrets, DATA_DIR)
importer_weather.do_import(START_DATE, END_DATE, MAX_REQUESTS)
#importer_weather.copy_to_hdfs()

importer_bikeshare = BikeshareImport(DATA_DIR)
importer_bikeshare.do_import()

print "Done."
