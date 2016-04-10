#!/usr/bin/python

import datetime

import secrets
from etl.weather.forecastio import ForecastIOImport

START_DATE = datetime.datetime(2013, 8, 29)
END_DATE = datetime.datetime(2015, 8, 31)
MAX_REQUESTS = 1000

importer = ForecastIOImport(secrets, "/vagrant/data")
importer.do_import(START_DATE, END_DATE, MAX_REQUESTS)
importer.copy_to_hdfs()
