#!/usr/bin/env python

import datetime
import os

import secrets
from etl.weather.forecastio import ForecastIOImport
from model.process_predictions import ForecastModel

ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = "%s/data" % ROOT_DIR

importer_weather = ForecastIOImport(secrets, ROOT_DIR, DATA_DIR)
dataset_name = importer_weather.get_current_weather()

model_wrapper = ForecastModel(ROOT_DIR, DATA_DIR)
model_wrapper.generate_predictions(dataset_name)

print "Done."
