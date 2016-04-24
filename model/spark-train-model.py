#!/etc/spark/bin/pyspark

import datetime
import os
import pandas

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.mllib.classification import SVMWithSGD
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint

sc = SparkContext("local", "Model Generation ETL", pyFiles=[])
sqlContext = SQLContext(sc)

weather_df = sqlContext.read.load("hdfs://hadoop:9000/weather_data_schema")
station_df = sqlContext.read.load("hdfs://hadoop:9000/station_data_schema")
status_df = sqlContext.read.load("hdfs://hadoop:9000/status_data_schema")

# In particular, we need the "city" column from station_df for each row in
# status_df.
status_stations_df = (status_df
    .join(station_df, station_df.id == status_df.id)
    .select(station_df.id, station_df.dockCount, station_df.city,
        status_df.bikesAvailable, status_df.docksAvailable,
        status_df.date, status_df.time)
    .distinct())
status_stations_df.show()

weather_df.show()

# The big merge! For each status row, find the weather at that location,
# date and time.
status_joined_df = (status_stations_df
    .join(weather_df,
        (status_stations_df.date == weather_df.date) &
        (status_stations_df.time == weather_df.time) &
        (status_stations_df.city == weather_df.city))
    .select(
        status_stations_df.bikesAvailable,
        status_stations_df.docksAvailable,
        weather_df.date,
        weather_df.time,
        weather_df.temperature,
        weather_df.humidity,
        weather_df.pressure,
        weather_df.visibility,
        weather_df.precipIntensity,
        weather_df.windSpeed
    ))

status_joined_df.show()

stats = (status_joined_df
    .agg(
        F.mean(status_joined_df.temperature),
        F.mean(status_joined_df.humidity),
        F.mean(status_joined_df.pressure),

        F.stddev(status_joined_df.temperature),
        F.stddev(status_joined_df.humidity),
        F.stddev(status_joined_df.pressure),
        F.stddev(status_joined_df.visibility),
        F.stddev(status_joined_df.precipIntensity),
        F.stddev(status_joined_df.windSpeed))
    .collect()[0])

print "Statistics: %s" % (stats,)

day_of_week = F.udf(
    lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").weekday(),
    IntegerType())

status_normalized_df = (status_joined_df
    .withColumn(
        "zTemperature", F.abs(status_joined_df.temperature - stats[0]) / stats[3])
    .withColumn(
        "zHumidity", F.abs(status_joined_df.humidity - stats[1]) / stats[4])
    .withColumn(
        "zPressure", F.abs(status_joined_df.pressure - stats[2]) / stats[5])
    .withColumn(
        "zVisibility", F.abs(10 - status_joined_df.visibility) / stats[6])
    .withColumn(
        "zPrecipitation", F.abs(status_joined_df.precipIntensity) / stats[7])
    .withColumn(
        "zWindSpeed", F.abs(status_joined_df.windSpeed) / stats[8])
    .withColumn(
        "dayOfWeek", day_of_week(status_joined_df.date))
    )

status_normalized_df.show()

status_normalized_df.toPandas().to_csv("/root/data/model_status_joined.csv")

positive_instances = status_normalized_df.filter(status_normalized_df.bikesAvailable == 0)
negative_instances = status_normalized_df.filter(status_normalized_df.bikesAvailable > 0)
positive_ratio = positive_instances.count() / float(status_normalized_df.count())
print "Positive ratio: %s/%s = %s" % (positive_instances.count(), status_normalized_df.count(), positive_ratio)

sample_ratio = positive_instances.count() / float(negative_instances.count())
negative_sample = negative_instances.sample(False, sample_ratio)

print "Negative count: %s" % negative_sample.count()

points = positive_instances.unionAll(negative_sample).map(
    lambda r: LabeledPoint(r.bikesAvailable == 0, [
        r.zTemperature,
        r.zHumidity,
        r.zVisibility,
        r.zPressure,
        r.zPrecipitation,
        r.zWindSpeed,
        r.dayOfWeek == 0,
        r.dayOfWeek == 1,
        r.dayOfWeek == 2,
        r.dayOfWeek == 3,
        r.dayOfWeek == 4,
        r.dayOfWeek == 5,
        r.dayOfWeek == 6,
    ]))

print "Total training points: %s" % points.count()

# Model 1: Logistic regression model for detecting when no bikes are available
model = LogisticRegressionWithSGD.train(points, intercept=True)

labelsAndPreds = points.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(points.count())
print "Training error: %s" % trainErr

model.save(sc, "hdfs://hadoop:9000/models/noBikesAvailable.model")

points = (status_normalized_df
    .map(
        lambda r: LabeledPoint(r.bikesAvailable, [
            r.zTemperature,
            r.zHumidity,
            r.zVisibility,
            r.zPressure,
            r.zPrecipitation,
            r.zWindSpeed,
            r.dayOfWeek == 0,
            r.dayOfWeek == 1,
            r.dayOfWeek == 2,
            r.dayOfWeek == 3,
            r.dayOfWeek == 4,
            r.dayOfWeek == 5,
            r.dayOfWeek == 6,
        ])))
print points.collect()[:20]

# Model 2: Linear regression model for detecting number of bikes available
model = SVMWithSGD.train(points, step=1.0, iterations=1, intercept=True)

valuesAndPreds = points.map(lambda p: (float(p.label), float(model.predict(p.features))))
sqlContext.createDataFrame(valuesAndPreds).toPandas().to_csv(
    "/root/data/model_predictions.csv")

print valuesAndPreds.collect()[:20]
MSE = (valuesAndPreds
    .map(lambda (v, p): (v - p)**2)
    .reduce(lambda x, y: x + y) / float(valuesAndPreds.count()))
print "Training error: %s" % MSE

model.save(sc, "hdfs://hadoop:9000/models/noBikesAvailable.model")
