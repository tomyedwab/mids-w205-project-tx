#!/etc/spark/bin/pyspark

import datetime
import os
import pandas

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.mllib.classification import SVMWithSGD
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
        status_stations_df.id,
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

stats_df = (status_joined_df
    .agg(
        F.mean(status_joined_df.temperature).alias("avgTemp"),
        F.mean(status_joined_df.humidity).alias("avgHumidity"),
        F.mean(status_joined_df.pressure).alias("avgPressure"),

        F.stddev(status_joined_df.temperature).alias("stddevTemp"),
        F.stddev(status_joined_df.humidity).alias("stddevHumidity"),
        F.stddev(status_joined_df.pressure).alias("stddevPressure"),
        F.stddev(status_joined_df.visibility).alias("stddevVisibility"),
        F.stddev(status_joined_df.precipIntensity).alias("stddevPrecipitation"),
        F.stddev(status_joined_df.windSpeed).alias("stddevWindSpeed")))

stats_df.write.mode('overwrite').parquet("hdfs://hadoop:9000/models/weather-stats")

stats = stats_df.collect()[0]

print "Statistics: %s" % (stats,)

day_of_week = F.udf(
    lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").weekday(),
    IntegerType())

status_normalized_df = (status_joined_df
    .withColumn(
        "zTemperature", F.abs(status_joined_df.temperature - stats.avgTemp) / stats.stddevTemp)
    .withColumn(
        "zHumidity", F.abs(status_joined_df.humidity - stats.avgHumidity) / stats.stddevHumidity)
    .withColumn(
        "zPressure", F.abs(status_joined_df.pressure - stats.avgPressure) / stats.stddevPressure)
    .withColumn(
        "zVisibility", F.abs(10 - status_joined_df.visibility) / stats.stddevVisibility)
    .withColumn(
        "zPrecipitation", F.abs(status_joined_df.precipIntensity) / stats.stddevPrecipitation)
    .withColumn(
        "zWindSpeed", F.abs(status_joined_df.windSpeed) / stats.stddevWindSpeed)
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
    lambda r: LabeledPoint(r.bikesAvailable == 0,
        [r.id == idx for idx in xrange(100)] + [
            r.zTemperature,
            r.zHumidity,
            r.zVisibility,
            r.zPressure,
            r.zPrecipitation,
            r.zWindSpeed
        ] + [r.dayOfWeek == idx for idx in xrange(7)]))

print "Total training points: %s" % points.count()

# Model 1: Support vector machine regression model for detecting when no bikes are available
model = SVMWithSGD.train(points, intercept=True)

labelsAndPreds = points.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(points.count())
print "Training error: %s" % trainErr

model.save(sc, "hdfs://hadoop:9000/models/noBikesAvailable.model")
