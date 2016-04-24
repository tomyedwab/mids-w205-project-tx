#!/etc/spark/bin/pyspark

import datetime
import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.mllib.classification import SVMModel

dataset_name = sys.argv[1]

sc = SparkContext("local", "Model Prediction", pyFiles=[])
sqlContext = SQLContext(sc)

# First load the model we saved in the model generation step
model = SVMModel.load(sc, "hdfs://hadoop:9000/models/noBikesAvailable.model")

# We also need the stats used to normalize the weather variables
stats_df = sqlContext.read.load("hdfs://hadoop:9000/models/weather-stats")
stats = stats_df.collect()[0]

# We want to produce output for each station
station_df = sqlContext.read.load("hdfs://hadoop:9000/station_data_schema")

print "Statistics: %s" % (stats,)

# Load the weather data

current_weather_csv = (
    sc.textFile("hdfs://hadoop:9000/current_weather/%s.csv" % dataset_name)
    .map(lambda line: line.split(","))
    .map(lambda line: (
        line[0], line[1], line[2],
        float(line[3]), float(line[4]), float(line[5]),
        float(line[6]), float(line[7]), float(line[8])
    )))

fields = [
    StructField("city", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("visibility", FloatType(), True),
    StructField("pressure", FloatType(), True),
    StructField("precipIntensity", FloatType(), True),
    StructField("windSpeed", FloatType(), True),
]
schema = StructType(fields)

input_df = sqlContext.createDataFrame(current_weather_csv, schema)

# Normalize the variables
day_of_week = F.udf(
    lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").weekday(),
    IntegerType())

weather_normalized_df = (input_df
    .withColumn(
        "zTemperature", F.abs(input_df.temperature - stats.avgTemp) / stats.stddevTemp)
    .withColumn(
        "zHumidity", F.abs(input_df.humidity - stats.avgHumidity) / stats.stddevHumidity)
    .withColumn(
        "zPressure", F.abs(input_df.pressure - stats.avgPressure) / stats.stddevPressure)
    .withColumn(
        "zVisibility", F.abs(10 - input_df.visibility) / stats.stddevVisibility)
    .withColumn(
        "zPrecipitation", F.abs(input_df.precipIntensity) / stats.stddevPrecipitation)
    .withColumn(
        "zWindSpeed", F.abs(input_df.windSpeed) / stats.stddevWindSpeed)
    .withColumn(
        "dayOfWeek", day_of_week(input_df.date))
    )

weather_joined_df = (weather_normalized_df
    .join(station_df, station_df.city == weather_normalized_df.city)
    .select(
        station_df.id,
        station_df.name,
        station_df.city,
        station_df.dockCount,
        weather_normalized_df.zTemperature,
        weather_normalized_df.zHumidity,
        weather_normalized_df.zPressure,
        weather_normalized_df.zVisibility,
        weather_normalized_df.zPrecipitation,
        weather_normalized_df.zWindSpeed,
        weather_normalized_df.dayOfWeek
    ))

weather_joined_df.show()

predict_udf = F.udf(
    lambda id, temp, hum, vis, press, prec, wind, day: model.predict(
        [id == idx for idx in xrange(100)] + [
            temp, hum, vis, press, prec, wind
        ] + [day == idx for idx in xrange(7)]))

output_df = weather_joined_df.withColumn(
    "prediction", predict_udf(
        weather_joined_df.id,
        weather_joined_df.zTemperature,
        weather_joined_df.zHumidity,
        weather_joined_df.zVisibility,
        weather_joined_df.zPressure,
        weather_joined_df.zPrecipitation,
        weather_joined_df.zWindSpeed,
        weather_joined_df.dayOfWeek
    ))
output_df.show()

output_df.rdd.saveAsTextFile("hdfs://hadoop:9000/predictions/%s" % dataset_name)
print "Done."
