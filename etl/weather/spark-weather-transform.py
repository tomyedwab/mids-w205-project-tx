#!/etc/spark/bin/pyspark

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

sc = SparkContext("local", "Weather ETL", pyFiles=[])
sqlContext = SQLContext(sc)

weather_csv = (sc.textFile("hdfs://hadoop:9000/weather.csv")
    .map(lambda line: line.split(","))
    .filter(lambda line: len(line)>1)
    .map(lambda line: (
        line[0], line[1], float(line[2] or "NaN"), float(line[3] or "NaN"),
        line[4], line[5], float(line[6] or "NaN"), float(line[7] or "NaN"),
        float(line[8] or "NaN"), float(line[9] or "NaN"),
        float(line[10] or "NaN"), float(line[11] or "NaN"),
        float(line[12] or "NaN"), float(line[13] or "NaN")
    )))

fields = [
    StructField("date", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("time", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("precipIntensity", FloatType(), True),
    StructField("precipProbability", FloatType(), True),
    StructField("temperature", FloatType(), True),
    StructField("apparentTemperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("windSpeed", FloatType(), True),
    StructField("visibility", FloatType(), True),
    StructField("pressure", FloatType(), True),
]
schema = StructType(fields)

# Apply the schema to the RDD.
weather_df = sqlContext.createDataFrame(weather_csv, schema)
print "#######"
print weather_df.printSchema()
print weather_df.show()
print "#######"

# Do some simple aggregations
weather_agg = weather_df.groupBy("date", "city").agg(
    F.min(weather_df.temperature), F.max(weather_df.temperature),
    F.min(weather_df.pressure), F.max(weather_df.pressure))
weather_agg.show()
