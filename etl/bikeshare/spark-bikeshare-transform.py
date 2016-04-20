#!/etc/spark/bin/pyspark

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

sc = SparkContext("local", "Bike Share ETL", pyFiles=[])
sqlContext = SQLContext(sc)

# Parse CSV and convert relevant fields to float
# Transform spaces in city names to underscores
station_csv = (sc.textFile("hdfs://hadoop:9000/station_data")
    .map(lambda line: line.split(","))
    .filter(lambda line: len(line)>1)
    .map(lambda line: (
        int(line[0]), line[1], float(line[2]), float(line[3]),
        int(line[4]), line[5].replace(" ", "_"), line[6]
    )))

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("dockCount", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("installed", StringType(), True),
])

# Apply the schema to the RDD.
station_df = sqlContext.createDataFrame(station_csv, schema)
print "#######"
print station_df.printSchema()
station_df.show()
print "#######"

status_csv = (sc.textFile("hdfs://hadoop:9000/status_data")
    .map(lambda line: line.split(","))
    .filter(lambda line: len(line)>1)
    .map(lambda line: (
        int(line[0].replace("\"", "")),
        int(line[1].replace("\"", "")),
        int(line[2].replace("\"", "")),
        line[3][1:11],
        line[3][12:17],
    )))

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("bikesAvailable", IntegerType(), True),
    StructField("docksAvailable", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
])

# Apply the schema to the RDD.
status_df = sqlContext.createDataFrame(status_csv, schema)
print "#######"
print status_df.printSchema()
status_df.show()
print "#######"

# Filter just the status entries that correspond to the first minute of an hour
def on_the_hour(time):
    return time.endswith(":00")

status_df_filtered = status_df.filter(F.udf(on_the_hour, BooleanType())(F.col("time")))
status_df_filtered.show()

# Save the new dataframes with schemas
station_df.write.save(
    "hdfs://hadoop:9000/station_data_schema", mode="overwrite")
status_df_filtered.write.save(
    "hdfs://hadoop:9000/status_data_schema", mode="overwrite")
