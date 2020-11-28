from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
import pandas as pd 
import geopandas as gpd
import contextily as ctx
#%matplotlib inline
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

appName = "Python Example - PySpark Read CSV"
master = 'local'
daytime_hour = (6, 9)
nighttime_hour = (15, 18)
# Create Spark session
spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

trip_schema = StructType([
    StructField('VendorID', StringType()),
    StructField('lpep_pickup_datetime', TimestampType()),
    StructField('lpep_dropoff_datetime', TimestampType()),
    StructField('store_and_fwd_flag', StringType()),
    StructField('RatecodeID', IntegerType()),
    StructField('PULocationID', IntegerType()),
    StructField('DOLocationID', IntegerType()),
    StructField('passenger_count', IntegerType()),
    StructField('trip_distance', DoubleType()),
    StructField('fare_amount', DoubleType()),
    StructField('extra', DoubleType()),

    StructField('mta_tax', DoubleType()),
    StructField('tip_amount', DoubleType()),
    StructField('tolls_amount', DoubleType()),
    StructField('ehail_fee', DoubleType()),
    StructField('improvement_surcharge', DoubleType()),
    StructField('total_amount', DoubleType()),
    StructField('payment_type', IntegerType()),
    StructField('trip_type', IntegerType()),
    StructField('congestion_surcharge', DoubleType()),
    ])

trip_data = spark.read \
    .option("header", True) \
    .schema(trip_schema) \
    .csv("./data/green/*")
trip_data.printSchema()
# trip_data.write.mode("overwrite").parquet("./values/taxi_green")
# trip_data = spark.read.parquet("./values/taxi_green")
extended_trips = trip_data \
    .withColumn("pick_date", f.to_date(trip_data["lpep_pickup_datetime"])) \
    .withColumn("pick_hour", f.hour(trip_data["lpep_pickup_datetime"]))\
    .withColumn("drop_date", f.to_date(trip_data["lpep_dropoff_datetime"])) \
    .withColumn("drop_hour", f.hour(trip_data["lpep_dropoff_datetime"])) \
    .withColumn("duration", f.unix_timestamp(trip_data["lpep_dropoff_datetime"]) - f.unix_timestamp(trip_data["lpep_pickup_datetime"]))
extended_trips = extended_trips.filter((trip_data["lpep_pickup_datetime"] > '2020-01-01 00:00:00'))

daytime_trips = extended_trips.filter((extended_trips["pick_hour"] >= daytime_hour[0]) & (extended_trips["pick_hour"] <= daytime_hour[1]))
nighttime_trips = extended_trips.filter((extended_trips["pick_hour"] >= nighttime_hour[0]) & (extended_trips["pick_hour"] <= nighttime_hour[1]))

daytime_trips_groupBy_PULocationID = daytime_trips\
    .withColumn("LocationID", daytime_trips["PULocationID"]) \
    .groupBy("PULocationID").agg(
        f.count(daytime_trips["fare_amount"]).alias("Ptrip_count"),
        f.sum(daytime_trips["passenger_count"]).alias("Ppassenger_count"),
        f.sum(daytime_trips["fare_amount"]).alias("Pfare_amount"),
        f.sum(daytime_trips["tip_amount"]).alias("Ptip_amount"),
        f.sum(daytime_trips["total_amount"]).alias("Ptotal_amount"),
        f.avg(daytime_trips["duration"]).alias("Pavg_duration")
    ).sort("PULocationID")
daytime_trips_groupBy_PULocationID = daytime_trips_groupBy_PULocationID.withColumn("LocationID", daytime_trips_groupBy_PULocationID["PULocationID"])

nighttime_trips_groupBy_PULocationID = nighttime_trips\
    .withColumn("LocationID", nighttime_trips["PULocationID"]) \
    .groupBy("PULocationID").agg(
        f.count(nighttime_trips["fare_amount"]).alias("Ptrip_count"),
        f.sum(nighttime_trips["passenger_count"]).alias("Ppassenger_count"),
        f.sum(nighttime_trips["fare_amount"]).alias("Pfare_amount"),
        f.sum(nighttime_trips["tip_amount"]).alias("Ptip_amount"),
        f.sum(nighttime_trips["total_amount"]).alias("Ptotal_amount"),
        f.avg(nighttime_trips["duration"]).alias("Pavg_duration")
    ).sort("PULocationID")
nighttime_trips_groupBy_PULocationID = nighttime_trips_groupBy_PULocationID.withColumn("LocationID", nighttime_trips_groupBy_PULocationID["PULocationID"])


daytime_trips_groupBy_DOLocationID = daytime_trips \
    .groupBy("DOLocationID").agg(
        f.count(daytime_trips["fare_amount"]).alias("Dtrip_count"),
        f.sum(daytime_trips["passenger_count"]).alias("Dpassenger_count"),
        f.sum(daytime_trips["fare_amount"]).alias("Dfare_amount"),
        f.sum(daytime_trips["tip_amount"]).alias("Dtip_amount"),
        f.sum(daytime_trips["total_amount"]).alias("Dtotal_amount"),
        f.avg(daytime_trips["duration"]).alias("Davg_duration")
    ).sort("DOLocationID")
daytime_trips_groupBy_DOLocationID = daytime_trips_groupBy_DOLocationID.withColumn("LocationID", daytime_trips_groupBy_DOLocationID["DOLocationID"])

daytime_trips_pick_and_drop = daytime_trips_groupBy_PULocationID.join(daytime_trips_groupBy_DOLocationID, on="LocationID", how="outer")

daytime_trips_PMD = daytime_trips_pick_and_drop\
    .withColumn("Mtrip_count", daytime_trips_pick_and_drop["Ptrip_count"] - daytime_trips_pick_and_drop["Dtrip_count"])\
    .withColumn("Mpassenger_count", daytime_trips_pick_and_drop["Ppassenger_count"] - daytime_trips_pick_and_drop["Dpassenger_count"])\
    .withColumn("Mfare_amount", daytime_trips_pick_and_drop["Pfare_amount"] - daytime_trips_pick_and_drop["Pfare_amount"])\
    .withColumn("Mtotal_amount", daytime_trips_pick_and_drop["Ptotal_amount"] - daytime_trips_pick_and_drop["Dtotal_amount"])

nighttime_trips_groupBy_DOLocationID = nighttime_trips \
    .groupBy("DOLocationID").agg(
        f.count(nighttime_trips["fare_amount"]).alias("Dtrip_count"),
        f.sum(nighttime_trips["passenger_count"]).alias("Dpassenger_count"),
        f.sum(nighttime_trips["fare_amount"]).alias("Dfare_amount"),
        f.sum(nighttime_trips["tip_amount"]).alias("Dtip_amount"),
        f.sum(nighttime_trips["total_amount"]).alias("Dtotal_amount"),
        f.avg(nighttime_trips["duration"]).alias("Davg_duration")
    ).sort("DOLocationID")
nighttime_trips_groupBy_DOLocationID = nighttime_trips_groupBy_DOLocationID.withColumn("LocationID", nighttime_trips_groupBy_DOLocationID["DOLocationID"])

nighttime_trips_pick_and_drop = nighttime_trips_groupBy_PULocationID.join(nighttime_trips_groupBy_DOLocationID, on="LocationID", how="outer")

nighttime_trips_PMD = nighttime_trips_pick_and_drop\
    .withColumn("Mtrip_count", nighttime_trips_pick_and_drop["Ptrip_count"] - nighttime_trips_pick_and_drop["Dtrip_count"])\
    .withColumn("Mpassenger_count", nighttime_trips_pick_and_drop["Ppassenger_count"] - nighttime_trips_pick_and_drop["Dpassenger_count"])\
    .withColumn("Mfare_amount", nighttime_trips_pick_and_drop["Pfare_amount"] - nighttime_trips_pick_and_drop["Pfare_amount"])\
    .withColumn("Mtotal_amount", nighttime_trips_pick_and_drop["Ptotal_amount"] - nighttime_trips_pick_and_drop["Dtotal_amount"])


plt.figure(1)
fig, ax = plt.subplots(1, 1,figsize=(40,40))
world = gpd.read_file('./map/taxi_zones/taxi_zones.shp')
world_PMD = world.merge(daytime_trips_PMD.toPandas(), on = "LocationID", how = "outer")
world_PMD.plot(column= 'Mpassenger_count', ax=ax, legend=True, cmap='hot')
plt.savefig("./plot/map_passengaer_count_pick_minus_drop_daytime" + str(daytime_hour[0]) + "_" + str(daytime_hour[1]) + ".png")

plt.figure(2)
fig, ax = plt.subplots(1, 1,figsize=(40,40))
world = gpd.read_file('./map/taxi_zones/taxi_zones.shp')
world_PMD_night = world.merge(nighttime_trips_PMD.toPandas(), on = "LocationID", how = "outer")
world_PMD_night.plot(column= 'Mpassenger_count', ax=ax, legend=True, cmap='hot')
plt.savefig("./plot/map_passengaer_count_pick_minus_drop_nighttime" + str(nighttime_hour[0]) + "_" + str(nighttime_hour[1]) + ".png")

print(daytime_trips.toPandas())