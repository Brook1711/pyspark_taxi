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

def add_basemap(ax, zoom, url='http://tile.stamen.com/terrain/tileZ/tileX/tileY.png'):
    xmin, xmax, ymin, ymax = ax.axis()
    basemap, extent = ctx.bounds2img(xmin, ymin, xmax, ymax, zoom=zoom, url=url)
    ax.imshow(basemap, extent=extent, interpolation='bilinear')
    # restore original x/y limits
    ax.axis((xmin, xmax, ymin, ymax))

appName = "Python Example - PySpark Read CSV"
master = 'local'

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

hourly_taxi_trips = extended_trips \
    .groupBy("pick_date", "pick_hour").agg(
        f.count(extended_trips["fare_amount"]).alias("trip_count"),
        f.sum(extended_trips["passenger_count"]).alias("passenger_count"),
        f.sum(extended_trips["fare_amount"]).alias("fare_amount"),
        f.sum(extended_trips["tip_amount"]).alias("tip_amount"),
        f.sum(extended_trips["total_amount"]).alias("total_amount"),
        f.avg(extended_trips["duration"]).alias("avg_duration")
    )
# hourly_taxi_trips.write.mode("overwrite").parquet("./values/taxi-trips-hourly")

hourly_taxi_trips_drop = extended_trips \
    .groupBy("drop_date", "drop_hour").agg(
        f.count(extended_trips["fare_amount"]).alias("trip_count"),
        f.sum(extended_trips["passenger_count"]).alias("passenger_count"),
        f.sum(extended_trips["fare_amount"]).alias("fare_amount"),
        f.sum(extended_trips["tip_amount"]).alias("tip_amount"),
        f.sum(extended_trips["total_amount"]).alias("total_amount"),
        f.avg(extended_trips["duration"]).alias("avg_duration")
    )

daily_taxi_trips = hourly_taxi_trips.groupBy("pick_date").agg(
    f.sum(hourly_taxi_trips["trip_count"]).alias("trip_count"),
    f.sum(hourly_taxi_trips["passenger_count"]).alias("passenger_count"),
    f.sum(hourly_taxi_trips["fare_amount"]).alias("fare_amount"),
    f.sum(hourly_taxi_trips["tip_amount"]).alias("tip_amount"),
    f.sum(hourly_taxi_trips["total_amount"]).alias("total_amount"),
    f.avg(hourly_taxi_trips["avg_duration"]).alias("avg_duration")
)
daily_taxi_trips = daily_taxi_trips.sort("pick_date")
# daily_taxi_trips.write.mode("overwrite").parquet("./values/daily_taxi_trips")

daily_taxi_trips_drop = hourly_taxi_trips_drop.groupBy("drop_date").agg(
    f.sum(hourly_taxi_trips_drop["trip_count"]).alias("trip_count"),
    f.sum(hourly_taxi_trips_drop["passenger_count"]).alias("passenger_count"),
    f.sum(hourly_taxi_trips_drop["fare_amount"]).alias("fare_amount"),
    f.sum(hourly_taxi_trips_drop["tip_amount"]).alias("tip_amount"),
    f.sum(hourly_taxi_trips_drop["total_amount"]).alias("total_amount"),
    f.avg(hourly_taxi_trips_drop["avg_duration"]).alias("avg_duration")
)
daily_taxi_trips_drop = daily_taxi_trips_drop.sort("drop_date")

daily_taxi_trips_pandas = daily_taxi_trips.toPandas()
plt.figure(0)
plt.xlabel("date")
plt.ylabel("passengaer amount")
plt.plot(daily_taxi_trips_pandas["pick_date"],daily_taxi_trips_pandas["passenger_count"])
plt.savefig("./plot/distribution_six_month_pick.png")

plt.cla()
plt.figure(0)
plt.xlabel("date")
plt.ylabel("avg_duration")
plt.plot(daily_taxi_trips_pandas["pick_date"],daily_taxi_trips_pandas["avg_duration"])
plt.savefig("./plot/avg_duration_six_month_pick.png")

daily_taxi_trips_drop_pandas = daily_taxi_trips_drop.toPandas()
plt.cla()
plt.figure(0)
plt.xlabel("date")
plt.ylabel("passengaer amount")
plt.plot(daily_taxi_trips_drop_pandas["drop_date"],daily_taxi_trips_drop_pandas["passenger_count"])
plt.savefig("./plot/distribution_six_month_drop.png")

plt.cla()
plt.figure(0)
plt.xlabel("date")
plt.ylabel("avg_duration")
plt.plot(daily_taxi_trips_drop_pandas["drop_date"],daily_taxi_trips_drop_pandas["avg_duration"])
plt.savefig("./plot/avg_duration_six_month_drop.png")

one_day_hourly_taxi_trips = hourly_taxi_trips.groupBy("pick_hour").agg(
    f.sum(hourly_taxi_trips["trip_count"]).alias("trip_count"),
    f.sum(hourly_taxi_trips["passenger_count"]).alias("passenger_count"),
    f.sum(hourly_taxi_trips["fare_amount"]).alias("fare_amount"),
    f.sum(hourly_taxi_trips["tip_amount"]).alias("tip_amount"),
    f.sum(hourly_taxi_trips["total_amount"]).alias("total_amount"),
    f.avg(hourly_taxi_trips["avg_duration"]).alias("avg_duration")
)
one_day_hourly_taxi_trips = one_day_hourly_taxi_trips.sort("pick_hour")
# one_day_hourly_taxi_trips.write.mode("overwrite").parquet("./values/one_day_hourly_taxi_trips")
one_day_hourly_taxi_trips_pandas = one_day_hourly_taxi_trips.toPandas()

plt.cla()
plt.figure(1)
plt.xlabel("hour")
plt.ylabel("passengaer amount")
plt.bar(one_day_hourly_taxi_trips_pandas["pick_hour"],one_day_hourly_taxi_trips_pandas["passenger_count"])
plt.savefig("./plot/distribution_in_one_day_pick.png")

plt.cla()
plt.figure(1)
plt.xlabel("hour")
plt.ylabel("avg_duration")
plt.bar(one_day_hourly_taxi_trips_pandas["pick_hour"],one_day_hourly_taxi_trips_pandas["avg_duration"])
plt.savefig("./plot/avg_duration_in_one_day_pick.png")

one_day_hourly_taxi_trips_drop = hourly_taxi_trips_drop.groupBy("drop_hour").agg(
    f.sum(hourly_taxi_trips_drop["trip_count"]).alias("trip_count"),
    f.sum(hourly_taxi_trips_drop["passenger_count"]).alias("passenger_count"),
    f.sum(hourly_taxi_trips_drop["fare_amount"]).alias("fare_amount"),
    f.sum(hourly_taxi_trips_drop["tip_amount"]).alias("tip_amount"),
    f.sum(hourly_taxi_trips_drop["total_amount"]).alias("total_amount"),
    f.avg(hourly_taxi_trips_drop["avg_duration"]).alias("avg_duration")
)
one_day_hourly_taxi_trips_drop = one_day_hourly_taxi_trips_drop.sort("drop_hour")
one_day_hourly_taxi_trips_drop_pandas = one_day_hourly_taxi_trips_drop.toPandas()

plt.cla()
plt.figure(1)
plt.xlabel("hour")
plt.ylabel("passengaer amount")
plt.bar(one_day_hourly_taxi_trips_drop_pandas["drop_hour"],one_day_hourly_taxi_trips_drop_pandas["passenger_count"])
plt.savefig("./plot/distribution_in_one_day_drop.png")

plt.cla()
plt.figure(1)
plt.xlabel("hour")
plt.ylabel("avg_duration")
plt.bar(one_day_hourly_taxi_trips_drop_pandas["drop_hour"],one_day_hourly_taxi_trips_drop_pandas["avg_duration"])
plt.savefig("./plot/avg_duration_in_one_day_drop.png")
"""
taxi_green_pdf = trip_data.toPandas()
taxi_green_pdf.write.parquet("./values/taxi_green_pdf")
"""
taxi_trips_sample = trip_data \
    .sample(0.001) \
    .cache()
"""
quantile = taxi_trips_sample \
    .filter((taxi_trips_sample["pickup_longitude"] > -75) & (taxi_trips_sample["pickup_longitude"] < -65)) \
    .filter((taxi_trips_sample["pickup_latitude"] > 35) & (taxi_trips_sample["pickup_latitude"] < 45)) \
    .stat.approxQuantile(["pickup_longitude", "pickup_latitude"], [0.025,0.975], 0.01)
"""
trip_data_group_by_PUL = extended_trips \
    .groupBy("PULocationID").agg(
        f.count(extended_trips["fare_amount"]).alias("trip_count"),
        f.sum(extended_trips["passenger_count"]).alias("passenger_count"),
        f.sum(extended_trips["fare_amount"]).alias("fare_amount"),
        f.sum(extended_trips["tip_amount"]).alias("tip_amount"),
        f.sum(extended_trips["total_amount"]).alias("total_amount"),
        f.avg(extended_trips["duration"]).alias("avg_duration")
    )

trip_data_group_by_PUL = trip_data_group_by_PUL.selectExpr("PULocationID as LocationID", "trip_count as trip_count", "passenger_count as passenger_count", "fare_amount as fare_amount", "tip_amount as tip_amount", "total_amount as total_amount")

trip_data_group_by_DOL = extended_trips \
    .groupBy("DOLocationID").agg(
        f.count(extended_trips["fare_amount"]).alias("trip_count"),
        f.sum(extended_trips["passenger_count"]).alias("passenger_count"),
        f.sum(extended_trips["fare_amount"]).alias("fare_amount"),
        f.sum(extended_trips["tip_amount"]).alias("tip_amount"),
        f.sum(extended_trips["total_amount"]).alias("total_amount"),
        f.avg(extended_trips["duration"]).alias("avg_duration")
    )
trip_data_group_by_DOL = trip_data_group_by_DOL.selectExpr("DOLocationID as LocationID", "trip_count as trip_count", "passenger_count as passenger_count", "fare_amount as fare_amount", "tip_amount as tip_amount", "total_amount as total_amount")

world = gpd.read_file('./map/taxi_zones/taxi_zones.shp')
world_P = world.merge(trip_data_group_by_PUL.toPandas(), on = "LocationID", how = "outer")
world_D = world.merge(trip_data_group_by_DOL.toPandas(), on = "LocationID", how = "outer")

plt.figure(2)
fig, ax = plt.subplots(1, 1, figsize=(40,40))
world_P.plot(column= 'passenger_count', ax=ax, legend=True)
plt.savefig("./plot/map_passengaer_count_pick.png")

plt.figure(3)
fig, ax = plt.subplots(1, 1, figsize=(40,40))
world_D.plot(column= 'passenger_count', ax=ax, legend=True)
plt.savefig("./plot/map_passengaer_count_drop.png")


trip_data_group_by_PUL = trip_data_group_by_PUL.selectExpr("LocationID as LocationID", "trip_count as Ptrip_count", "passenger_count as Ppassenger_count", "fare_amount as Pfare_amount", "tip_amount as Ptip_amount", "total_amount as Ptotal_amount")
trip_data_group_by_DOL = trip_data_group_by_DOL.selectExpr("LocationID as LocationID", "trip_count as Dtrip_count", "passenger_count as Dpassenger_count", "fare_amount as Dfare_amount", "tip_amount as Dtip_amount", "total_amount as Dtotal_amount")

trip_data_group_by_LOC_PminusD = trip_data_group_by_PUL.join(trip_data_group_by_DOL, on = 'LocationID', how="outer") 
trip_data_group_by_LOC_PminusD = trip_data_group_by_LOC_PminusD\
    .withColumn('Mtrip_count', trip_data_group_by_LOC_PminusD['Ptrip_count']-trip_data_group_by_LOC_PminusD['Dtrip_count'])\
    .withColumn('Mpassenger_count', trip_data_group_by_LOC_PminusD['Ppassenger_count']-trip_data_group_by_LOC_PminusD['Dpassenger_count'])\
    .withColumn('Mfare_amount', trip_data_group_by_LOC_PminusD['Pfare_amount']-trip_data_group_by_LOC_PminusD['Dfare_amount'])\
    .withColumn('Mtip_amount', trip_data_group_by_LOC_PminusD['Ptip_amount']-trip_data_group_by_LOC_PminusD['Dtip_amount'])\
    .withColumn('Mtotal_amount', trip_data_group_by_LOC_PminusD['Ptotal_amount']-trip_data_group_by_LOC_PminusD['Dtotal_amount'])

plt.figure(4)
fig, ax = plt.subplots(1, 1,figsize=(40,40))
world_PMD = world.merge(trip_data_group_by_LOC_PminusD.toPandas(), on = "LocationID", how = "outer")
world_PMD.plot(column= 'Mpassenger_count', ax=ax, legend=True, cmap='hot')
# world_PMD.plot(column= 'Mpassenger_count', ax=ax, legend=True, cmap='OrRd', scheme='quantiles')
plt.savefig("./plot/map_passengaer_count_pick_minus_drop.png")


print(f'Record count is: {trip_data.count()}')