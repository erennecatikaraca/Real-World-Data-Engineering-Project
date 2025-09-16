from pyspark.sql.functions import col, to_timestamp, lit, when
from delta.tables import DeltaTable

lakehouse_name = "myLake"
silver_table_name = "cleaned_silver_trips"
bronze_data_table_name = "raw_data"
bronze_fare_table_name = "raw_fare"

df_data = spark.read.format("delta").table(f"{lakehouse_name}.{bronze_data_table_name}")
df_fare = spark.read.format("delta").table(f"{lakehouse_name}.{bronze_fare_table_name}")

df_data = df_data.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
df_fare = df_fare.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))

df_data = df_data.dropDuplicates(["medallion", "pickup_datetime"])
df_fare = df_fare.dropDuplicates(["medallion", "pickup_datetime"])

df_data_cleaned = df_data.select(
    col("medallion").alias("trip_id"),
    col("vendor_id"),
    col("pickup_datetime"),
    to_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss").alias("dropoff_datetime"),
    col("passenger_count"),
    col("trip_distance"),
    col("rate_code"),
    when((col("store_and_fwd_flag").isNull()) | (col("store_and_fwd_flag") == ""), lit("N"))
        .otherwise(col("store_and_fwd_flag")).alias("store_and_fwd_flag"),
    col("pickup_longitude"),
    col("pickup_latitude"),
    col("dropoff_longitude"),
    col("dropoff_latitude")
).filter(
    (col("passenger_count") >= 0) &
    (col("trip_distance") >= 0) &
    (col("pickup_longitude") != 0) &
    (col("pickup_latitude") != 0) &
    (col("dropoff_longitude") != 0) &
    (col("dropoff_latitude") != 0)
)

df_fare_cleaned = df_fare.select(
    col("medallion").alias("trip_id"),
    col("pickup_datetime"),
    col("payment_type"),
    col("fare_amount"),
    col("surcharge").alias("extra"),
    col("mta_tax"),
    col("tip_amount"),
    col("tolls_amount"),
    col("total_amount")
).na.fill(0, ["extra", "tip_amount", "tolls_amount"])

df_joined = df_data_cleaned.join(
    df_fare_cleaned,
    ["trip_id", "pickup_datetime"],
    "left"
)

df_joined = df_joined.fillna(
    0,
    subset=["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "total_amount"]
)

df_final = df_joined.withColumn(
    "trip_duration",
    (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long"))
).withColumn("trip_distance", col("trip_distance").cast("double")) \
.withColumn("trip_duration", col("trip_duration").cast("double")) \
.withColumn("average_speed", when(col("trip_duration") < 60, lit(None))
    .otherwise(col("trip_distance") / (col("trip_duration") / 3600))
).filter(
    (col("trip_duration") >= 60) &
    (col("trip_duration") < (10 * 3600)) &
    (col("total_amount") >= 0)
).withColumn(
    "pickup_date",
    col("pickup_datetime").cast("date")
).filter(
    (col("average_speed") > 0) & (col("average_speed") < 150)
)

if spark.catalog.tableExists(f"{lakehouse_name}.{silver_table_name}"):
    silver_delta_table = DeltaTable.forName(spark, f"{lakehouse_name}.{silver_table_name}")
    silver_delta_table.alias("target").merge(
        df_final.alias("source"),
        "target.trip_id = source.trip_id AND target.pickup_datetime = source.pickup_datetime"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    df_final.write.format("delta") \
        .saveAsTable(f"{lakehouse_name}.{silver_table_name}")
