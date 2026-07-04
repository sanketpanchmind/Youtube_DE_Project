from pyspark import pipelines as dp
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, explode, from_unixtime, current_timestamp

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

volume_path = '/Volumes/youtube_dev/bronze/earthquake_data_bronze';

feature_schema = ArrayType(
    StructType([
        StructField("type", StringType(), True),

        StructField("properties", StructType([
            StructField("mag", DoubleType(), True),
            StructField("place", StringType(), True),
            StructField("time", LongType(), True),
            StructField("updated", LongType(), True),
            StructField("tz", IntegerType(), True),
            StructField("url", StringType(), True),
            StructField("detail", StringType(), True),
            StructField("felt", IntegerType(), True),
            StructField("cdi", DoubleType(), True),
            StructField("mmi", DoubleType(), True),
            StructField("alert", StringType(), True),
            StructField("status", StringType(), True),
            StructField("tsunami", IntegerType(), True),
            StructField("sig", IntegerType(), True),
            StructField("net", StringType(), True),
            StructField("code", StringType(), True),
            StructField("ids", StringType(), True),
            StructField("sources", StringType(), True),
            StructField("types", StringType(), True),
            StructField("nst", IntegerType(), True),
            StructField("dmin", DoubleType(), True),
            StructField("rms", DoubleType(), True),
            StructField("gap", DoubleType(), True),
            StructField("magType", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True)
        ]), True),

        StructField("geometry", StructType([
            StructField("type", StringType(), True),
            StructField("coordinates", ArrayType(DoubleType()), True)
        ]), True),

        StructField("id", StringType(), True)
    ])
)

schema = feature_schema;

@dp.temporary_view()
def earthquake_data_view():
    df =  spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "json")\
        .load(volume_path)\
        .withColumn("_load_timestamp", current_timestamp())

    df = df.withColumn("parsed_data",from_json(col("features"),schema))
    df = df.select(explode(col("parsed_data")).alias("features"), "_load_timestamp")
    df = df.select(
    "features.properties.*",
    "features.id",
    col("features.geometry.coordinates")[0].alias("longitude"),
    col("features.geometry.coordinates")[1].alias("latitude"),
    col("features.geometry.coordinates")[2].alias("depth"),
    "_load_timestamp"
    )  

    # the time is proprly renderd so using unizxtime

    df = df.withColumn("time", from_unixtime(col("time")/1000).cast("timestamp"))\
    .withColumn("updated", from_unixtime(col("updated")/1000).cast("timestamp"))\
    .withColumn("felt", col("felt").cast("double"))\
    .withColumn("nst", col("nst").cast("integer"))\
    .withColumn("sig", col("sig").cast("integer"))\
    .withColumn("tsunami", col("tsunami").cast("double"))\
    .withColumn("mag", col("mag").cast("double"))\
    .withColumn("cdi", col("cdi").cast("double"))\
    .withColumn("mmi", col("mmi").cast("double"))
                    
    return df



# Create target streaming table
dp.create_streaming_table(name="earthquake_data_final")

# Define Auto CDC flow
dp.apply_changes(
    target="earthquake_data_final",
    source="earthquake_data_view",
    keys=["id"],
    sequence_by="_load_timestamp",
    stored_as_scd_type=1
)
