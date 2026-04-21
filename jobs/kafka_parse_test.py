from pyspark.sql import SparkSession, functions as F, types as T

spark = (
    SparkSession.builder
    .appName("kafka-parse-test")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = T.StructType([
    T.StructField("id", T.StringType(), True),
    T.StructField("custom_track", T.StringType(), True),
    T.StructField("job_id", T.IntegerType(), True),
    T.StructField("publisher_id", T.IntegerType(), True),
    T.StructField("campaign_id", T.IntegerType(), True),
    T.StructField("group_id", T.IntegerType(), True),
    T.StructField("bid", T.DoubleType(), True),
    T.StructField("ts", T.StringType(), True),
])

stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:29092")
    .option("subscribe", "tracking-events")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    stream_df
    .selectExpr("CAST(value AS STRING) AS value")
    .select(F.from_json("value", schema).alias("data"))
    .select("data.*")
    .withColumn("ts", F.to_timestamp("ts"))
)

query = (
    parsed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()