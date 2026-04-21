from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("kafka-stream-console")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:29092")
    .option("subscribe", "tracking-events")
    .option("startingOffsets", "earliest")
    .load()
)

result = stream_df.selectExpr(
    "CAST(key AS STRING) AS kafka_key",
    "CAST(value AS STRING) AS kafka_value",
    "timestamp AS kafka_timestamp"
)

query = (
    result.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()