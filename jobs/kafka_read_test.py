from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("kafka-read-test")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:29092")
    .option("subscribe", "tracking-events")
    .load()
)

result = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
result.show(truncate=False)

spark.stop()