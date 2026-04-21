from pyspark.sql import SparkSession, functions as F, types as T

MYSQL_URL = "jdbc:mysql://mysql:3306/etl_dw"
MYSQL_USER = "root"
MYSQL_PASSWORD = "1"

spark = (
    SparkSession.builder
    .appName("kafka-to-mysql-stream")
    .config("spark.sql.session.timeZone", "UTC")
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

parsed_df = (
    stream_df
    .selectExpr("CAST(value AS STRING) AS value")
    .select(F.from_json(F.col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("ts", F.to_timestamp("ts"))
    .filter(F.col("job_id").isNotNull())
)

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    result_df = (
        batch_df
        .withColumn("dates", F.to_date("ts"))
        .withColumn("hours", F.hour("ts"))
        .withColumn("clicks", F.when(F.col("custom_track") == "click", 1).otherwise(0))
        .withColumn("impressions", F.when(F.col("custom_track") == "alive", 1).otherwise(0))
        .withColumn(
            "qualified_application",
            F.when(F.col("custom_track") == "qualified", 1).otherwise(0)
        )
        .withColumn(
            "disqualified_application",
            F.when(F.col("custom_track") == "unqualified", 1).otherwise(0)
        )
        .withColumn(
            "conversion",
            F.when(F.col("custom_track") == "conversion", 1).otherwise(0)
        )
        .withColumn(
            "spend_component",
            F.when(F.col("custom_track") == "click", F.coalesce(F.col("bid"), F.lit(0.0))).otherwise(F.lit(0.0))
        )
        .groupBy("job_id", "dates", "hours", "publisher_id", "campaign_id", "group_id")
        .agg(
            F.sum("clicks").alias("clicks"),
            F.sum("impressions").alias("impressions"),
            F.sum("qualified_application").alias("qualified_application"),
            F.sum("disqualified_application").alias("disqualified_application"),
            F.sum("conversion").alias("conversion"),
            F.max(F.coalesce(F.col("bid"), F.lit(0.0))).alias("bid_set"),
            F.sum("spend_component").alias("spend_hour"),
            F.max("ts").alias("latest_update")
        )
        .withColumn("sources", F.lit("Kafka"))
        .select(
            "job_id",
            "dates",
            "hours",
            "publisher_id",
            "campaign_id",
            "group_id",
            "clicks",
            "impressions",
            "qualified_application",
            "disqualified_application",
            "conversion",
            "bid_set",
            "spend_hour",
            "sources",
            "latest_update"
        )
    )

    (
        result_df.write
        .format("jdbc")
        .option("url", MYSQL_URL)
        .option("dbtable", "fact_events_hourly_kafka")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("append")
        .save()
    )

query = (
    parsed_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/kafka_to_mysql_stream")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()