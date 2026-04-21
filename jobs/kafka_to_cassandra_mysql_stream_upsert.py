import mysql.connector
from pyspark.sql import SparkSession, functions as F, types as T

MYSQL_HOST = "mysql"
MYSQL_PORT = 3306
MYSQL_DB = "etl_dw"
MYSQL_USER = "root"
MYSQL_PASSWORD = "1"

spark = (
    SparkSession.builder
    .appName("kafka-to-cassandra-mysql-stream-upsert")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
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

def mysql_conn():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )

def truncate_stage():
    conn = mysql_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE fact_events_hourly_kafka_stage")
    conn.commit()
    cur.close()
    conn.close()

def merge_stage():
    conn = mysql_conn()
    cur = conn.cursor()

    merge_sql = """
    INSERT INTO fact_events_hourly_kafka_final (
        job_id, dates, hours, publisher_id, campaign_id, group_id,
        clicks, impressions, qualified_application, disqualified_application,
        conversion, bid_set, spend_hour, sources, latest_update
    )
    SELECT
        job_id, dates, hours, publisher_id, campaign_id, group_id,
        clicks, impressions, qualified_application, disqualified_application,
        conversion, bid_set, spend_hour, sources, latest_update
    FROM fact_events_hourly_kafka_stage AS incoming
    ON DUPLICATE KEY UPDATE
        clicks = fact_events_hourly_kafka_final.clicks + incoming.clicks,
        impressions = fact_events_hourly_kafka_final.impressions + incoming.impressions,
        qualified_application = fact_events_hourly_kafka_final.qualified_application + incoming.qualified_application,
        disqualified_application = fact_events_hourly_kafka_final.disqualified_application + incoming.disqualified_application,
        conversion = fact_events_hourly_kafka_final.conversion + incoming.conversion,
        bid_set = GREATEST(fact_events_hourly_kafka_final.bid_set, incoming.bid_set),
        spend_hour = fact_events_hourly_kafka_final.spend_hour + incoming.spend_hour,
        latest_update = GREATEST(fact_events_hourly_kafka_final.latest_update, incoming.latest_update),
        sources = incoming.sources
    """

    cur.execute(merge_sql)
    conn.commit()
    cur.close()
    conn.close()

stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:29092")
    .option("subscribe", "tracking-events")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    stream_df
    .selectExpr("CAST(value AS STRING) AS value")
    .select(F.from_json(F.col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("ts", F.to_timestamp("ts"))
    .filter(F.col("id").isNotNull())
)

def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    prepared_df = (
        batch_df
        .dropDuplicates(["id"])
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn("sources", F.lit("Kafka"))
        .cache()
    )

    # 1) ghi raw vào Cassandra
    raw_df = prepared_df.select(
        "id",
        "custom_track",
        "job_id",
        "publisher_id",
        "campaign_id",
        "group_id",
        "bid",
        "ts",
        "ingest_time",
        "sources"
    )

    (
        raw_df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table="tracking_raw_kafka", keyspace="logs")
        .mode("append")
        .save()
    )

    # 2) aggregate
    agg_df = (
        prepared_df
        .filter(F.col("job_id").isNotNull())
        .withColumn("publisher_id", F.coalesce(F.col("publisher_id"), F.lit(-1)))
        .withColumn("campaign_id", F.coalesce(F.col("campaign_id"), F.lit(-1)))
        .withColumn("group_id", F.coalesce(F.col("group_id"), F.lit(-1)))
        .withColumn("dates", F.to_date("ts"))
        .withColumn("hours", F.hour("ts"))
        .withColumn("clicks", F.when(F.col("custom_track") == "click", 1).otherwise(0))
        .withColumn("impressions", F.when(F.col("custom_track") == "alive", 1).otherwise(0))
        .withColumn("qualified_application", F.when(F.col("custom_track") == "qualified", 1).otherwise(0))
        .withColumn("disqualified_application", F.when(F.col("custom_track") == "unqualified", 1).otherwise(0))
        .withColumn("conversion", F.when(F.col("custom_track") == "conversion", 1).otherwise(0))
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
            "job_id", "dates", "hours", "publisher_id", "campaign_id", "group_id",
            "clicks", "impressions", "qualified_application", "disqualified_application",
            "conversion", "bid_set", "spend_hour", "sources", "latest_update"
        )
    )

    truncate_stage()

    (
        agg_df.write
        .format("jdbc")
        .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
        .option("dbtable", "fact_events_hourly_kafka_stage")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("append")
        .save()
    )

    merge_stage()
    truncate_stage()
    prepared_df.unpersist()

query = (
    parsed_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/kafka_to_cassandra_mysql_stream_upsert")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()