import argparse
import mysql.connector
from pyspark.sql import SparkSession, functions as F


MYSQL_HOST = "mysql"
MYSQL_PORT = 3306
MYSQL_DB = "etl_dw"
MYSQL_USER = "root"
MYSQL_PASSWORD = "1"
PIPELINE_NAME = "tracking_to_events_hourly"


def get_spark():
    spark = (
        SparkSession.builder
        .appName("tracking_to_events_hourly")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


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
    cur.execute("TRUNCATE TABLE fact_events_hourly_stage")
    conn.commit()
    cur.close()
    conn.close()


def merge_stage(max_ts):
    conn = mysql_conn()
    cur = conn.cursor()

    merge_sql = """
    INSERT INTO fact_events_hourly (
        job_id, dates, hours,
        disqualified_application,
        qualified_application,
        conversion,
        group_id,
        campaign_id,
        publisher_id,
        company_name,
        bid_set,
        clicks,
        impressions,
        spend_hour,
        sources,
        latest_update
    )
    SELECT
        job_id, dates, hours,
        disqualified_application,
        qualified_application,
        conversion,
        group_id,
        campaign_id,
        publisher_id,
        company_name,
        bid_set,
        clicks,
        impressions,
        spend_hour,
        sources,
        latest_update
    FROM fact_events_hourly_stage
    ON DUPLICATE KEY UPDATE
        disqualified_application = COALESCE(fact_events_hourly.disqualified_application, 0) + VALUES(disqualified_application),
        qualified_application = COALESCE(fact_events_hourly.qualified_application, 0) + VALUES(qualified_application),
        conversion = COALESCE(fact_events_hourly.conversion, 0) + VALUES(conversion),
        clicks = COALESCE(fact_events_hourly.clicks, 0) + VALUES(clicks),
        impressions = COALESCE(fact_events_hourly.impressions, 0) + VALUES(impressions),
        spend_hour = COALESCE(fact_events_hourly.spend_hour, 0) + VALUES(spend_hour),
        bid_set = GREATEST(COALESCE(fact_events_hourly.bid_set, 0), VALUES(bid_set)),
        latest_update = GREATEST(fact_events_hourly.latest_update, VALUES(latest_update)),
        group_id = COALESCE(VALUES(group_id), fact_events_hourly.group_id),
        company_name = COALESCE(VALUES(company_name), fact_events_hourly.company_name),
        sources = VALUES(sources)
    """

    cur.execute(merge_sql)

    watermark_sql = """
    INSERT INTO etl_watermark (pipeline_name, last_loaded_ts)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE last_loaded_ts = VALUES(last_loaded_ts)
    """
    cur.execute(watermark_sql, (PIPELINE_NAME, max_ts))

    cur.execute("TRUNCATE TABLE fact_events_hourly_stage")
    conn.commit()
    cur.close()
    conn.close()


def empty_to_null_int(col):
    return F.when(F.trim(col) == "", None).otherwise(col.cast("int"))


def empty_to_null_double(col):
    return F.when(F.trim(col) == "", None).otherwise(col.cast("double"))


def main(since_ts: str):
    spark = get_spark()

    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

    tracking_df = (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(table="tracking_raw", keyspace="logs")
        .load()
    )

    # Lấy JSON fields từ ed
    tracking_df = (
        tracking_df
        .withColumn("ed_custom_event", F.get_json_object("ed", "$.customEvent"))
        .withColumn("ed_job_id", F.get_json_object("ed", "$.jobId"))
        .withColumn("ed_publisher_id", F.get_json_object("ed", "$.publisherId"))
    )

    # Lấy job_id / publisher_id từ URL nếu JSON/top-level không có
    tracking_df = (
        tracking_df
        .withColumn("url_job_id_1", F.regexp_extract("dl", r"jobId=(\\d+)", 1))
        .withColumn("url_job_id_2", F.regexp_extract("dl", r"param1=(\\d+)", 1))
        .withColumn("url_publisher_id_1", F.regexp_extract("dl", r"publisherId=(\\d+)", 1))
        .withColumn("url_publisher_id_2", F.regexp_extract("dl", r"param2=(\\d+)", 1))
    )

    tracking_df = (
        tracking_df
        .withColumn(
            "event_name",
            F.coalesce(
                F.when(F.trim(F.col("custom_track")) == "", None).otherwise(F.col("custom_track")),
                F.when(F.trim(F.col("ed_custom_event")) == "", None).otherwise(F.col("ed_custom_event"))
            )
        )
        .withColumn(
            "job_id_norm",
            F.coalesce(
                F.col("job_id").cast("int"),
                empty_to_null_int(F.col("ed_job_id")),
                empty_to_null_int(F.col("url_job_id_1")),
                empty_to_null_int(F.col("url_job_id_2"))
            )
        )
        .withColumn(
            "publisher_id_norm",
            F.coalesce(
                F.col("publisher_id").cast("int"),
                empty_to_null_int(F.col("ed_publisher_id")),
                empty_to_null_int(F.col("url_publisher_id_1")),
                empty_to_null_int(F.col("url_publisher_id_2"))
            )
        )
        .withColumn("campaign_id_norm", F.col("campaign_id").cast("int"))
        .withColumn("group_id_norm", F.col("group_id").cast("int"))
        .withColumn("bid_norm", F.col("bid").cast("double"))
        .withColumn("ts_norm", F.col("ts").cast("timestamp"))
    )

    delta_df = (
        tracking_df
        .filter(F.col("ts_norm").isNotNull())
        .filter(F.col("ts_norm") > F.to_timestamp(F.lit(since_ts)))
    )

    if delta_df.rdd.isEmpty():
        print(f"[INFO] Không có dữ liệu mới sau {since_ts}")
        spark.stop()
        return

    # Đọc search lookup từ MySQL
    search_df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dim_search_job")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
        .select(
            F.col("job_id").cast("int").alias("lookup_job_id"),
            F.col("campaign_id").cast("int").alias("lookup_campaign_id"),
            F.col("bid").cast("double").alias("lookup_bid"),
            F.col("company_name")
        )
        .dropDuplicates(["lookup_job_id"])
    )

    enriched_df = (
        delta_df
        .join(search_df, delta_df.job_id_norm == search_df.lookup_job_id, "left")
        .withColumn("campaign_id_final", F.coalesce(F.col("campaign_id_norm"), F.col("lookup_campaign_id")))
        .withColumn("bid_final", F.coalesce(F.col("bid_norm"), F.col("lookup_bid"), F.lit(0.0)))
        .withColumn("dates", F.to_date("ts_norm"))
        .withColumn("hours", F.hour("ts_norm"))
        .withColumn("clicks", F.when(F.col("event_name") == "click", 1).otherwise(0))
        .withColumn("impressions", F.when(F.col("event_name") == "alive", 1).otherwise(0))
        .withColumn("qualified_application",
                    F.when(F.col("event_name").isin("qualified", "qualified_application"), 1).otherwise(0))
        .withColumn("disqualified_application",
                    F.when(F.col("event_name").isin("unqualified", "disqualified", "disqualified_application"), 1).otherwise(0))
        .withColumn("conversion",
                    F.when(F.col("event_name").isin("conversion", "apply"), 1).otherwise(0))
        .withColumn("spend_component",
                    F.when(F.col("event_name") == "click", F.col("bid_final")).otherwise(F.lit(0.0)))
    )

    # Bỏ các dòng vẫn không suy ra được job_id
    enriched_df = enriched_df.filter(F.col("job_id_norm").isNotNull())

    result_df = (
        enriched_df
        .groupBy(
            F.col("job_id_norm").alias("job_id"),
            "dates",
            "hours",
            F.col("group_id_norm").alias("group_id"),
            F.col("campaign_id_final").alias("campaign_id"),
            F.col("publisher_id_norm").alias("publisher_id"),
            "company_name"
        )
        .agg(
            F.sum("disqualified_application").alias("disqualified_application"),
            F.sum("qualified_application").alias("qualified_application"),
            F.sum("conversion").alias("conversion"),
            F.max("bid_final").alias("bid_set"),
            F.sum("clicks").alias("clicks"),
            F.sum("impressions").alias("impressions"),
            F.sum("spend_component").alias("spend_hour"),
            F.max("ts_norm").alias("latest_update")
        )
        .withColumn("sources", F.lit("Cassandra"))
    )

    truncate_stage()

    (
        result_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "fact_events_hourly_stage")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("append")
        .save()
    )

    max_ts = result_df.agg(F.max("latest_update").alias("max_ts")).collect()[0]["max_ts"]
    merge_stage(max_ts)

    print(f"[INFO] ETL xong. Watermark mới = {max_ts}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--since-ts", required=True)
    args = parser.parse_args()
    main(args.since_ts)