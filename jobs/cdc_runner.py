import time
import subprocess
import mysql.connector


MYSQL_HOST = "mysql"
MYSQL_PORT = 3306
MYSQL_DB = "etl_dw"
MYSQL_USER = "root"
MYSQL_PASSWORD = "1"
PIPELINE_NAME = "tracking_to_events_hourly"
SLEEP_SECONDS = 60


def get_last_loaded_ts():
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cur = conn.cursor()
    cur.execute(
        "SELECT last_loaded_ts FROM etl_watermark WHERE pipeline_name = %s",
        (PIPELINE_NAME,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None or row[0] is None:
        return "1970-01-01 00:00:00"

    return str(row[0])


def main():
    print("[CDC] Runner started...", flush=True)

    while True:
        try:
            since_ts = get_last_loaded_ts()
            print(f"[CDC] Chạy incremental từ mốc: {since_ts}", flush=True)

            cmd = [
                "spark-submit",
                "--master", "spark://spark-master:7077",
                "/opt/project/jobs/main.py",
                "--since-ts", since_ts
            ]

            subprocess.run(cmd, check=True)
            print("[CDC] Job chạy xong.", flush=True)

        except Exception as e:
            print(f"[CDC] Runner lỗi: {e}", flush=True)

        print(f"[CDC] Sleep {SLEEP_SECONDS} giây...", flush=True)
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()