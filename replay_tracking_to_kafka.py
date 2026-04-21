import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8")
)

with open("data/cassandra/tracking.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        message = {
            "id": row.get("create_time"),
            "custom_track": row.get("custom_track") or None,
            "job_id": int(row["job_id"]) if row.get("job_id") else None,
            "publisher_id": int(row["publisher_id"]) if row.get("publisher_id") else None,
            "campaign_id": int(row["campaign_id"]) if row.get("campaign_id") else None,
            "group_id": int(row["group_id"]) if row.get("group_id") else None,
            "bid": float(row["bid"]) if row.get("bid") else 0.0,
            "ts": row.get("ts")
        }

        producer.send("tracking-events", key=message["id"], value=message)
        print("Sent:", message)
        time.sleep(1)

producer.flush()
producer.close()