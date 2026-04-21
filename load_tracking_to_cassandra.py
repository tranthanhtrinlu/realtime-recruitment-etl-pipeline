import csv
import re
from pathlib import Path
from datetime import datetime
from cassandra.cluster import Cluster

INPUT_FILE = Path(r"data/cassandra/tracking.csv")
BAD_FILE = Path(r"data/cassandra/tracking_bad_rows_after_load.csv")

EXPECTED_HEADER = [
    "create_time", "bid", "bn", "campaign_id", "cd", "custom_track", "de", "dl",
    "dt", "ed", "ev", "group_id", "id", "job_id", "md", "publisher_id", "rl",
    "sr", "ts", "tz", "ua", "uid", "utm_campaign", "utm_content", "utm_medium",
    "utm_source", "utm_term", "v", "vp"
]

EXPECTED_COLS = len(EXPECTED_HEADER)

INT_COLUMNS = {
    "bid", "campaign_id", "cd", "ev", "group_id", "job_id", "publisher_id", "tz", "v"
}

TIMEUUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?$"
)


def norm_text(v):
    if v is None:
        return ""
    return str(v).strip()


def norm_nullable_text(v):
    s = norm_text(v)
    return s if s != "" else None


def norm_int(v):
    s = norm_text(v)
    if s == "":
        return None

    try:
        return int(float(s))
    except Exception:
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        if m:
            try:
                return int(float(m.group(0)))
            except Exception:
                return None
        return None


def norm_timestamp(v):
    s = norm_text(v)
    if s == "":
        return None

    # thử parse dạng có milliseconds trước
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    return None


def looks_like_uuid(v):
    return bool(TIMEUUID_RE.match(norm_text(v)))


def looks_like_ts(v):
    return bool(TS_RE.match(norm_text(v)))


def find_ts_index(row):
    for i in range(15, len(row)):
        if looks_like_ts(row[i]):
            return i
    return -1


def sanitize_row(row):
    row = [norm_text(x) for x in row]

    if len(row) < 20:
        return None, "too_few_cols"

    if not looks_like_uuid(row[0]):
        return None, "bad_create_time"

    # Trường hợp chuẩn
    if len(row) == EXPECTED_COLS:
        fixed = row[:]
    else:
        # cứu dòng bị vỡ ở cột ed
        prefix = row[:9]  # create_time..dt
        if len(prefix) != 9:
            return None, "bad_prefix"

        ts_idx = find_ts_index(row)
        if ts_idx == -1:
            return None, "ts_not_found"

        suffix = row[ts_idx:]
        if len(suffix) < 11:
            return None, "suffix_short"

        suffix = suffix[-11:]  # ts..vp

        middle = row[9:len(row) - 11]
        if len(middle) < 9:
            return None, "middle_short"

        ed_parts = middle[: len(middle) - 8]
        tail8 = middle[len(middle) - 8 :]

        if len(tail8) != 8:
            return None, "tail8_bad"

        ed = ",".join(ed_parts).strip()
        fixed = prefix + [ed] + tail8 + suffix

        if len(fixed) != EXPECTED_COLS:
            return None, f"fixed_len_{len(fixed)}"

    data = {}
    for col, val in zip(EXPECTED_HEADER, fixed):
        if col in INT_COLUMNS:
            data[col] = norm_int(val)
        elif col == "ts":
            data[col] = norm_timestamp(val)
        else:
            data[col] = norm_nullable_text(val)

    if not data["create_time"]:
        return None, "empty_create_time"

    return data, "ok"


def main():
    cluster = Cluster(["127.0.0.1"], port=19042)
    session = cluster.connect("logs")

    insert_cql = """
    INSERT INTO tracking_raw (
        create_time, bid, bn, campaign_id, cd, custom_track, de, dl, dt, ed, ev,
        group_id, id, job_id, md, publisher_id, rl, sr, ts, tz, ua, uid,
        utm_campaign, utm_content, utm_medium, utm_source, utm_term, v, vp
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """
    prepared = session.prepare(insert_cql)

    total = 0
    ok = 0
    bad = 0
    reasons = {}

    with INPUT_FILE.open("r", encoding="utf-8-sig", newline="") as fin, \
         BAD_FILE.open("w", encoding="utf-8", newline="") as fbad:

        reader = csv.reader(fin)
        bad_writer = csv.writer(fbad)
        bad_writer.writerow(["row_number", "reason", "raw_col_count", "raw_row"])

        header = next(reader, None)
        if header is None:
            raise ValueError("tracking.csv is empty")

        for row_number, row in enumerate(reader, start=2):
            total += 1
            cleaned, reason = sanitize_row(row)

            if cleaned is None:
                bad += 1
                reasons[reason] = reasons.get(reason, 0) + 1
                bad_writer.writerow([row_number, reason, len(row), row])
                continue

            try:
                session.execute(prepared, [
                    cleaned["create_time"],
                    cleaned["bid"],
                    cleaned["bn"],
                    cleaned["campaign_id"],
                    cleaned["cd"],
                    cleaned["custom_track"],
                    cleaned["de"],
                    cleaned["dl"],
                    cleaned["dt"],
                    cleaned["ed"],
                    cleaned["ev"],
                    cleaned["group_id"],
                    cleaned["id"],
                    cleaned["job_id"],
                    cleaned["md"],
                    cleaned["publisher_id"],
                    cleaned["rl"],
                    cleaned["sr"],
                    cleaned["ts"],
                    cleaned["tz"],
                    cleaned["ua"],
                    cleaned["uid"],
                    cleaned["utm_campaign"],
                    cleaned["utm_content"],
                    cleaned["utm_medium"],
                    cleaned["utm_source"],
                    cleaned["utm_term"],
                    cleaned["v"],
                    cleaned["vp"],
                ])
                ok += 1
            except Exception as e:
                bad += 1
                key = f"{type(e).__name__}: {str(e)}"
                reasons[key] = reasons.get(key, 0) + 1
                bad_writer.writerow([row_number, key, len(row), row])

    print(f"total_rows={total}")
    print(f"inserted_rows={ok}")
    print(f"bad_rows={bad}")
    print("bad_reasons=")
    for k, v in sorted(reasons.items()):
        print(f"- {k}: {v}")

    cluster.shutdown()


if __name__ == "__main__":
    main()