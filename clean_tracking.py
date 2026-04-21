import pandas as pd
from pathlib import Path

INPUT_FILE = Path(r"data/cassandra/tracking.csv")
OUTPUT_FILE = Path(r"data/cassandra/tracking_clean.csv")
BAD_FILE = Path(r"data/cassandra/tracking_bad_rows.csv")
REPORT_FILE = Path(r"data/cassandra/tracking_clean_report.txt")

COLUMNS = [
    "create_time", "bid", "bn", "campaign_id", "cd", "custom_track", "de", "dl",
    "dt", "ed", "ev", "group_id", "id", "job_id", "md", "publisher_id", "rl",
    "sr", "ts", "tz", "ua", "uid", "utm_campaign", "utm_content", "utm_medium",
    "utm_source", "utm_term", "v", "vp"
]

INT_COLS = ["bid", "campaign_id", "cd", "ev", "group_id", "job_id", "publisher_id", "tz", "v"]

def clean_text(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()

    # bỏ escape kiểu backslash gây rối cho Cassandra
    s = s.replace('\\"', '"')

    # Cassandra COPY chịu được quote CSV chuẩn hơn backslash escape
    return s

def clean_int(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s == "":
        return ""
    try:
        return str(int(float(s)))
    except Exception:
        return ""

def main():
    bad_rows = []

    # on_bad_lines='skip' để bỏ các dòng quá bẩn mà pandas không parse nổi
    df = pd.read_csv(
        INPUT_FILE,
        dtype=str,
        keep_default_na=False,
        on_bad_lines="skip",
        engine="python"
    )

    source_cols = list(df.columns)

    # chỉ lấy đúng các cột cần
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[COLUMNS].copy()

    # làm sạch text
    for col in COLUMNS:
        if col not in INT_COLS:
            df[col] = df[col].map(clean_text)

    # làm sạch int
    for col in INT_COLS:
        df[col] = df[col].map(clean_int)

    # loại dòng không có create_time
    before = len(df)
    df = df[df["create_time"].astype(str).str.strip() != ""].copy()
    after_nonempty_ct = len(df)

    # bỏ duplicate theo create_time
    df = df.drop_duplicates(subset=["create_time"], keep="first").copy()
    after_dedup = len(df)

    # ghi bad row report rất đơn giản
    report = []
    report.append(f"INPUT_FILE={INPUT_FILE}")
    report.append(f"OUTPUT_FILE={OUTPUT_FILE}")
    report.append(f"BAD_FILE={BAD_FILE}")
    report.append("")
    report.append(f"source_columns={source_cols}")
    report.append(f"expected_columns={COLUMNS}")
    report.append("")
    report.append(f"rows_after_read={before}")
    report.append(f"rows_after_drop_empty_create_time={after_nonempty_ct}")
    report.append(f"rows_after_dedup={after_dedup}")
    report.append(f"rows_dropped_empty_create_time={before - after_nonempty_ct}")
    report.append(f"rows_dropped_duplicates={after_nonempty_ct - after_dedup}")

    REPORT_FILE.write_text("\n".join(report), encoding="utf-8")

    # xuất bad file placeholder
    pd.DataFrame(bad_rows).to_csv(BAD_FILE, index=False, encoding="utf-8")

    # QUAN TRỌNG: pandas sẽ ghi CSV chuẩn hơn cho Cassandra
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print("\n".join(report))

if __name__ == "__main__":
    main()