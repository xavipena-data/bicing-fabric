import csv
from pathlib import Path

SRC_DIR = Path("data/raw/csv/info")
DST_DIR = Path("data/raw/csv/info_hourly")


def hourly_snapshot(src, dst):
    """Copy src to dst keeping only the first snapshot of each hour."""
    stats = {"rows_read": 0, "rows_kept": 0,
             "snapshots_read": 0, "snapshots_kept": 0, "bad_rows": 0}
    last_ts = None      # last_updated of the previous row
    last_hour = None    # hour of the snapshot we are keeping
    kept_ts = None      # last_updated of the snapshot we keep for that hour

    with open(src, newline="", encoding="utf-8-sig") as fin, \
         open(dst, "w", newline="", encoding="utf-8") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            stats["rows_read"] += 1
            ts = row["last_updated"]
            if not ts.isdigit():
                stats["bad_rows"] += 1
                continue
            if ts != last_ts:                  # a new snapshot starts
                last_ts = ts
                stats["snapshots_read"] += 1
                hour = int(ts) // 3600         # epoch seconds -> hours
                if hour != last_hour:          # first snapshot of a new hour
                    last_hour = hour
                    kept_ts = ts
                    stats["snapshots_kept"] += 1
            if ts == kept_ts:                  # every station of the kept snapshot
                writer.writerow(row)
                stats["rows_kept"] += 1
    return stats

MONTHS = ["2024-11", "2024-12", "2025-01", "2025-02", "2025-03", "2025-04",
          "2025-05", "2025-06", "2025-07", "2025-08", "2025-09", "2025-10"]

if __name__ == "__main__":
    DST_DIR.mkdir(parents=True, exist_ok=True)
    for month in MONTHS:
        name = f"info_{month}.csv"
        stats = hourly_snapshot(SRC_DIR / name, DST_DIR / name)
        print(name, stats)