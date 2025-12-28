import os
import json
import requests
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT_ID = "xkcd-case-study"  # change if needed
XKCD_CURRENT_URL = "https://xkcd.com/info.0.json"

def main():
    raw_dataset = os.getenv("BQ_RAW_DATASET", "raw_xkcd")
    raw_table = os.getenv("BQ_RAW_TABLE", "comics_json")
    table_id = f"{PROJECT_ID}.{raw_dataset}.{raw_table}"

    client = bigquery.Client(project=PROJECT_ID)

    # Clear existing data (keep table + schema)
    client.query(f"TRUNCATE TABLE `{table_id}`").result()

    # Find latest comic number
    r = requests.get(XKCD_CURRENT_URL, timeout=30)
    r.raise_for_status()
    latest_num = int(r.json()["num"])

    for n in range(1, latest_num + 1):
        url = f"https://xkcd.com/{n}/info.0.json"
        resp = requests.get(url, timeout=30)

        if resp.status_code == 404:
            continue  # e.g., comic 404 doesn't exist

        resp.raise_for_status()
        payload = resp.json()

        row = {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source": "all",
            "comic_num": int(payload.get("num", n)),
            "payload": json.dumps(payload),
        }

        errors = client.insert_rows_json(table_id, [row])
        if errors:
            raise RuntimeError(f"BigQuery insert failed for comic {n}: {errors}")

        print(f"Inserted comic #{n}")

if __name__ == "__main__":
    main()
