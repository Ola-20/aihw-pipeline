import os
import requests
import math
from pathlib import Path

WEBHOOK_URL = os.getenv("FIVETRAN_WEBHOOK_URL")
if not WEBHOOK_URL:
    raise ValueError("FIVETRAN_WEBHOOK_URL environment variable not set")

MEASURE_CODE = "MYH0024"
AIHW_URL = f"https://myhospitalsapi.aihw.gov.au/api/v1/measures/{MEASURE_CODE}/data-items"

BATCH_SIZE = 500
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

VERSION_FILE = Path("last_data_version.txt")


def read_last_version() -> int:
    if not VERSION_FILE.exists():
        return 0
    txt = VERSION_FILE.read_text().strip()
    return int(txt) if txt else 0


def write_last_version(v: int) -> None:
    VERSION_FILE.write_text(str(v) + "\n")


print("Fetching AIHW data...")
resp = requests.get(AIHW_URL, headers=HEADERS)
resp.raise_for_status()
data = resp.json()

version_info = data.get("version_information", {})
current_version = int(version_info.get("data_version", 0))
last_version = read_last_version()

print(f"Last ingested data_version: {last_version}")
print(f"Current AIHW data_version:   {current_version}")

if current_version == 0:
    raise ValueError("AIHW response missing version_information.data_version")

if current_version <= last_version:
    print("No new AIHW version. Skipping send ✅")
    raise SystemExit(0)

records = data.get("result", [])
print(f"New version detected! Records to send: {len(records)}")

num_batches = math.ceil(len(records) / BATCH_SIZE)
print(f"Sending {num_batches} batches...")

for i in range(num_batches):
    start = i * BATCH_SIZE
    end = start + BATCH_SIZE
    batch = records[start:end]

    payload = [
        {
            "measure_code": MEASURE_CODE,
            "data_version": current_version,
            "batch_number": i + 1,
            "total_batches": num_batches,
            "record": r
        }
        for r in batch
    ]

    r = requests.post(WEBHOOK_URL, json=payload)
    r.raise_for_status()
    print(f"Batch {i + 1}/{num_batches} sent")

print("All AIHW data sent ✅ Updating last_data_version.txt...")
write_last_version(current_version)
print("Updated last_data_version.txt ✅")