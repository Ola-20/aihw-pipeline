import os
import requests
import math

# -----------------------------
# Config
# -----------------------------
WEBHOOK_URL = os.getenv("FIVETRAN_WEBHOOK_URL")
if not WEBHOOK_URL:
    raise ValueError("FIVETRAN_WEBHOOK_URL environment variable not set")

MEASURE_CODE = "MYH0024"
AIHW_URL = f"https://myhospitalsapi.aihw.gov.au/api/v1/measures/{MEASURE_CODE}/data-items"

BATCH_SIZE = 500   # safe size for webhook ingestion

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# -----------------------------
# Step 1: Fetch AIHW data
# -----------------------------
print("Fetching AIHW data...")
resp = requests.get(AIHW_URL, headers=HEADERS)
resp.raise_for_status()

data = resp.json()

if "result" not in data:
    raise ValueError("AIHW response missing 'result' key")

records = data["result"]
total_records = len(records)

print(f"Fetched {total_records} records from AIHW")

# -----------------------------
# Step 2: Send to webhook in batches
# -----------------------------
num_batches = math.ceil(total_records / BATCH_SIZE)
print(f"Sending data in {num_batches} batches...")

for i in range(num_batches):
    start = i * BATCH_SIZE
    end = start + BATCH_SIZE
    batch = records[start:end]

    payload = [
        {
            "measure_code": MEASURE_CODE,
            "batch_number": i + 1,
            "total_batches": num_batches,
            "record": r
        }
        for r in batch
    ]

    r = requests.post(WEBHOOK_URL, json=payload)
    r.raise_for_status()

    print(f"Batch {i + 1}/{num_batches} sent successfully")

print("All AIHW data sent to Fivetran webhook ✅")