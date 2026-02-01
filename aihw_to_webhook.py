import os
import requests
import math
from pathlib import Path

# get secret webhook link in env variable from GitHub Actions secret and set it as WEBHOOK_URL. 
# FIVETRAN_WEBHOOK_URL must have been set in the environment where this script runs i.e github actions secret
WEBHOOK_URL = os.getenv("FIVETRAN_WEBHOOK_URL") 

# raise (used to stop execution in programs immediately) error if WEBHOOK_URL is not set or if it is empty
# ValueError is a built-in exception in Python that is raised when a function receives an argument of the right type but an inappropriate value
if not WEBHOOK_URL:
    raise ValueError("FIVETRAN_WEBHOOK_URL environment variable not set")

#These 2 line below constructs the exact API endpoint URL that requests.get function later uses to fetch data 
MEASURE_CODE = "MYH0024"
AIHW_URL = f"https://myhospitalsapi.aihw.gov.au/api/v1/measures/{MEASURE_CODE}/data-items"

# batch size is used to generate number of batches for sending data to webhook in chunks
BATCH_SIZE = 500

# title case headers for the HTTP request to mimic a standard web browser request so that the server accepts it
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# Path library object pointing to the file that stores the last ingested data version which will be updated each time the script runs.
# the line used to update the file will be added later
VERSION_FILE = Path("last_data_version.txt")

# This function checks a file to see what data version was sent last time,If the file doesn’t exist or is empty, it returns 0
# If the file has a number, it returns that number.This helps the program avoid sending the same data again.
def read_last_version() -> int:
    if not VERSION_FILE.exists():
        return 0
    txt = VERSION_FILE.read_text().strip()
    return int(txt) if txt else 0

# This function below takes a number and writes it into a file. If the file does not exist, it creates it.
# If the file already exists, it replaces what is inside. The newline helps keep the file clean and readable.
# This function does not return anything hence we included -> None.
# Note: (v: int) indicates that the function expects an integer argument. it's not enforced but just a hint for readability
def write_last_version(v: int) -> None:
    VERSION_FILE.write_text(str(v) + "\n")

# I created a resp object which stores the response from the AIHW API when we make a GET request to fetch data.
# I check if the request was successful using raise_for_status(), which will raise an error if error code returned by the site 
# is equal to or greater than 400.
# I then parse the JSON response into a Python dictionary using resp.json() and store it in a variable called data.
print("Fetching AIHW data...")
resp = requests.get(AIHW_URL, headers=HEADERS)
resp.raise_for_status()
data = resp.json()

# I Safely extract the "version_information" section from the API response(data dict) to create a dictionary called version_info. 
# I use data.get("version_information", {}) instead of data["version_information"]
# so the script does not crash if the key is missing.If the key does not exist, an empty dictionary is returned as a safe default.

# I then extract the current data version number from this version_info dictionary, defaulting to 0 if it's not found.

# I read the last ingested data version from the local file using the read_last_version() function.
# I print both the last ingested version and the current version for comparison.
version_info = data.get("version_information", {})
current_version = int(version_info.get("data_version", 0))
last_version = read_last_version()

print(f"Last ingested data_version: {last_version}")
print(f"Current AIHW data_version:   {current_version}")

# If the current version is 0, it indicates that the version information is missing from the API response.
# In this case, I raise a ValueError to alert that the response is incomplete.
# If the current version is less than or equal to the last ingested version, it means there is no new data to send.
# I print a message indicating that there is no new version and exit the script gracefully using System
if current_version == 0:
    raise ValueError("AIHW response missing version_information.data_version")

if current_version <= last_version:
    print("No new AIHW version. Skipping send ✅")
    raise SystemExit(0)

# If a new version is detected (current_version > last_version), That is both "if" checks above are false
#  I proceed to extract the actual data records from the API response which is the "result" key. it should be a list else default to empty list
# I print the number of records to be sent.
records = data.get("result", [])
print(f"New version detected! Records to send: {len(records)}")

# Below, is a classic batching logic to send data in chunks to avoid overwhelming the receiving server or hitting payload size limits.
# I calculate the number of batches needed to send all records based on the BATCH_SIZE.
# math.ceil is used to round up to ensure all records are included even if the last batch is not full.
# that is if there are 1200 records and batch size is 500, we need 3 batches (2 full of 500 and 1 with 200)
num_batches = math.ceil(len(records) / BATCH_SIZE)
print(f"Sending {num_batches} batches...")


# after setting variables and functions above, I now loop over the number of batches to send each batch to the webhook URL.
# for each batch, I calculate the start and end indices to slice the records list.
# I create a payload which is iterating over each record in the records list for the current batch
# and constructing a dictionary for each record that includes the record itself and additional metadata to be sent to the webhook.
# I then make a POST request to the WEBHOOK_URL with the payload as JSON.
# After sending each batch, I print a confirmation message.
 
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


# After all batches are sent successfully, 
#           I update the last_data_version.txt file with the current version
#           using the function "write_last_version" i created earlier in the script,
# i print a message indicating that all data has been sent and the version file is being updated.
print("All AIHW data sent ✅ Updating last_data_version.txt...")
write_last_version(current_version)
print("Updated last_data_version.txt ✅")