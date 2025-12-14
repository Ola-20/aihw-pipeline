import os
import requests

# Get webhook URL from GitHub Actions secret
WEBHOOK_URL = os.getenv("FIVETRAN_WEBHOOK_URL")

if not WEBHOOK_URL:
    raise ValueError("FIVETRAN_WEBHOOK_URL environment variable not set")

test_payload = {
    "source": "github_actions",
    "message": "Simple webhook test from GitHub Actions",
    "records": 1
}

print("Sending test payload to Fivetran webhook...")
resp = requests.post(WEBHOOK_URL, json=test_payload)

print("Status code:", resp.status_code)
print("Response body:", resp.text)