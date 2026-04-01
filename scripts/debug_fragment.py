"""
One-shot debug: fetch a single ?dbut=true fragment and print the raw response.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.ftl_collector import _get_client, FTL_BASE
import time

# The confirmed pool containing PANGA Daniel Jason
url = (
    f"{FTL_BASE}/pools/scores"
    f"/0B46E82914D84C2ABBE2B00173850C46"
    f"/AF021EED5B9D443B867B23054B676183"
    f"/F336F034D0BF459A8310C7A5A48EA07D?dbut=true"
)

client = _get_client()
time.sleep(1.5)
r = client.get(url)

print(f"Status  : {r.status_code}")
print(f"URL     : {r.url}")
print(f"Headers : {dict(r.headers)}")
print(f"Length  : {len(r.text)} chars")
print(f"\n--- First 2000 chars of body ---\n")
print(r.text[:2000])
