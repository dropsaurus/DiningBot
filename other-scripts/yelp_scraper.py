from dotenv import load_dotenv
load_dotenv()

import os, time, json, random
from datetime import datetime, timezone
import requests
import boto3
from decimal import Decimal

# ----------------------
# CONFIG
# ----------------------
YELP_API_KEY = os.getenv("YELP_API_KEY", "")  # Set via environment variable
YELP_ENDPOINT = "https://api.yelp.com/v3/businesses/search"
YELP_HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
DDB_TABLE = os.getenv("DDB_TABLE", "yelp-restaurants")

LOCATION = "Manhattan, NY"
PER_CUISINE_TARGET = 200
PAGE_LIMIT = 50
CUISINES = ["chinese", "italian", "mexican", "thai", "indian", "mediterranean"]

SLEEP_BETWEEN_CALLS = 0.35

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(DDB_TABLE)

def to_decimal(value):
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: to_decimal(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [to_decimal(v) for v in value if v is not None]
    return value

def yelp_search(term, location, limit=50, offset=0, categories=None):
    params = {
        "location": location,
        "limit": limit,
        "offset": offset
    }
    if categories:
        params["categories"] = categories
    else:
        params["term"] = f"{term} restaurants"

    r = requests.get(YELP_ENDPOINT, headers=YELP_HEADERS, params=params, timeout=30)

    if r.status_code == 429:
        time.sleep(2.0)
        r = requests.get(YELP_ENDPOINT, headers=YELP_HEADERS, params=params, timeout=30)

    if not r.ok:
        # Helpful diagnostics
        try:
            print("Yelp error:", r.status_code, r.text)
        except Exception:
            pass
        r.raise_for_status()

    return r.json()

def to_ddb_item(b, cuisine):
    addr = ", ".join(b.get("location", {}).get("display_address", []) or [])
    coords = b.get("coordinates", {})
    item = {
        "BusinessID": b.get("id"),  # ← Changed to match DynamoDB partition key
        "Name": b.get("name"),
        "Address": addr,
        "Coordinates": {
            "Latitude": coords.get("latitude"),
            "Longitude": coords.get("longitude")
        },
        "NumberOfReviews": b.get("review_count"),
        "Rating": b.get("rating"),
        "ZipCode": (b.get("location", {}) or {}).get("zip_code"),
        "Cuisine": cuisine,
        "insertedAtTimestamp": datetime.now(timezone.utc).isoformat()
    }
    return to_decimal(item)

def put_batch(items):
    with table.batch_writer(overwrite_by_pkeys=["BusinessID"]) as batch:  # ← Changed
        for it in items:
            batch.put_item(Item=it)

def harvest_cuisine(cuisine, global_seen_ids):
    print(f"\n=== Harvesting {cuisine} ===")
    collected, local_seen = [], set()

    offset = 0
    consecutive_empty = 0  # Track empty responses
    
    while len(collected) < PER_CUISINE_TARGET:
        try:
            limit = PAGE_LIMIT
            
            # Don't exceed target
            if len(collected) + limit > PER_CUISINE_TARGET:
                limit = PER_CUISINE_TARGET - len(collected)

            data = yelp_search(cuisine, LOCATION, limit=limit, offset=offset, categories=cuisine)
        except requests.HTTPError as e:
            print(f"HTTPError at offset {offset}: {e}")
            break

        businesses = data.get("businesses", [])
        
        if not businesses:
            consecutive_empty += 1
            if consecutive_empty >= 3:  # Stop after 3 empty responses
                print(f"[{cuisine}] No more results available")
                break
        else:
            consecutive_empty = 0

        new_in_this_batch = 0
        for b in businesses:
            bid = b.get("id")
            if not bid or bid in global_seen_ids or bid in local_seen:
                continue
            item = to_ddb_item(b, cuisine)
            collected.append(item)
            local_seen.add(bid)
            global_seen_ids.add(bid)
            new_in_this_batch += 1

            if len(collected) >= PER_CUISINE_TARGET:
                break

        print(f"[{cuisine}] collected: {len(collected)}/{PER_CUISINE_TARGET} (offset {offset}, new: {new_in_this_batch})")
        
        # If no new restaurants in this batch, move forward anyway
        if new_in_this_batch == 0:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                print(f"[{cuisine}] Too many batches with no new restaurants")
                break
        
        offset += limit  # IMPORTANT: Always increment offset
        time.sleep(SLEEP_BETWEEN_CALLS)
        
        # Yelp max offset
        if offset >= 1000:
            print(f"[{cuisine}] Reached Yelp API max offset")
            break

    if collected:
        put_batch(collected)
    print(f"=== {cuisine}: wrote {len(collected)} items ===")
    return len(collected)

def main():
    if not YELP_API_KEY:
        raise RuntimeError("Set YELP_API_KEY env var with your Yelp Fusion API key.")

    table.load()
    print(f"Writing to DynamoDB table: {DDB_TABLE} in {AWS_REGION}")

    global_seen = set()
    totals = {}

    for cuisine in CUISINES:
        count = harvest_cuisine(cuisine, global_seen)
        totals[cuisine] = count

    total = sum(totals.values())
    print("\n==== Done ====")
    print(json.dumps(totals, indent=2))
    print(f"TOTAL WRITTEN: {total}")

if __name__ == "__main__":  # ← Fixed
    main()