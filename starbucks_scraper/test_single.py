from starbucks_scraper import get_starbucks_stores_by_location, extract_store_hours
import json

# Test with a single location
stores = get_starbucks_stores_by_location("10001")
print(f"Found {len(stores)} stores near 10001")

if stores:
    # Print first store raw data to inspect structure
    print("\nRaw data structure:")
    print(json.dumps(stores[0], indent=2)[:1000])  # First 1000 chars
    
    # Extract hours
    store_info = extract_store_hours(stores[0])
    print("\nExtracted store info:")
    for key, value in store_info.items():
        print(f"{key}: {value}")
