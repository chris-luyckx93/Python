import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import time
from typing import List, Dict

def get_starbucks_stores_by_location(location: str) -> List[Dict]:
    """
    Fetch Starbucks store data for a given location (zip code, city, state, etc.)
    """
    url = f"https://www.starbucks.com/store-locator?place={location}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the script tag containing the JSON data
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and 'window.__BOOTSTRAP' in script.string:
                # Extract and clean the JSON
                json_text = script.string
                json_text = json_text.split('window.__BOOTSTRAP = ')[1]
                json_text = json_text.split('window.__INTL_MESSAGES')[0]
                json_text = json_text.rstrip(';\n ')
                
                data = json.loads(json_text)
                
                # Extract store locations
                locations = data.get('storeLocator', {}).get('locationState', {}).get('locations', [])
                return locations
                
    except Exception as e:
        print(f"Error fetching data for {location}: {str(e)}")
        return []
    
    return []

def extract_store_hours(store_data: Dict) -> Dict:
    """
    Extract and format store hours from the store data
    """
    store_info = {
        'store_id': store_data.get('storeNumber'),
        'store_name': store_data.get('name'),
        'brand': store_data.get('brandName'),
        'address': store_data.get('address', {}).get('streetAddressLine1'),
        'city': store_data.get('address', {}).get('city'),
        'state': store_data.get('address', {}).get('countrySubdivisionCode'),
        'zip': store_data.get('address', {}).get('postalCode'),
        'latitude': store_data.get('coordinates', {}).get('latitude'),
        'longitude': store_data.get('coordinates', {}).get('longitude'),
        'is_open': store_data.get('open'),
    }
    
    # Extract hours (usually in regularHours or regularSchedule)
    hours = store_data.get('regularHours', [])
    if not hours:
        hours = store_data.get('schedules', {}).get('regularSchedule', [])
    
    # Parse hours for each day
    for day_hours in hours:
        day = day_hours.get('day', day_hours.get('dayOfWeek', 'Unknown'))
        opens = day_hours.get('open', day_hours.get('opens', 'N/A'))
        closes = day_hours.get('close', day_hours.get('closes', 'N/A'))
        
        store_info[f'{day}_open'] = opens
        store_info[f'{day}_close'] = closes
    
    return store_info

def get_us_zip_codes() -> List[str]:
    """
    Get a list of US zip codes. You can use a CSV file or API for comprehensive coverage.
    For demonstration, this returns a sample list.
    """
    # Option 1: Load from a file
    # df = pd.read_csv('us_zip_codes.csv')
    # return df['zip'].tolist()
    
    # Option 2: For demo purposes, sample zip codes covering major areas
    # You'd want to get a comprehensive list from: https://www.unitedstateszipcodes.org/
    sample_zips = ['10001', '90210', '60601', '77001', '19101']  # Expand this list
    return sample_zips

def scrape_all_starbucks_stores() -> pd.DataFrame:
    """
    Main function to scrape all Starbucks stores in the US
    """
    all_stores = []
    seen_store_ids = set()
    
    zip_codes = get_us_zip_codes()
    
    for i, zip_code in enumerate(zip_codes):
        print(f"Processing {zip_code} ({i+1}/{len(zip_codes)})")
        
        stores = get_starbucks_stores_by_location(zip_code)
        
        for store in stores:
            store_id = store.get('storeNumber')
            
            # Avoid duplicates
            if store_id and store_id not in seen_store_ids:
                seen_store_ids.add(store_id)
                store_info = extract_store_hours(store)
                all_stores.append(store_info)
        
        # Be respectful with rate limiting
        time.sleep(1)
    
    df = pd.DataFrame(all_stores)
    return df

# Usage
if __name__ == "__main__":
    # Test with a single location first
    print("Testing with New York...")
    stores = get_starbucks_stores_by_location("New York, NY")
    print(f"Found {len(stores)} stores")
    
    if stores:
        sample_store = extract_store_hours(stores[0])
        print("\nSample store data:")
        for key, value in sample_store.items():
            print(f"{key}: {value}")
    
    # Uncomment to run full scrape
    # df = scrape_all_starbucks_stores()
    # df.to_csv('starbucks_all_stores_hours.csv', index=False)
    # print(f"\nTotal stores scraped: {len(df)}")
