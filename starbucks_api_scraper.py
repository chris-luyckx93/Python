import requests
import pandas as pd
import time
from typing import List, Dict, Tuple
import itertools

def get_starbucks_stores_by_coords(lat: float, lng: float) -> List[Dict]:
    """
    Fetch Starbucks stores using the working API endpoint
    """
    url = "https://www.starbucks.com/apiproxy/v1/locations"
    
    params = {
        'lat': lat,
        'lng': lng
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': f'https://www.starbucks.com/store-locator?map={lat},{lng},12z',
        'x-requested-with': 'XMLHttpRequest',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        # The API returns a list of objects, each with a "store" key
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            stores = data.get('stores', [])
            return stores
        else:
            return []
        
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error for coords ({lat}, {lng}): {e}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Request Error for coords ({lat}, {lng}): {e}")
        return []
    except Exception as e:
        print(f"Error for coords ({lat}, {lng}): {str(e)}")
        return []

def extract_store_details(store_wrapper: Dict) -> Dict:
    """
    Extract store details including hours and amenities
    The API returns objects with a "store" key containing the actual store data
    """
    # Extract the actual store object
    store = store_wrapper.get('store', store_wrapper)
    
    store_info = {
        'store_id': store.get('storeNumber'),
        'store_internal_id': store.get('id'),
        'store_name': store.get('name'),
        'ownership_type': store.get('ownershipTypeCode'),
        'address': store.get('address', {}).get('streetAddressLine1'),
        'city': store.get('address', {}).get('city'),
        'state': store.get('address', {}).get('countrySubdivisionCode'),
        'zip': store.get('address', {}).get('postalCode'),
        'country': store.get('address', {}).get('countryCode'),
        'latitude': store.get('coordinates', {}).get('latitude'),
        'longitude': store.get('coordinates', {}).get('longitude'),
        'phone': store.get('phoneNumber'),
        'is_open': store.get('open'),
        'is_open_24_hours': store.get('isOpen24Hours'),
        'closing_soon': store.get('closingSoon', False),
        'current_status': store.get('openStatusFormatted', ''),
        'hours_status': store.get('hoursStatusFormatted', ''),
        'slug': store.get('slug'),
        'timezone': store.get('timeZone', {}).get('timeZoneId') if store.get('timeZone') else None,
    }
    
    # Add distance and recommendation info if available
    store_info['distance'] = store_wrapper.get('distance', None)
    store_info['is_nearby'] = store_wrapper.get('isNearby', None)
    
    # Extract amenities
    amenities = store.get('amenities', []) or []
    
    # Initialize amenity flags
    amenity_dict = {
        'has_drive_thru': False,
        'has_mobile_order': False,
        'has_oven_warmed_food': False,
        'has_nitro_cold_brew': False,
        'has_cafe_seating': False,
        'has_redeem_rewards': False,
        'has_outdoor_seating': False,
        'has_in_store': False,
        'has_wifi': False
    }
    
    # Map amenity codes
    for amenity in amenities:
        code = amenity.get('code', '')
        
        if code == 'DT':
            amenity_dict['has_drive_thru'] = True
        elif code == 'XO':
            amenity_dict['has_mobile_order'] = True
        elif code == 'WA':
            amenity_dict['has_oven_warmed_food'] = True
        elif code == 'NB':
            amenity_dict['has_nitro_cold_brew'] = True
        elif code == 'CS':
            amenity_dict['has_cafe_seating'] = True
        elif code == 'DR':
            amenity_dict['has_redeem_rewards'] = True
        elif code == 'OS':
            amenity_dict['has_outdoor_seating'] = True
        elif code == '16':
            amenity_dict['has_in_store'] = True
        elif code == 'GO':
            amenity_dict['has_wifi'] = True
    
    store_info.update(amenity_dict)
    
    # Extract schedule (hours for each day) - FIXED
    schedule = store.get('schedule', [])
    if schedule is None:
        schedule = []
    
    # Parse hours for each day of week
    days_map = {
        'MONDAY': 'mon',
        'TUESDAY': 'tue',
        'WEDNESDAY': 'wed',
        'THURSDAY': 'thu',
        'FRIDAY': 'fri',
        'SATURDAY': 'sat',
        'SUNDAY': 'sun'
    }
    
    for day_hours in schedule:
        day = day_hours.get('dayOfWeek', '').upper()
        hours_formatted = day_hours.get('hoursFormatted', '')
        
        if day in days_map:
            short_day = days_map[day]
            store_info[f'{short_day}_hours'] = hours_formatted
            
            # Also extract structured open/close times
            if ' to ' in hours_formatted:
                try:
                    parts = hours_formatted.split(' to ')
                    if len(parts) == 2:
                        store_info[f'{short_day}_open'] = parts[0].strip()
                        store_info[f'{short_day}_close'] = parts[1].strip()
                except:
                    pass
    
    # Extract mobile ordering availability
    mobile_ordering = store.get('mobileOrdering', {}) or {}
    store_info['mobile_ordering_availability'] = mobile_ordering.get('availability')
    store_info['guest_ordering'] = mobile_ordering.get('guestOrdering')
    
    return store_info

def generate_us_grid_coordinates(grid_size: float = 0.75) -> List[Tuple[float, float]]:
    """
    Generate a grid of lat/lng coordinates covering the continental US
    """
    lat_min, lat_max = 24.5, 49.5
    lng_min, lng_max = -125.0, -66.0
    
    lats = [lat_min + i * grid_size for i in range(int((lat_max - lat_min) / grid_size) + 1)]
    lngs = [lng_min + i * grid_size for i in range(int((lng_max - lng_min) / grid_size) + 1)]
    
    coords = list(itertools.product(lats, lngs))
    return coords

def resume_scrape_from_checkpoint(checkpoint_file='checkpoint_800.csv', start_point=820):
    """Resume scraping from the last checkpoint"""
    
    # Load existing data
    df_existing = pd.read_csv(checkpoint_file)
    seen_store_ids = set(df_existing['store_id'].dropna())
    all_stores = df_existing.to_dict('records')
    
    print(f"Resuming from checkpoint with {len(seen_store_ids)} stores")
    
    # Generate all coordinates
    coordinates = generate_us_grid_coordinates(grid_size=0.40)
    
    print(f"Starting from point {start_point}/{len(coordinates)}")
    print(f"Estimated time remaining: {(len(coordinates) - start_point) * 1.2 / 60:.1f} minutes\n")
    
    for i, (lat, lng) in enumerate(coordinates[start_point:], start=start_point):
        if (i + 1) % 20 == 0:
            print(f"Progress: {i+1}/{len(coordinates)} points | {len(seen_store_ids)} unique stores found")
        
        stores = get_starbucks_stores_by_coords(lat, lng)
        
        for store_wrapper in stores:
            store = store_wrapper.get('store', store_wrapper)
            store_id = store.get('storeNumber')
            
            country = store.get('address', {}).get('countryCode', '')
            if country != 'US':
                continue
            
            if store_id and store_id not in seen_store_ids:
                seen_store_ids.add(store_id)
                store_info = extract_store_details(store_wrapper)
                all_stores.append(store_info)
        
        time.sleep(1.2)
        
        if (i + 1) % 200 == 0 and all_stores:
            checkpoint_df = pd.DataFrame(all_stores)
            checkpoint_df.to_csv(f'checkpoint_{i+1}.csv', index=False)
            print(f"✓ Checkpoint saved: {len(all_stores)} stores")
    
    df = pd.DataFrame(all_stores)
    return df

def scrape_all_starbucks_us() -> pd.DataFrame:
    """
    Scrape all Starbucks stores in the US using coordinate grid
    """
    all_stores = []
    seen_store_ids = set()
    
    coordinates = generate_us_grid_coordinates(grid_size=0.75)
    
    print(f"Generated {len(coordinates)} coordinate points to search")
    print(f"Estimated time: {len(coordinates) * 1.2 / 60:.1f} minutes\n")
    
    for i, (lat, lng) in enumerate(coordinates):
        if (i + 1) % 20 == 0:
            print(f"Progress: {i+1}/{len(coordinates)} points | {len(seen_store_ids)} unique stores found")
        
        stores = get_starbucks_stores_by_coords(lat, lng)
        
        for store_wrapper in stores:
            store = store_wrapper.get('store', store_wrapper)
            store_id = store.get('storeNumber')
            
            country = store.get('address', {}).get('countryCode', '')
            if country != 'US':
                continue
            
            if store_id and store_id not in seen_store_ids:
                seen_store_ids.add(store_id)
                store_info = extract_store_details(store_wrapper)
                all_stores.append(store_info)
        
        time.sleep(1.2)
        
        if (i + 1) % 200 == 0 and all_stores:
            checkpoint_df = pd.DataFrame(all_stores)
            checkpoint_df.to_csv(f'checkpoint_{i+1}.csv', index=False)
            print(f"✓ Checkpoint saved: {len(all_stores)} stores")
    
    df = pd.DataFrame(all_stores)
    return df

if __name__ == "__main__":
    import sys
    
    # Check if we should resume from checkpoint
    if len(sys.argv) > 1 and sys.argv[1] == 'resume':
        print("Resuming from last checkpoint...\n")
        df = resume_scrape_from_checkpoint('checkpoint_800.csv', start_point=820)
        df.to_csv('starbucks_us_stores_complete.csv', index=False)
        print(f"\n✓ Complete! Scraped {len(df)} unique US Starbucks stores")
        print(f"✓ Saved to: starbucks_us_stores_complete.csv")
    else:
        print("Starting fresh scrape...\n")
        df = scrape_all_starbucks_us()
        df.to_csv('starbucks_us_stores_complete.csv', index=False)
        print(f"\n✓ Complete! Scraped {len(df)} unique US Starbucks stores")
