import requests
import pandas as pd
import time
import json
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
        
        # The API returns either a list directly or a dict with stores
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            stores = data.get('stores', [])
            if not stores:
                stores = data.get('paginatedStores', {}).get('stores', [])
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
        import traceback
        traceback.print_exc()
        return []

def extract_store_details(store: Dict) -> Dict:
    """
    Extract store details including hours and amenities
    """
    store_info = {
        'store_id': store.get('storeNumber') or store.get('id') or store.get('store_number'),
        'store_name': store.get('name') or store.get('storeName'),
        'brand': store.get('brandName') or store.get('brand'),
        'address': store.get('address', {}).get('streetAddressLine1') if isinstance(store.get('address'), dict) else store.get('streetAddress'),
        'city': store.get('address', {}).get('city') if isinstance(store.get('address'), dict) else store.get('city'),
        'state': store.get('address', {}).get('countrySubdivisionCode') if isinstance(store.get('address'), dict) else store.get('state'),
        'zip': store.get('address', {}).get('postalCode') if isinstance(store.get('address'), dict) else store.get('zip') or store.get('postalCode'),
        'country': store.get('address', {}).get('countryCode') if isinstance(store.get('address'), dict) else store.get('country'),
        'latitude': store.get('coordinates', {}).get('latitude') if isinstance(store.get('coordinates'), dict) else store.get('latitude') or store.get('lat'),
        'longitude': store.get('coordinates', {}).get('longitude') if isinstance(store.get('coordinates'), dict) else store.get('longitude') or store.get('lng'),
        'phone': store.get('phoneNumber') or store.get('phone'),
        'is_open': store.get('open') or store.get('isOpen'),
        'closing_soon': store.get('closingSoon', False),
    }
    
    # Extract amenities
    amenities = store.get('amenities', [])
    
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
        elif code == 'MO':
            amenity_dict['has_mobile_order'] = True
        elif code == 'WA':
            amenity_dict['has_oven_warmed_food'] = True
        elif code == 'NB':
            amenity_dict['has_nitro_cold_brew'] = True
        elif code == 'CS':
            amenity_dict['has_cafe_seating'] = True
        elif code == 'OR':
            amenity_dict['has_redeem_rewards'] = True
        elif code == 'OS':
            amenity_dict['has_outdoor_seating'] = True
        elif code == '16':
            amenity_dict['has_in_store'] = True
        elif code == 'WF':
            amenity_dict['has_wifi'] = True
    
    store_info.update(amenity_dict)
    
    # Extract regular hours
    regular_hours = store.get('regularHours', [])
    
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
    
    for day_hours in regular_hours:
        day = day_hours.get('dayOfWeek', '').upper()
        hours_formatted = day_hours.get('hoursFormatted', '')
        
        if day in days_map:
            short_day = days_map[day]
            store_info[f'{short_day}_hours'] = hours_formatted
            
            # Also extract structured open/close times
            if ' to ' in hours_formatted or ' - ' in hours_formatted:
                try:
                    separator = ' to ' if ' to ' in hours_formatted else ' - '
                    parts = hours_formatted.split(separator)
                    if len(parts) == 2:
                        store_info[f'{short_day}_open'] = parts[0].strip()
                        store_info[f'{short_day}_close'] = parts[1].strip()
                except:
                    pass
    
    # Capture current status
    open_status = store.get('openStatusFormatted', '')
    store_info['current_status'] = open_status
    
    return store_info

def test_single_location_debug():
    """Test and show raw JSON structure"""
    lat, lng = 32.38431, -99.76757
    
    print(f"Testing with coordinates: {lat}, {lng}\n")
    
    url = "https://www.starbucks.com/apiproxy/v1/locations"
    params = {'lat': lat, 'lng': lng}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': f'https://www.starbucks.com/store-locator?map={lat},{lng},12z',
        'x-requested-with': 'XMLHttpRequest',
    }
    
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    
    print("="*80)
    print("RAW JSON STRUCTURE:")
    print("="*80)
    print(json.dumps(data[0] if isinstance(data, list) and len(data) > 0 else data, indent=2))
    print("\n")
    
    return data

if __name__ == "__main__":
    print("Debugging API response structure...\n")
    raw_data = test_single_location_debug()
