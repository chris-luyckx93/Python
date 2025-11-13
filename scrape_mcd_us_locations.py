import requests
import json
import time
from itertools import product
import csv

def get_mcdonalds_locations(latitude, longitude, radius=50, max_results=100):
    """
    Fetch McDonald's locations for a given coordinate.
    
    Args:
        latitude: Center latitude
        longitude: Center longitude
        radius: Search radius in miles (default 50)
        max_results: Maximum results to return (default 100)
    
    Returns:
        List of location dictionaries
    """
    url = "https://www.mcdonalds.com/googleappsv2/geolocation"
    
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'radius': radius,
        'maxResults': max_results,
        'country': 'us',
        'language': 'en-us'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get('features', [])
    except Exception as e:
        print(f"Error fetching data for ({latitude}, {longitude}): {e}")
        return []

def generate_us_grid(lat_step=0.5, lon_step=0.5):
    """
    Generate a grid of coordinates covering the continental US + Alaska + Hawaii.
    
    Args:
        lat_step: Latitude step size in degrees (0.5 = ~35 miles)
        lon_step: Longitude step size in degrees (0.5 = ~35 miles)
    
    Returns:
        List of (latitude, longitude) tuples
    """
    grids = []
    
    # Continental US (expanded bounds)
    lat_min, lat_max = 24, 50
    lon_min, lon_max = -125, -66
    
    for lat in [lat_min + i * lat_step for i in range(int((lat_max - lat_min) / lat_step) + 1)]:
        for lon in [lon_min + i * lon_step for i in range(int((lon_max - lon_min) / lon_step) + 1)]:
            grids.append((lat, lon))
    
    # Alaska
    ak_lat_min, ak_lat_max = 51, 72
    ak_lon_min, ak_lon_max = -169, -130
    for lat in [ak_lat_min + i * lat_step for i in range(int((ak_lat_max - ak_lat_min) / lat_step) + 1)]:
        for lon in [ak_lon_min + i * lon_step for i in range(int((ak_lon_max - ak_lon_min) / lon_step) + 1)]:
            grids.append((lat, lon))
    
    # Hawaii
    hi_lat_min, hi_lat_max = 18.5, 22.5
    hi_lon_min, hi_lon_max = -161, -154
    for lat in [hi_lat_min + i * lat_step for i in range(int((hi_lat_max - hi_lat_min) / lat_step) + 1)]:
        for lon in [hi_lon_min + i * lon_step for i in range(int((hi_lon_max - hi_lon_min) / lon_step) + 1)]:
            grids.append((lat, lon))
    
    return grids
def extract_location_data(feature):
    """
    Extract relevant data from a location feature.
    
    Args:
        feature: Feature dictionary from API response
    
    Returns:
        Dictionary with cleaned location data
    """
    coords = feature.get('geometry', {}).get('coordinates', [None, None])
    props = feature.get('properties', {})
    
    # Get store identifier
    identifiers = props.get('identifiers', {}).get('storeIdentifier', [])
    store_number = None
    for ident in identifiers:
        if ident.get('identifierType') == 'NATLSTRNUMBER':
            store_number = ident.get('identifierValue')
            break
    
    return {
        'store_number': store_number,
        'longitude': coords[0],
        'latitude': coords[1],
        'name': props.get('shortDescription', ''),
        'address': props.get('addressLine1', ''),
        'city': props.get('addressLine3', ''),
        'state': props.get('subDivision', ''),
        'zipcode': props.get('postcode', ''),
        'phone': props.get('telephone', ''),
        'hours_today': props.get('todayHours', ''),
        'open_status': props.get('openstatus', ''),
        'has_drive_thru': props.get('driveThru', '0'),
        'has_wifi': props.get('wifi', '0'),
        'has_indoor_dining': '1' if 'INDOORDINING' in props.get('filterType', []) else '0',
        'has_24_hours': '1' if 'TWENTYFOURHOURS' in props.get('filterType', []) else '0',
        'timezone': props.get('timeZone', '')
    }

def scrape_all_locations(output_file='mcdonalds_locations.csv', lat_step=1.0, lon_step=1.0, delay=0.5):
    """
    Scrape all McDonald's locations in the US and save to CSV.
    
    Args:
        output_file: Output CSV filename
        lat_step: Latitude grid step size
        lon_step: Longitude grid step size
        delay: Delay between requests in seconds
    """
    print("Generating coordinate grid...")
    grid = generate_us_grid(lat_step, lon_step)
    print(f"Generated {len(grid)} grid points")
    
    all_locations = {}  # Use dict to deduplicate by store_number
    
    print("Starting to scrape locations...")
    for i, (lat, lon) in enumerate(grid, 1):
        print(f"Progress: {i}/{len(grid)} - Checking ({lat:.2f}, {lon:.2f})")
        
        features = get_mcdonalds_locations(lat, lon, radius=40, max_results=100)
        
        for feature in features:
            location_data = extract_location_data(feature)
            store_num = location_data['store_number']
            
            if store_num and store_num not in all_locations:
                all_locations[store_num] = location_data
                print(f"  Found new location: {location_data['name']} - {location_data['city']}, {location_data['state']}")
        
        time.sleep(delay)  # Be respectful with API calls
    
    # Write to CSV
    print(f"\nWriting {len(all_locations)} locations to {output_file}...")
    
    if all_locations:
        fieldnames = list(next(iter(all_locations.values())).keys())
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_locations.values())
        
        print(f"Successfully saved {len(all_locations)} unique locations!")
    else:
        print("No locations found!")

def scrape_by_state(state_coords, output_file='mcdonalds_locations.csv'):
    """
    Scrape locations by searching around major cities/areas in each state.
    More efficient than grid search.
    
    Args:
        state_coords: Dictionary mapping state abbreviations to (lat, lon) tuples
        output_file: Output CSV filename
    """
    all_locations = {}
    
    for state, (lat, lon) in state_coords.items():
        print(f"Scraping {state}...")
        features = get_mcdonalds_locations(lat, lon, radius=100, max_results=100)
        
        for feature in features:
            location_data = extract_location_data(feature)
            store_num = location_data['store_number']
            
            if store_num and store_num not in all_locations:
                all_locations[store_num] = location_data
        
        time.sleep(0.5)
    
    # Write results
    if all_locations:
        fieldnames = list(next(iter(all_locations.values())).keys())
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_locations.values())
        
        print(f"Saved {len(all_locations)} locations to {output_file}")

# Example usage
if __name__ == "__main__":
    # Option 1: Grid search (thorough but slower)
    # scrape_all_locations('mcdonalds_all.csv', lat_step=1.0, lon_step=1.0, delay=0.5)
    
    # Option 2: Test with a single location
    print("Testing with New York City coordinates...")
    locations = get_mcdonalds_locations(40.7128, -74.0060, radius=10, max_results=50)
    print(f"Found {len(locations)} locations")
    
    for loc in locations[:3]:  # Print first 3
        data = extract_location_data(loc)
        print(f"\n{data['name']}")
        print(f"  Address: {data['address']}, {data['city']}, {data['state']} {data['zipcode']}")
        print(f"  Coordinates: ({data['latitude']}, {data['longitude']})")
        print(f"  Phone: {data['phone']}")
        print(f"  Status: {data['open_status']}")
    
    # Uncomment to run full scrape:
    scrape_all_locations('mcdonalds_locations.csv', lat_step=0.5, lon_step=0.5, delay=0.3)