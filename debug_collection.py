#!/usr/bin/env python3
"""
Debug script to check collection status and API response
"""
import os
from utils.cratejoy_client import CratejoyClient

# Calculate current page from record count
current_records = 118955
records_per_page = 1000
current_page = current_records // records_per_page
records_in_page = current_records % records_per_page

print(f"Current records: {current_records}")
print(f"Current page: {current_page}")
print(f"Records in current page: {records_in_page}")
print(f"Should check page: {current_page}")

# Test API at current position
try:
    cratejoy_client = CratejoyClient(
        os.getenv("CRATEJOY_API_KEY", ""), 
        "", 
        os.getenv("CRATEJOY_CLIENT_SECRET", "")
    )
    
    print(f"\nTesting API at page {current_page}...")
    response = cratejoy_client.get_customers(limit=1000, page=current_page)
    
    customers = response.get('results', [])
    print(f"Page {current_page} returned {len(customers)} customers")
    
    if customers:
        print(f"First customer ID: {customers[0].get('id')}")
        print(f"Last customer ID: {customers[-1].get('id')}")
    
    next_url = response.get('next')
    print(f"Next URL: {next_url}")
    
    if next_url:
        print(f"\nTesting next page...")
        page_119_response = cratejoy_client.get_customers(limit=1000, page=current_page + 1)
        page_119_customers = page_119_response.get('results', [])
        print(f"Page {current_page + 1} returned {len(page_119_customers)} customers")
        
        if page_119_customers:
            print(f"Next page first customer ID: {page_119_customers[0].get('id')}")
    else:
        print("No next page available - collection may be complete")

except Exception as e:
    print(f"API Error: {e}")