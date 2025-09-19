#!/usr/bin/env python3
"""
Data integrity verification script for Cratejoy customer collection.
Identifies missing records and gaps in the collected data.
"""

import os
import json
from typing import Dict, List, Tuple, Set
from utils.cratejoy_client import CratejoyClient
from utils.database import DatabaseManager
from sqlalchemy import text

def get_collected_customer_ids() -> Set[int]:
    """Get all customer IDs that have been collected and stored in database"""
    db_manager = DatabaseManager(os.getenv('DATABASE_URL'))
    session = db_manager.get_session()
    
    try:
        result = session.execute(text("SELECT cratejoy_id FROM cratejoy_customers ORDER BY cratejoy_id"))
        collected_ids = {row[0] for row in result.fetchall()}
        return collected_ids
    finally:
        session.close()

def get_cratejoy_customer_count() -> int:
    """Get total customer count from Cratejoy API"""
    cratejoy_client = CratejoyClient(
        os.getenv("CRATEJOY_API_KEY"), 
        os.getenv("CRATEJOY_DOMAIN"), 
        os.getenv("CRATEJOY_CLIENT_SECRET", "")
    )
    
    # Get first page to extract total count
    response = cratejoy_client.get_customers(limit=1, page=0)
    return response.get('count', 0)

def sample_cratejoy_pages(pages_to_check: List[int]) -> Dict[int, List[int]]:
    """Sample specific pages from Cratejoy to identify missing records"""
    cratejoy_client = CratejoyClient(
        os.getenv("CRATEJOY_API_KEY"), 
        os.getenv("CRATEJOY_DOMAIN"), 
        os.getenv("CRATEJOY_CLIENT_SECRET", "")
    )
    
    page_customers = {}
    
    for page in pages_to_check:
        print(f"Checking page {page}...")
        try:
            response = cratejoy_client.get_customers(limit=1000, page=page)
            customers = response.get('results', [])
            customer_ids = [customer['id'] for customer in customers]
            page_customers[page] = customer_ids
            print(f"Page {page}: {len(customer_ids)} customers")
        except Exception as e:
            print(f"Error checking page {page}: {e}")
            page_customers[page] = []
    
    return page_customers

def find_missing_records(collected_ids: Set[int], sample_pages: Dict[int, List[int]]) -> List[int]:
    """Find customer IDs that exist in Cratejoy but missing from our collection"""
    missing_ids = []
    
    for page, customer_ids in sample_pages.items():
        for customer_id in customer_ids:
            if customer_id not in collected_ids:
                missing_ids.append(customer_id)
                print(f"Missing customer ID: {customer_id} (found on page {page})")
    
    return missing_ids

def analyze_id_gaps(collected_ids: Set[int]) -> List[Tuple[int, int]]:
    """Analyze gaps in customer ID sequence"""
    if not collected_ids:
        return []
    
    sorted_ids = sorted(collected_ids)
    gaps = []
    
    for i in range(len(sorted_ids) - 1):
        current_id = sorted_ids[i]
        next_id = sorted_ids[i + 1]
        
        # If there's a gap larger than 1000, it might indicate missing pages
        if next_id - current_id > 1000:
            gaps.append((current_id, next_id))
    
    return gaps

def generate_integrity_report() -> Dict:
    """Generate comprehensive data integrity report"""
    print("Starting data integrity verification...")
    
    # Get collected data
    print("Loading collected customer IDs...")
    collected_ids = get_collected_customer_ids()
    collected_count = len(collected_ids)
    
    # Get Cratejoy total
    print("Fetching Cratejoy total count...")
    cratejoy_total = get_cratejoy_customer_count()
    
    # Analyze gaps in collected data
    print("Analyzing ID sequence gaps...")
    id_gaps = analyze_id_gaps(collected_ids)
    
    # Sample problematic pages (118-119 and around gaps)
    pages_to_check = [118, 119]
    
    # Add pages around large gaps
    for gap_start, gap_end in id_gaps[:5]:  # Check first 5 large gaps
        # Estimate pages based on typical customer density
        start_page = max(0, (gap_start // 1000) - 1)
        end_page = (gap_end // 1000) + 1
        pages_to_check.extend([start_page, end_page])
    
    print(f"Sampling pages: {sorted(set(pages_to_check))}")
    sample_data = sample_cratejoy_pages(sorted(set(pages_to_check)))
    
    # Find missing records
    missing_ids = find_missing_records(collected_ids, sample_data)
    
    # Generate report
    report = {
        "collection_stats": {
            "collected_count": collected_count,
            "cratejoy_total": cratejoy_total,
            "collection_percentage": (collected_count / cratejoy_total * 100) if cratejoy_total > 0 else 0,
            "estimated_missing": cratejoy_total - collected_count
        },
        "id_analysis": {
            "min_id": min(collected_ids) if collected_ids else 0,
            "max_id": max(collected_ids) if collected_ids else 0,
            "large_gaps": id_gaps,
            "gap_count": len(id_gaps)
        },
        "sampling_results": {
            "pages_checked": list(sample_data.keys()),
            "missing_records_found": missing_ids,
            "missing_count_in_sample": len(missing_ids)
        },
        "recommendations": []
    }
    
    # Add recommendations
    if report["collection_stats"]["estimated_missing"] > 1000:
        report["recommendations"].append("Significant missing records detected - full verification recommended")
    
    if missing_ids:
        report["recommendations"].append(f"Found {len(missing_ids)} missing records in sampled pages - backfill needed")
    
    if id_gaps:
        report["recommendations"].append(f"Found {len(id_gaps)} large gaps in ID sequence - check corresponding pages")
    
    return report

def save_report(report: Dict, filename: str = "data_integrity_report.json"):
    """Save integrity report to file"""
    with open(filename, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"Report saved to {filename}")

def print_report_summary(report: Dict):
    """Print human-readable report summary"""
    stats = report["collection_stats"]
    
    print("\n" + "="*60)
    print("DATA INTEGRITY REPORT SUMMARY")
    print("="*60)
    
    print(f"Collected Records: {stats['collected_count']:,}")
    print(f"Cratejoy Total: {stats['cratejoy_total']:,}")
    print(f"Collection Progress: {stats['collection_percentage']:.1f}%")
    print(f"Estimated Missing: {stats['estimated_missing']:,}")
    
    if report["id_analysis"]["large_gaps"]:
        print(f"\nLarge ID Gaps Found: {report['id_analysis']['gap_count']}")
        for i, (start, end) in enumerate(report["id_analysis"]["large_gaps"][:3]):
            print(f"  Gap {i+1}: {start:,} → {end:,} (difference: {end-start:,})")
    
    if report["sampling_results"]["missing_records_found"]:
        print(f"\nMissing Records in Sample: {report['sampling_results']['missing_count_in_sample']}")
        missing_ids = report["sampling_results"]["missing_records_found"][:10]
        print(f"  Sample IDs: {missing_ids}")
    
    if report["recommendations"]:
        print("\nRecommendations:")
        for rec in report["recommendations"]:
            print(f"  • {rec}")
    
    print("="*60)

if __name__ == "__main__":
    try:
        report = generate_integrity_report()
        print_report_summary(report)
        save_report(report)
        
        print(f"\nVerification complete. Check data_integrity_report.json for full details.")
        
    except Exception as e:
        print(f"Error during integrity verification: {e}")
        import traceback
        traceback.print_exc()