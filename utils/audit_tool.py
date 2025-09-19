"""
Audit Tool - Compare Database vs Cratejoy API
Identifies missing records, data discrepancies, and sync issues
"""
import json
import logging
from typing import Dict, List, Set, Optional, Any
from sqlalchemy import text
from utils.database import DatabaseManager
from utils.cratejoy_client import CratejoyClient

logger = logging.getLogger(__name__)

class DatabaseAuditor:
    """Audits database against Cratejoy API to find missing/inconsistent records"""

    def __init__(self, cratejoy_client: CratejoyClient, database_url: Optional[str] = None):
        self.cratejoy_client = cratejoy_client
        self.db_manager = DatabaseManager(database_url)

    def audit_page_range(self, start_page: int, end_page: int, batch_size: int = 1000) -> Dict[str, Any]:
        """
        Audit a specific range of pages against Cratejoy API

        Args:
            start_page: First page to audit
            end_page: Last page to audit (inclusive)
            batch_size: Records per page

        Returns:
            Audit results with missing records, extra records, etc.
        """
        logger.info(f"Starting audit of pages {start_page} to {end_page}")

        audit_results = {
            'pages_audited': [],
            'missing_from_db': [],  # In Cratejoy but not in DB
            'extra_in_db': [],      # In DB but not in expected range
            'data_mismatches': [],  # Different data between API and DB
            'api_errors': [],       # API calls that failed
            'summary': {
                'total_api_records': 0,
                'total_db_records': 0,
                'missing_count': 0,
                'extra_count': 0,
                'mismatch_count': 0
            }
        }

        # Get all customer IDs that should exist in this page range
        expected_customer_ids = set()

        for page in range(start_page, end_page + 1):
            try:
                logger.info(f"Auditing page {page}")

                # Fetch from Cratejoy API
                response = self.cratejoy_client.get_customers(limit=batch_size, page=page)
                customers = response.get('results', [])

                if not customers:
                    logger.warning(f"No customers returned for page {page}")
                    break

                page_customer_ids = set()
                for customer in customers:
                    customer_id = customer.get('id')
                    if customer_id:
                        expected_customer_ids.add(customer_id)
                        page_customer_ids.add(customer_id)

                audit_results['pages_audited'].append({
                    'page': page,
                    'api_count': len(customers),
                    'customer_ids': list(page_customer_ids)
                })

                audit_results['summary']['total_api_records'] += len(customers)
                logger.info(f"Page {page}: {len(customers)} customers from API")

            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                audit_results['api_errors'].append({
                    'page': page,
                    'error': str(e)
                })

        # Get customer IDs from database in the expected range
        db_customer_ids = self._get_db_customer_ids_in_range(expected_customer_ids)
        audit_results['summary']['total_db_records'] = len(db_customer_ids)

        # Find missing and extra records
        missing_ids = expected_customer_ids - db_customer_ids
        extra_ids = db_customer_ids - expected_customer_ids

        audit_results['missing_from_db'] = list(missing_ids)
        audit_results['extra_in_db'] = list(extra_ids)
        audit_results['summary']['missing_count'] = len(missing_ids)
        audit_results['summary']['extra_count'] = len(extra_ids)

        logger.info(f"Audit complete: {len(missing_ids)} missing, {len(extra_ids)} extra")

        return audit_results

    def audit_specific_page(self, page: int, batch_size: int = 1000) -> Dict[str, Any]:
        """Detailed audit of a specific page"""
        logger.info(f"Detailed audit of page {page}")

        try:
            # Fetch from API
            response = self.cratejoy_client.get_customers(limit=batch_size, page=page)
            api_customers = response.get('results', [])

            # Get details for each customer
            page_audit = {
                'page': page,
                'api_count': len(api_customers),
                'db_count': 0,
                'missing_customers': [],
                'present_customers': [],
                'data_issues': []
            }

            session = self.db_manager.get_session()
            try:
                for customer in api_customers:
                    customer_id = customer.get('id')
                    if not customer_id:
                        page_audit['data_issues'].append({
                            'issue': 'missing_customer_id',
                            'customer_data': customer
                        })
                        continue

                    # Check if exists in DB
                    db_result = session.execute(
                        text("SELECT raw_data FROM cratejoy_customers WHERE cratejoy_id = :cid"),
                        {"cid": customer_id}
                    ).first()

                    if db_result:
                        page_audit['present_customers'].append(customer_id)
                        page_audit['db_count'] += 1

                        # Compare data (optional detailed check)
                        try:
                            db_data = json.loads(db_result[0])
                            if db_data != customer:
                                page_audit['data_issues'].append({
                                    'issue': 'data_mismatch',
                                    'customer_id': customer_id,
                                    'api_email': customer.get('email'),
                                    'db_email': db_data.get('email')
                                })
                        except json.JSONDecodeError:
                            page_audit['data_issues'].append({
                                'issue': 'invalid_json_in_db',
                                'customer_id': customer_id
                            })
                    else:
                        page_audit['missing_customers'].append({
                            'customer_id': customer_id,
                            'email': customer.get('email', 'N/A'),
                            'has_id': customer_id is not None,
                            'data_size': len(json.dumps(customer))
                        })
            finally:
                session.close()

            return page_audit

        except Exception as e:
            logger.error(f"Failed to audit page {page}: {e}")
            return {
                'page': page,
                'error': str(e)
            }

    def find_customer_id_gaps(self, start_id: int, end_id: int) -> List[int]:
        """Find missing customer IDs in a sequential range"""
        session = self.db_manager.get_session()
        try:
            result = session.execute(
                text("SELECT cratejoy_id FROM cratejoy_customers WHERE cratejoy_id BETWEEN :start AND :end ORDER BY cratejoy_id"),
                {"start": start_id, "end": end_id}
            ).fetchall()

            existing_ids = set(row[0] for row in result)
            expected_ids = set(range(start_id, end_id + 1))
            missing_ids = expected_ids - existing_ids

            return sorted(list(missing_ids))
        finally:
            session.close()

    def get_database_stats_by_page_range(self, pages_per_chunk: int = 10) -> List[Dict[str, Any]]:
        """Get database record counts grouped by page ranges"""
        session = self.db_manager.get_session()
        try:
            # Get all customer IDs and estimate their page numbers
            result = session.execute(
                text("SELECT cratejoy_id FROM cratejoy_customers ORDER BY cratejoy_id")
            ).fetchall()

            customer_ids = [row[0] for row in result]

            # Group by estimated page ranges (assuming 1000 records per page)
            page_stats = []
            for i in range(0, len(customer_ids), pages_per_chunk * 1000):
                chunk_ids = customer_ids[i:i + pages_per_chunk * 1000]
                if chunk_ids:
                    start_page = i // 1000
                    end_page = start_page + pages_per_chunk - 1
                    page_stats.append({
                        'page_range': f"{start_page}-{end_page}",
                        'record_count': len(chunk_ids),
                        'first_id': chunk_ids[0],
                        'last_id': chunk_ids[-1],
                        'expected_count': pages_per_chunk * 1000
                    })

            return page_stats
        finally:
            session.close()

    def _get_db_customer_ids_in_range(self, expected_ids: Set[int]) -> Set[int]:
        """Get customer IDs from database that match expected range"""
        if not expected_ids:
            return set()

        session = self.db_manager.get_session()
        try:
            # Convert to list for SQL IN clause
            id_list = list(expected_ids)

            # SQLite has a limit on IN clause size, so batch it
            batch_size = 999  # SQLite limit is 1000
            all_db_ids = set()

            for i in range(0, len(id_list), batch_size):
                batch = id_list[i:i + batch_size]
                placeholders = ','.join(['?' for _ in batch])

                result = session.execute(
                    text(f"SELECT cratejoy_id FROM cratejoy_customers WHERE cratejoy_id IN ({placeholders})"),
                    batch
                ).fetchall()

                batch_ids = set(row[0] for row in result)
                all_db_ids.update(batch_ids)

            return all_db_ids
        finally:
            session.close()

    def export_audit_report(self, audit_results: Dict[str, Any], filename: str = None) -> str:
        """Export audit results to JSON file"""
        if not filename:
            from datetime import datetime
            filename = f"audit_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(filename, 'w') as f:
            json.dump(audit_results, f, indent=2, default=str)

        logger.info(f"Audit report exported to {filename}")
        return filename