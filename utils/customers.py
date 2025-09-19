"""
Customer Collection Module
Handles fetching customers from Cratejoy API and saving to local database
"""
import json
import time
import logging
from typing import Dict, List, Optional, Any, Callable
from sqlalchemy import text
from utils.database import DatabaseManager
from utils.cratejoy_client import CratejoyClient

logger = logging.getLogger(__name__)

class CustomerCollector:
    """Handles customer data collection from Cratejoy"""

    def __init__(self, cratejoy_client: CratejoyClient, database_url: Optional[str] = None):
        self.cratejoy_client = cratejoy_client
        self.db_manager = DatabaseManager(database_url)
        self.is_running = False

    def collect_customers(self, 
                         batch_size: int = 1000, 
                         start_page: int = 0,
                         progress_callback: Optional[Callable] = None,
                         stop_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """
        Collect customers from Cratejoy API with live progress updates

        Args:
            batch_size: Number of customers per API call
            start_page: Page number to start from
            progress_callback: Function to call with progress updates
            stop_callback: Function to check if collection should stop

        Returns:
            Dict with collection statistics
        """
        logger.info(f"Starting customer collection from page {start_page} with batch size {batch_size}")

        self.is_running = True
        collected = 0
        failed = 0
        current_page = start_page

        try:
            while self.is_running:
                # Check if we should stop
                if stop_callback and stop_callback():
                    logger.info("Collection stopped by external signal")
                    break

                # Update progress
                if progress_callback:
                    progress_callback({
                        'status': f'Fetching page {current_page} from Cratejoy API...',
                        'current_page': current_page,
                        'collected': collected,
                        'failed': failed
                    })

                # Fetch data from Cratejoy API
                try:
                    response = self.cratejoy_client.get_customers(
                        limit=batch_size, 
                        page=current_page
                    )
                    customers = response.get('results', [])

                    if not customers:
                        logger.info("No more customers available - collection complete")
                        break

                    logger.info(f"Processing {len(customers)} customers from page {current_page}")

                    # Process batch
                    batch_collected, batch_failed = self._process_customer_batch(customers, progress_callback)
                    collected += batch_collected
                    failed += batch_failed

                    # Handle pagination
                    next_page_info = response.get('next')
                    if not next_page_info:
                        logger.info("No more pages available - collection complete")
                        break

                    # Parse next page number
                    current_page = self._parse_next_page(next_page_info, current_page)

                    # API-friendly delay
                    time.sleep(0.2)

                except Exception as e:
                    logger.error(f"API error on page {current_page}: {e}")
                    failed += batch_size  # Assume all failed
                    current_page += 1  # Try next page

                    if current_page - start_page > 10:  # Don't retry too many times
                        break

            logger.info(f"Customer collection completed: {collected} collected, {failed} failed")
            return {
                'customers_collected': collected,
                'customers_failed': failed,
                'final_page': current_page
            }

        except Exception as e:
            logger.error(f"Customer collection failed: {e}")
            raise
        finally:
            self.is_running = False

    def _process_customer_batch(self, customers: List[Dict], progress_callback: Optional[Callable] = None) -> tuple[int, int]:
        """Process a batch of customers and save to database"""
        collected = 0
        failed = 0

        session = self.db_manager.get_session()
        try:
            for customer in customers:
                try:
                    customer_id = customer.get('id')
                    if not customer_id:
                        failed += 1
                        continue

                    # Fast UPSERT - insert or update in single query
                    session.execute(
                        text("""
                            INSERT INTO cratejoy_customers (cratejoy_id, email, raw_data, fetched_at) 
                            VALUES (:cid, :email, :data, NOW())
                            ON CONFLICT (cratejoy_id) 
                            DO UPDATE SET 
                                raw_data = EXCLUDED.raw_data,
                                email = EXCLUDED.email,
                                fetched_at = EXCLUDED.fetched_at
                        """),
                        {"cid": customer_id, "email": customer.get('email', ''), "data": json.dumps(customer)}
                    )

                    collected += 1

                    # Progress update every 100 customers
                    if collected % 100 == 0 and progress_callback:
                        progress_callback({
                            'status': f'Processed {collected} customers in current batch',
                            'batch_collected': collected,
                            'batch_failed': failed,
                            'last_customer_id': customer_id
                        })

                except Exception as e:
                    logger.error(f"Failed to save customer {customer.get('id', 'unknown')}: {e}")
                    failed += 1

            # Commit the entire batch
            session.commit()
            logger.info(f"Batch processed: {collected} customers saved, {failed} failed")

        except Exception as e:
            session.rollback()
            logger.error(f"Batch processing failed: {e}")
            failed += len(customers)
            raise
        finally:
            session.close()

        return collected, failed

    def _parse_next_page(self, next_page_info: str, current_page: int) -> int:
        """Parse next page number from API response"""
        if 'page=' in next_page_info:
            try:
                next_page = int(next_page_info.split('page=')[1].split('&')[0])
                logger.debug(f"Parsed next page: {next_page}")
                return next_page
            except (ValueError, IndexError):
                logger.warning(f"Failed to parse page from: {next_page_info}")

        # Fallback to incrementing
        return current_page + 1

    def stop_collection(self):
        """Stop the current collection process"""
        self.is_running = False
        logger.info("Customer collection stop requested")

    def get_customer_count(self) -> int:
        """Get total number of customers in database"""
        session = self.db_manager.get_session()
        try:
            count = session.execute(text("SELECT COUNT(*) FROM cratejoy_customers")).scalar()
            return count or 0
        finally:
            session.close()

    def get_customer_stats(self) -> Dict[str, Any]:
        """Get customer collection statistics"""
        session = self.db_manager.get_session()
        try:
            total = session.execute(text("SELECT COUNT(*) FROM cratejoy_customers")).scalar() or 0

            # Get migration status counts if columns exist
            try:
                migrated = session.execute(
                    text("SELECT COUNT(*) FROM cratejoy_customers WHERE migration_status = 'migrated'")
                ).scalar() or 0
                pending = session.execute(
                    text("SELECT COUNT(*) FROM cratejoy_customers WHERE migration_status = 'pending'")
                ).scalar() or 0
                failed = session.execute(
                    text("SELECT COUNT(*) FROM cratejoy_customers WHERE migration_status = 'failed'")
                ).scalar() or 0
            except:
                # Migration status columns don't exist
                migrated = pending = failed = 0

            return {
                'total_customers': total,
                'migrated': migrated,
                'pending': pending,
                'failed': failed
            }
        finally:
            session.close()

    def clear_all_customers(self):
        """Delete all customer data from database"""
        session = self.db_manager.get_session()
        try:
            session.execute(text("DELETE FROM cratejoy_customers"))
            session.commit()
            logger.info("All customer data deleted")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to delete customer data: {e}")
            raise
        finally:
            session.close()