"""
Orders Collection Module
Handles fetching orders from Cratejoy API and saving to local database
"""
import json
import time
import logging
from typing import Dict, List, Optional, Any, Callable
from sqlalchemy import text
from utils.database import DatabaseManager
from utils.cratejoy_client import CratejoyClient

logger = logging.getLogger(__name__)

class OrderCollector:
    """Handles order data collection from Cratejoy"""

    def __init__(self, cratejoy_client: CratejoyClient, database_url: Optional[str] = None):
        self.cratejoy_client = cratejoy_client
        self.db_manager = DatabaseManager(database_url)
        self.is_running = False

    def collect_orders(self, 
                      batch_size: int = 1000, 
                      start_page: int = 0,
                      progress_callback: Optional[Callable] = None,
                      stop_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """
        Collect orders from Cratejoy API with live progress updates

        Args:
            batch_size: Number of orders per API call
            start_page: Page number to start from
            progress_callback: Function to call with progress updates
            stop_callback: Function to check if collection should stop

        Returns:
            Dict with collection statistics
        """
        logger.info(f"Starting order collection from page {start_page} with batch size {batch_size}")

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

                # Fetch data from Cratejoy API using page-based pagination
                try:
                    response = self.cratejoy_client.get_orders(
                        limit=batch_size, 
                        page=current_page
                    )
                    orders = response.get('results', [])

                    if not orders:
                        logger.info("No more orders available - collection complete")
                        break

                    logger.info(f"Processing {len(orders)} orders from page {current_page}")

                    # Process batch
                    batch_collected, batch_failed = self._process_order_batch(orders, progress_callback)
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

            logger.info(f"Order collection completed: {collected} collected, {failed} failed")
            return {
                'orders_collected': collected,
                'orders_failed': failed,
                'final_page': current_page
            }

        except Exception as e:
            logger.error(f"Order collection failed: {e}")
            raise
        finally:
            self.is_running = False

    def _process_order_batch(self, orders: List[Dict], progress_callback: Optional[Callable] = None) -> tuple[int, int]:
        """Process a batch of orders and save to database"""
        collected = 0
        failed = 0

        session = self.db_manager.get_session()
        try:
            for order in orders:
                try:
                    order_id = order.get('id')
                    customer_id = order.get('customer_id')

                    if not order_id:
                        failed += 1
                        continue

                    # Fast UPSERT - insert or update in single query
                    session.execute(
                        text("""
                            INSERT INTO cratejoy_orders (cratejoy_id, cratejoy_customer_id, raw_data, fetched_at) 
                            VALUES (:oid, :cid, :data, NOW())
                            ON CONFLICT (cratejoy_id) 
                            DO UPDATE SET 
                                raw_data = EXCLUDED.raw_data,
                                cratejoy_customer_id = EXCLUDED.cratejoy_customer_id,
                                fetched_at = EXCLUDED.fetched_at
                        """),
                        {"oid": order_id, "cid": customer_id, "data": json.dumps(order)}
                    )

                    collected += 1

                    # Progress update every 100 orders
                    if collected % 100 == 0 and progress_callback:
                        progress_callback({
                            'status': f'Processed {collected} orders in current batch',
                            'batch_collected': collected,
                            'batch_failed': failed,
                            'last_order_id': order_id
                        })

                except Exception as e:
                    logger.error(f"Failed to save order {order.get('id', 'unknown')}: {e}")
                    failed += 1
                    # Continue processing other orders in the batch

            # Commit the entire batch
            session.commit()
            logger.info(f"Batch processed: {collected} orders saved, {failed} failed")

        except Exception as e:
            # Properly handle transaction errors
            try:
                session.rollback()
                logger.error(f"Batch processing failed, rolled back: {e}")
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction: {rollback_error}")

            # Mark all records as failed if batch commit fails
            failed += len(orders) - collected
            raise
        finally:
            # Always close the session properly
            try:
                session.close()
            except Exception as close_error:
                logger.error(f"Failed to close session: {close_error}")

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
        logger.info("Order collection stop requested")

    def get_order_count(self) -> int:
        """Get total number of orders in database"""
        session = self.db_manager.get_session()
        try:
            count = session.execute(text("SELECT COUNT(*) FROM cratejoy_orders")).scalar()
            return count or 0
        finally:
            session.close()

    def get_orders_by_customer(self, cratejoy_customer_id: int) -> List[Dict[str, Any]]:
        """Get all orders for a specific customer"""
        session = self.db_manager.get_session()
        try:
            result = session.execute(
                text("SELECT raw_data FROM cratejoy_orders WHERE cratejoy_customer_id = :cid"),
                {"cid": cratejoy_customer_id}
            ).fetchall()

            orders = []
            for row in result:
                try:
                    order_data = json.loads(row[0])
                    orders.append(order_data)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse order data: {e}")

            return orders
        finally:
            session.close()

    def get_order_stats(self) -> Dict[str, Any]:
        """Get order collection statistics"""
        session = self.db_manager.get_session()
        try:
            total = session.execute(text("SELECT COUNT(*) FROM cratejoy_orders")).scalar() or 0

            # Get unique customer count from orders
            unique_customers = session.execute(
                text("SELECT COUNT(DISTINCT cratejoy_customer_id) FROM cratejoy_orders")
            ).scalar() or 0

            # Get migration status counts if columns exist
            try:
                migrated = session.execute(
                    text("SELECT COUNT(*) FROM cratejoy_orders WHERE migration_status = 'migrated'")
                ).scalar() or 0
                pending = session.execute(
                    text("SELECT COUNT(*) FROM cratejoy_orders WHERE migration_status = 'pending'")
                ).scalar() or 0
                failed = session.execute(
                    text("SELECT COUNT(*) FROM cratejoy_orders WHERE migration_status = 'failed'")
                ).scalar() or 0
            except:
                # Migration status columns don't exist
                migrated = pending = failed = 0

            return {
                'total_orders': total,
                'unique_customers': unique_customers,
                'migrated': migrated,
                'pending': pending,
                'failed': failed
            }
        finally:
            session.close()

    def clear_all_orders(self):
        """Delete all order data from database"""
        session = self.db_manager.get_session()
        try:
            session.execute(text("DELETE FROM cratejoy_orders"))
            session.commit()
            logger.info("All order data deleted")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to delete order data: {e}")
            raise
        finally:
            session.close()