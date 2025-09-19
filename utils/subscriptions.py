"""
Subscriptions Collection Module
Handles fetching subscriptions from Cratejoy API and saving to local database
"""
import json
import time
import logging
from typing import Dict, List, Optional, Any, Callable
from sqlalchemy import text
from utils.database import DatabaseManager
from utils.cratejoy_client import CratejoyClient

logger = logging.getLogger(__name__)


class SubscriptionCollector:
    """Handles subscription data collection from Cratejoy"""

    def __init__(self,
                 cratejoy_client: CratejoyClient,
                 database_url: Optional[str] = None):
        self.cratejoy_client = cratejoy_client
        self.db_manager = DatabaseManager(database_url)
        self.is_running = False

    def collect_subscriptions(
            self,
            batch_size: int = 1000,
            start_page: int = 0,
            progress_callback: Optional[Callable] = None,
            stop_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """
        Collect subscriptions from Cratejoy API with live progress updates

        Args:
            batch_size: Number of subscriptions per API call
            start_page: Page number to start from
            progress_callback: Function to call with progress updates
            stop_callback: Function to check if collection should stop

        Returns:
            Dict with collection statistics
        """
        logger.info(
            f"Starting subscription collection from page {start_page} with batch size {batch_size}"
        )

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
                        'status':
                        f'Fetching page {current_page} from Cratejoy API...',
                        'current_page': current_page,
                        'collected': collected,
                        'failed': failed
                    })

                # Fetch data from Cratejoy API
                try:
                    response = self.cratejoy_client.get_subscriptions(
                        limit=batch_size, page=current_page)
                    subscriptions = response.get('results', [])

                    if not subscriptions:
                        logger.info(
                            "No more subscriptions available - collection complete"
                        )
                        break

                    logger.info(
                        f"Processing {len(subscriptions)} subscriptions from page {current_page}"
                    )

                    # Process batch
                    batch_collected, batch_failed = self._process_subscription_batch(
                        subscriptions, progress_callback)
                    collected += batch_collected
                    failed += batch_failed

                    # Handle pagination
                    next_page_info = response.get('next')
                    if not next_page_info:
                        logger.info(
                            "No more pages available - collection complete")
                        break

                    # Parse next page number
                    current_page = self._parse_next_page(
                        next_page_info, current_page)

                    # API-friendly delay
                    time.sleep(0.2)

                except Exception as e:
                    logger.error(f"API error on page {current_page}: {e}")
                    failed += batch_size  # Assume all failed
                    current_page += 1  # Try next page

                    if current_page - start_page > 10:  # Don't retry too many times
                        break

            logger.info(
                f"Subscription collection completed: {collected} collected, {failed} failed"
            )
            return {
                'subscriptions_collected': collected,
                'subscriptions_failed': failed,
                'final_page': current_page
            }

        except Exception as e:
            logger.error(f"Subscription collection failed: {e}")
            raise
        finally:
            self.is_running = False

    def _process_subscription_batch(
            self,
            subscriptions: List[Dict],
            progress_callback: Optional[Callable] = None) -> tuple[int, int]:
        """Process a batch of subscriptions and save to database"""
        collected = 0
        failed = 0

        session = self.db_manager.get_session()
        try:
            for subscription in subscriptions:
                try:
                    subscription_id = subscription.get('id')

                    if not subscription_id:
                        failed += 1
                        continue

                    # Fast UPSERT - insert or update in single query
                    session.execute(
                        text("""
                            INSERT INTO cratejoy_subscriptions (cratejoy_id, raw_data, fetched_at) 
                            VALUES (:sid, :data, NOW())
                            ON CONFLICT (cratejoy_id) 
                            DO UPDATE SET 
                                raw_data = EXCLUDED.raw_data,
                                fetched_at = EXCLUDED.fetched_at
                        """), {
                            "sid": subscription_id,
                            "data": json.dumps(subscription)
                        })

                    collected += 1

                    # Progress update every 100 subscriptions
                    if collected % 100 == 0 and progress_callback:
                        progress_callback({
                            'status':
                            f'Processed {collected} subscriptions in current batch',
                            'batch_collected':
                            collected,
                            'batch_failed':
                            failed,
                            'last_subscription_id':
                            subscription_id
                        })

                except Exception as e:
                    logger.error(
                        f"Failed to save subscription {subscription.get('id', 'unknown')}: {e}"
                    )
                    failed += 1

            # Commit the entire batch
            session.commit()
            logger.info(
                f"Batch processed: {collected} subscriptions saved, {failed} failed"
            )

        except Exception as e:
            session.rollback()
            logger.error(f"Batch processing failed: {e}")
            failed += len(subscriptions)
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
        logger.info("Subscription collection stop requested")

    def get_subscription_count(self) -> int:
        """Get total number of subscriptions in database"""
        session = self.db_manager.get_session()
        try:
            count = session.execute(
                text("SELECT COUNT(*) FROM cratejoy_subscriptions")).scalar()
            return count or 0
        finally:
            session.close()

    def get_subscriptions_by_customer(self, cratejoy_customer_id: int) -> List[Dict[str, Any]]:
        """Get all subscriptions for a specific customer"""
        session = self.db_manager.get_session()
        try:
            result = session.execute(
                text("SELECT raw_data FROM cratejoy_subscriptions WHERE cratejoy_customer_id = :cid"),
                {"cid": cratejoy_customer_id}
            ).fetchall()

            subscriptions = []
            for row in result:
                try:
                    subscription_data = json.loads(row[0])
                    subscriptions.append(subscription_data)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse subscription data: {e}")

            return subscriptions
        finally:
            session.close()

    def clear_all_subscriptions(self):
        """Delete all subscription data from database"""
        session = self.db_manager.get_session()
        try:
            session.execute(text("DELETE FROM cratejoy_subscriptions"))
            session.commit()
            logger.info("All subscription data deleted")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to delete subscription data: {e}")
            raise
        finally:
            session.close()
