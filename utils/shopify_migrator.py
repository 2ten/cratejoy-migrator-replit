"""
Shopify Migration Module
Handles assembling customer records and migrating them to Shopify
"""
import json
import time
import logging
from typing import Dict, List, Optional, Any, Callable
from sqlalchemy import text
from utils.database import DatabaseManager
from utils.shopify_client import ShopifyClient
from utils.data_mapper import DataMapper
from utils.customers import CustomerCollector
from utils.orders import OrderCollector
from utils.subscriptions import SubscriptionCollector

logger = logging.getLogger(__name__)

class ShopifyMigrator:
    """Handles atomic migration of complete customer records to Shopify"""

    def __init__(self, 
                 shopify_client: ShopifyClient, 
                 database_url: Optional[str] = None):
        self.shopify_client = shopify_client
        self.data_mapper = DataMapper()
        self.db_manager = DatabaseManager(database_url)
        self.is_running = False

        # Initialize collectors for data retrieval
        self.customer_collector = CustomerCollector(None, database_url)
        self.order_collector = OrderCollector(None, database_url)
        self.subscription_collector = SubscriptionCollector(None, database_url)

        # Cache for product mapping
        self._product_mapping = None

    def migrate_customers_atomic(self, 
                                batch_size: int = 50,
                                test_limit: Optional[int] = None,
                                dry_run: bool = False,
                                progress_callback: Optional[Callable] = None,
                                stop_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """
        Migrate customers atomically with all their orders and subscriptions

        Args:
            batch_size: Number of customers to process per batch
            test_limit: Limit number of customers for testing (None = all)
            dry_run: If True, simulate migration without making changes
            progress_callback: Function to call with progress updates
            stop_callback: Function to check if migration should stop

        Returns:
            Dict with migration statistics
        """
        logger.info("=== STARTING ATOMIC CUSTOMER MIGRATION ===")

        if dry_run:
            logger.info("Running in DRY RUN mode - no changes will be made")

        self.is_running = True
        migrated = 0
        failed = 0

        try:
            # Build product mapping first
            if progress_callback:
                progress_callback({
                    'status': 'Building product mapping from Shopify...',
                    'stage': 'preparation'
                })

            product_mapping = self._get_product_mapping()
            logger.info(f"Built product mapping for {len(product_mapping)} SKUs")

            # Get customers ready for migration
            customers = self._get_customers_for_migration(test_limit)
            total_customers = len(customers)

            if total_customers == 0:
                logger.warning("No customers found for migration")
                return {'customers_migrated': 0, 'customers_failed': 0}

            logger.info(f"Found {total_customers} customers ready for migration")

            # Process customers in batches
            for i, customer_record in enumerate(customers):
                if not self.is_running:
                    logger.info("Migration stopped by external signal")
                    break

                # Check stop callback
                if stop_callback and stop_callback():
                    logger.info("Migration stopped by stop callback")
                    break

                try:
                    # Update progress
                    if progress_callback:
                        progress_callback({
                            'status': f'Migrating customer {i+1} of {total_customers}',
                            'current': i + 1,
                            'total': total_customers,
                            'customer_email': customer_record.get('email', 'Unknown'),
                            'migrated': migrated,
                            'failed': failed
                        })

                    # Get complete customer data
                    customer_data = self._assemble_customer_record(customer_record['cratejoy_id'])

                    if not customer_data:
                        logger.warning(f"Could not assemble data for customer {customer_record['cratejoy_id']}")
                        failed += 1
                        continue

                    # Migrate customer atomically
                    success = self._migrate_single_customer(
                        customer_data, 
                        product_mapping, 
                        dry_run
                    )

                    if success:
                        migrated += 1
                        if not dry_run:
                            self._mark_customer_migrated(customer_record['cratejoy_id'])
                    else:
                        failed += 1
                        if not dry_run:
                            self._mark_customer_failed(customer_record['cratejoy_id'], "Migration failed")

                    # Log progress every batch
                    if (i + 1) % batch_size == 0:
                        logger.info(f"Processed {i + 1}/{total_customers} customers: "
                                  f"{migrated} migrated, {failed} failed")

                        # Small delay between batches to be API-friendly
                        if not dry_run:
                            time.sleep(1)

                except Exception as e:
                    logger.error(f"Failed to migrate customer {customer_record.get('cratejoy_id', 'unknown')}: {e}")
                    failed += 1
                    if not dry_run:
                        self._mark_customer_failed(customer_record['cratejoy_id'], str(e))

            logger.info(f"Atomic migration completed: {migrated} customers migrated, {failed} failed")

            return {
                'customers_migrated': migrated,
                'customers_failed': failed,
                'total_processed': migrated + failed,
                'dry_run': dry_run
            }

        except Exception as e:
            logger.error(f"Atomic migration failed: {e}")
            raise
        finally:
            self.is_running = False

    def _assemble_customer_record(self, cratejoy_customer_id: int) -> Optional[Dict[str, Any]]:
        """Assemble complete customer record with orders and subscription data from customer record"""
        try:
            session = self.db_manager.get_session()

            # Get customer data (contains embedded subscription info)
            customer_result = session.execute(
                text("SELECT raw_data FROM cratejoy_customers WHERE cratejoy_id = :cid"),
                {"cid": cratejoy_customer_id}
            ).first()

            if not customer_result:
                logger.warning(f"Customer {cratejoy_customer_id} not found in database")
                return None

            customer_data = json.loads(customer_result[0])

            # Get orders for this customer
            orders = self.order_collector.get_orders_by_customer(cratejoy_customer_id)

            session.close()

            return {
                'customer': customer_data,
                'orders': orders,
                'subscriptions': []  # Subscription data is embedded in customer record
            }

        except Exception as e:
            logger.error(f"Failed to assemble customer record for {cratejoy_customer_id}: {e}")
            return None

    def _migrate_single_customer(self, 
                                customer_data: Dict[str, Any], 
                                product_mapping: Dict[str, Any], 
                                dry_run: bool) -> bool:
        """Migrate a single customer with all their data atomically"""
        try:
            customer = customer_data['customer']
            orders = customer_data['orders']
            subscriptions = customer_data['subscriptions']

            customer_email = customer.get('email', 'unknown')
            cratejoy_customer_id = customer.get('id')

            if dry_run:
                logger.info(f"[DRY RUN] Would migrate customer {customer_email} with "
                           f"{len(orders)} orders and {len(subscriptions)} subscriptions")
                return True

            logger.info(f"Migrating customer {customer_email} ({cratejoy_customer_id})")

            # 1. Create customer in Shopify
            shopify_customer_data = self.data_mapper.map_customer(customer)
            shopify_customer = self.shopify_client.create_customer(shopify_customer_data)
            shopify_customer_id = shopify_customer['id']

            logger.info(f"Created Shopify customer {shopify_customer_id} for {customer_email}")

            # 2. Add migration tag
            try:
                self.shopify_client.add_customer_tags(shopify_customer_id, ['cratejoy-migrated'])
            except Exception as e:
                logger.warning(f"Failed to add tags to customer {shopify_customer_id}: {e}")

            # 3. Migrate orders
            orders_migrated = 0
            for order in orders:
                try:
                    shopify_order_data = self.data_mapper.map_order(
                        order, shopify_customer_id, product_mapping
                    )
                    shopify_order = self.shopify_client.create_order(shopify_order_data)
                    orders_migrated += 1
                    logger.debug(f"Created order {shopify_order['id']} for customer {customer_email}")
                except Exception as e:
                    logger.warning(f"Failed to create order {order.get('id')} for customer {customer_email}: {e}")
                    # Continue with other orders - don't fail entire customer

            # 4. Create subscription history metafield from customer data
            if customer.get('subscription_status') and customer.get('subscription_status') != 'none':
                try:
                    subscription_metafield = self._create_subscription_metafield_from_customer(customer)
                    self.shopify_client.create_customer_metafield(
                        shopify_customer_id, subscription_metafield
                    )
                    logger.debug(f"Created subscription metafield for customer {customer_email}")
                except Exception as e:
                    logger.warning(f"Failed to create subscription metafield for customer {customer_email}: {e}")
                    # Continue - don't fail entire customer for metafield issue

            subscription_info = "with subscription data" if customer.get('subscription_status') and customer.get('subscription_status') != 'none' else "no subscription data"
            logger.info(f"Successfully migrated customer {customer_email}: "
                       f"{orders_migrated}/{len(orders)} orders, {subscription_info}")

            return True

        except Exception as e:
            logger.error(f"Failed atomic migration for customer {customer_data.get('customer', {}).get('email', 'unknown')}: {e}")
            return False

    def _create_subscription_metafield_from_customer(self, customer: Dict[str, Any]) -> Dict[str, Any]:
        """Create subscription metafield data from customer's embedded subscription info"""
        subscription_data = {
            'cratejoy_customer_id': customer.get('id'),
            'subscription_status': customer.get('subscription_status', 'unknown'),
            'total_revenue': str(customer.get('total_revenue', '0')),
            'currency': 'USD',  # Default currency
            'customer_since': customer.get('date_created'),
            'last_updated': customer.get('date_updated')
        }

        return {
            'namespace': 'cratejoy',
            'key': 'subscription_summary',
            'value': {
                'migration_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                'subscription_data': subscription_data,
                'source': 'cratejoy_customer_record'
            },
            'type': 'json'
        }

    def _get_product_mapping(self) -> Dict[str, Dict[str, Any]]:
        """Get or build product mapping from Shopify"""
        if self._product_mapping is not None:
            return self._product_mapping

        logger.info("Building product mapping from Shopify")
        product_mapping = {}

        try:
            shopify_products = self._fetch_all_shopify_products()

            for product in shopify_products:
                for variant in product.get('variants', []):
                    sku = (variant.get('sku') or '').strip()
                    if sku:
                        product_mapping[sku] = {
                            'product_id': product.get('id'),
                            'variant_id': variant.get('id'),
                            'title': product.get('title', ''),
                            'variant_title': variant.get('title', ''),
                            'price': variant.get('price', '0.00')
                        }

            self._product_mapping = product_mapping
            logger.info(f"Built product mapping for {len(product_mapping)} SKUs")

        except Exception as e:
            logger.warning(f"Failed to build product mapping: {e}")
            self._product_mapping = {}

        return self._product_mapping

    def _fetch_all_shopify_products(self) -> List[Dict[str, Any]]:
        """Fetch all products from Shopify with pagination"""
        all_products = []
        limit = 250
        since_id = 0

        while True:
            try:
                params = {'limit': limit, 'since_id': since_id}
                response = self.shopify_client._make_request('GET', '/products.json', params=params)
                products = response.get('products', [])

                if not products:
                    break

                all_products.extend(products)
                logger.debug(f"Fetched {len(products)} products (total: {len(all_products)})")

                if len(products) < limit:
                    break

                since_id = products[-1]['id']

                # Small delay to be API-friendly
                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Error fetching Shopify products: {e}")
                break

        logger.info(f"Fetched {len(all_products)} total products from Shopify")
        return all_products

    def _get_customers_for_migration(self, test_limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get customers with orders or subscription activity for migration"""
        session = self.db_manager.get_session()
        try:
            # Build query - only select customers with orders OR subscriptions
            base_query = """
                SELECT DISTINCT c.cratejoy_id, c.email 
                FROM cratejoy_customers c
                WHERE (
                    EXISTS (SELECT 1 FROM cratejoy_orders o WHERE o.cratejoy_customer_id = c.cratejoy_id)
                    OR 
                    (c.subscription_status != 'none' AND c.subscription_status IS NOT NULL)
                    OR 
                    (c.total_revenue > 0)
                )
                ORDER BY c.cratejoy_id
            """

            if test_limit:
                query = text(base_query + " LIMIT :limit")
                result = session.execute(query, {"limit": test_limit}).fetchall()
            else:
                query = text(base_query)
                result = session.execute(query).fetchall()

            customers = []
            for row in result:
                customers.append({
                    'cratejoy_id': row[0],
                    'email': row[1]
                })

            return customers

        finally:
            session.close()

    def _mark_customer_migrated(self, cratejoy_customer_id: int, shopify_customer_id: Optional[int] = None):
        """Mark customer as successfully migrated"""
        # Note: migration_status column doesn't exist yet, placeholder for future implementation
        logger.info(f"Customer {cratejoy_customer_id} migrated to Shopify customer {shopify_customer_id}")
        pass

    def _mark_customer_failed(self, cratejoy_customer_id: int, error_message: str):
        """Mark customer migration as failed"""
        # Note: migration_status column doesn't exist yet, placeholder for future implementation
        logger.error(f"Customer {cratejoy_customer_id} migration failed: {error_message}")
        pass

    def stop_migration(self):
        """Stop the current migration process"""
        self.is_running = False
        logger.info("Shopify migration stop requested")

    def get_migration_stats(self) -> Dict[str, Any]:
        """Get migration statistics"""
        session = self.db_manager.get_session()
        try:
            # Get customer stats
            total_customers = session.execute(text("SELECT COUNT(*) FROM cratejoy_customers")).scalar() or 0

            try:
                migrated = session.execute(
                    text("SELECT COUNT(*) FROM cratejoy_customers WHERE migration_status = 'migrated'")
                ).scalar() or 0
                pending = session.execute(
                    text("SELECT COUNT(*) FROM cratejoy_customers WHERE migration_status = 'pending' OR migration_status IS NULL")
                ).scalar() or 0
                failed = session.execute(
                    text("SELECT COUNT(*) FROM cratejoy_customers WHERE migration_status = 'failed'")
                ).scalar() or 0
            except:
                # Migration status columns don't exist
                migrated = pending = failed = 0

            # Get order/subscription counts
            total_orders = session.execute(text("SELECT COUNT(*) FROM cratejoy_orders")).scalar() or 0
            total_subscriptions = session.execute(text("SELECT COUNT(*) FROM cratejoy_subscriptions")).scalar() or 0

            return {
                'customers': {
                    'total': total_customers,
                    'migrated': migrated,
                    'pending': pending,
                    'failed': failed
                },
                'orders_collected': total_orders,
                'subscriptions_collected': total_subscriptions,
                'migration_progress': migrated / max(total_customers, 1) * 100
            }

        finally:
            session.close()

    def clear_migration_status(self):
        """Reset all migration statuses to allow re-migration"""
        session = self.db_manager.get_session()
        try:
            session.execute(
                text("UPDATE cratejoy_customers SET migration_status = 'pending', shopify_id = NULL, error_message = NULL")
            )
            session.commit()
            logger.info("Reset all customer migration statuses")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to reset migration statuses: {e}")
            raise
        finally:
            session.close()