"""
Cratejoy to Shopify Migration Tool - Modular Interface
Streamlit interface that uses specialized modules for each data type
"""
import streamlit as st
import os
import time
import logging
from datetime import datetime
from sqlalchemy import text

# Import our modular components
from utils.cratejoy_client import CratejoyClient
from utils.shopify_client import ShopifyClient
from utils.database import DatabaseManager
from utils.logger import setup_logger
from utils.customers import CustomerCollector
from utils.orders import OrderCollector
from utils.subscriptions import SubscriptionCollector
from utils.audit_tool import DatabaseAuditor
from utils.shopify_migrator import ShopifyMigrator


# Configure page
st.set_page_config(
    page_title="Cratejoy to Shopify Migration Tool",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
def init_session_state():
    """Initialize all session state variables"""
    defaults = {
        'password_correct': None,
        'collection_active': False,
        'collection_type': None,
        'collection_start_time': None,
        'migration_active': False,
        'confirm_delete': False,
        'collectors': {}
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

def check_password():
    """Authentication check"""
    def password_entered():
        username = st.session_state.get("username", "")
        password = st.session_state.get("password", "")

        if username and password:
            expected_username = st.secrets.get("auth", {}).get("username", "admin")
            expected_password = st.secrets.get("auth", {}).get("password", "migration2024")

            if username == expected_username and password == expected_password:
                st.session_state["password_correct"] = True
                for key in ["password", "username"]:
                    if key in st.session_state:
                        del st.session_state[key]
            else:
                st.session_state["password_correct"] = False

    if st.session_state.get("password_correct"):
        return True

    st.markdown("### 🔐 Authentication Required")
    col1, col2 = st.columns(2)

    with col1:
        st.text_input("Username", on_change=password_entered, key="username")
    with col2:
        st.text_input("Password", type="password", on_change=password_entered, key="password")

    if st.session_state.get("password_correct") is False:
        st.error("❌ Username or password incorrect")
    else:
        st.info("Please enter your credentials to access the migration tool.")

    return False

@st.cache_data(ttl=30)  # Cache for 30 seconds
def get_database_stats():
    """Get database statistics with caching"""
    try:
        db_manager = DatabaseManager(os.getenv('DATABASE_URL'))
        session = db_manager.get_session()

        try:
            customer_count = session.execute(text("SELECT COUNT(*) FROM cratejoy_customers")).scalar() or 0
            order_count = session.execute(text("SELECT COUNT(*) FROM cratejoy_orders")).scalar() or 0
            subscription_count = session.execute(text("SELECT COUNT(*) FROM cratejoy_subscriptions")).scalar() or 0

            return {
                'customers': customer_count,
                'orders': order_count,
                'subscriptions': subscription_count,
                'total': customer_count + order_count + subscription_count
            }
        finally:
            session.close()
    except Exception as e:
        return {'customers': 0, 'orders': 0, 'subscriptions': 0, 'total': 0}

def get_api_credentials():
    """Get API credentials from sidebar"""
    with st.sidebar:
        st.header("⚙️ API Configuration")

        with st.expander("🔧 Cratejoy API", expanded=True):
            cratejoy_api_key = st.text_input(
                "API Key", 
                value=os.getenv("CRATEJOY_API_KEY", ""),
                type="password",
                key="cratejoy_key"
            )

        with st.expander("🛍️ Shopify API", expanded=True):
            shopify_api_key = st.text_input(
                "API Key",
                value=os.getenv("SHOPIFY_API_KEY", ""),
                type="password",
                key="shopify_key"
            )
            shopify_password = st.text_input(
                "Password",
                value=os.getenv("SHOPIFY_PASSWORD", ""),
                type="password",
                key="shopify_pass"
            )
            shopify_domain = st.text_input(
                "Store Domain",
                value=os.getenv("SHOPIFY_DOMAIN", ""),
                key="shopify_domain"
            )

        credentials_complete = all([cratejoy_api_key, shopify_api_key, shopify_password, shopify_domain])

        if credentials_complete:
            st.success("✅ All credentials provided")
        else:
            st.warning("⚠️ Please provide all API credentials")

        return {
            'cratejoy_api_key': cratejoy_api_key,
            'shopify_api_key': shopify_api_key,
            'shopify_password': shopify_password,
            'shopify_domain': shopify_domain,
            'complete': credentials_complete
        }

def initialize_clients(credentials):
    """Initialize API clients and collectors"""
    if not credentials['complete']:
        return None

    try:
        # Initialize clients
        cratejoy_client = CratejoyClient(
            credentials['cratejoy_api_key'], 
            "", 
            os.getenv("CRATEJOY_CLIENT_SECRET", "")
        )
        shopify_client = ShopifyClient(
            credentials['shopify_api_key'], 
            credentials['shopify_password'], 
            credentials['shopify_domain']
        )

        # Initialize collectors (including auditor)
        collectors = {
            'customers': CustomerCollector(cratejoy_client, os.getenv('DATABASE_URL')),
            'orders': OrderCollector(cratejoy_client, os.getenv('DATABASE_URL')),
            'subscriptions': SubscriptionCollector(cratejoy_client, os.getenv('DATABASE_URL')),
            'migrator': ShopifyMigrator(shopify_client, os.getenv('DATABASE_URL')),
            'auditor': DatabaseAuditor(cratejoy_client, os.getenv('DATABASE_URL'))  # Add auditor
        }

        return collectors

    except Exception as e:
        st.error(f"Failed to initialize clients: {e}")
        return None

def render_progress_dashboard():
    """Render the real-time progress dashboard"""
    st.subheader("📊 Real-Time Collection Progress")

    stats = get_database_stats()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Customers", f"{stats['customers']:,}")
        if stats['customers'] > 0:
            progress = min(stats['customers'] / 100000, 1.0)
            st.progress(progress)
            st.caption(f"{progress:.1%} to 100k milestone")

    with col2:
        st.metric("Orders", f"{stats['orders']:,}")

    with col3:
        st.metric("Subscriptions", f"{stats['subscriptions']:,}")

    with col4:
        st.metric("Total Records", f"{stats['total']:,}")

def render_collection_interface(collectors):
    """Render the data collection interface"""
    st.subheader("Phase 1: Bulk Data Collection")
    st.markdown("Collect all data from Cratejoy and store locally for atomic migration")

    if not collectors:
        st.warning("⚠️ Please provide valid API credentials to enable collection")
        return

    # Collection configuration
    col1, col2 = st.columns(2)

    with col1:
        batch_size = st.number_input(
            "Batch Size", 
            min_value=100, 
            max_value=1000, 
            value=1000, 
            step=100
        )
        start_page = st.number_input(
            "Start Page", 
            min_value=0, 
            max_value=10000, 
            value=0
        )

    with col2:
        collect_customers = st.checkbox("Collect Customers", value=True)
        collect_orders = st.checkbox("Collect Orders", value=False)
        collect_subscriptions = st.checkbox("Collect Subscriptions", value=False)

    # Collection controls
    render_collection_controls(collectors, batch_size, start_page, 
                             collect_customers, collect_orders, collect_subscriptions)

def render_collection_controls(collectors, batch_size, start_page, 
                             collect_customers, collect_orders, collect_subscriptions):
    """Render collection control buttons"""
    st.markdown("---")

    # Status display
    status_col1, status_col2, status_col3 = st.columns([3, 1, 1])

    with status_col1:
        if st.session_state.collection_active:
            elapsed = int(time.time() - st.session_state.collection_start_time) if st.session_state.collection_start_time else 0
            st.success(f"🟢 Collection Active: {st.session_state.collection_type}")
            st.caption(f"Running for {elapsed//60}m {elapsed%60}s")
        else:
            st.info("⚪ Collection Idle")

    with status_col2:
        if st.session_state.collection_active:
            if st.button("⏹️ Stop Collection", type="secondary"):
                stop_collection(collectors)

    with status_col3:
        if st.button("🔄 Refresh Stats"):
            st.cache_data.clear()
            st.rerun()

    # Collection buttons
    if not st.session_state.collection_active:
        col_btn1, col_btn2, col_btn3 = st.columns(3)

        with col_btn1:
            if collect_customers and st.button("📥 Collect Customers", type="primary"):
                start_collection("customers", collectors, batch_size, start_page)

        with col_btn2:
            if collect_orders and st.button("📥 Collect Orders"):
                start_collection("orders", collectors, batch_size, start_page)

        with col_btn3:
            if collect_subscriptions and st.button("📥 Collect Subscriptions"):
                start_collection("subscriptions", collectors, batch_size, start_page)

    # Show active collection progress
    if st.session_state.collection_active and st.session_state.collection_type:
        show_collection_progress(collectors)

def start_collection(collection_type, collectors, batch_size, start_page):
    """Start a collection process"""
    # Store parameters in session state so they're available during collection
    st.session_state.collection_batch_size = batch_size
    st.session_state.collection_start_page = start_page
    st.session_state.collection_active = True
    st.session_state.collection_type = collection_type
    st.session_state.collection_start_time = time.time()
    st.rerun()

def stop_collection(collectors):
    """Stop active collection"""
    if st.session_state.collection_type and collectors:
        collector = collectors.get(st.session_state.collection_type)
        if collector:
            collector.stop_collection()

    st.session_state.collection_active = False
    st.session_state.collection_type = None
    st.session_state.collection_start_time = None
    st.rerun()

def show_collection_progress(collectors):
    """Show live collection progress"""
    st.markdown("### 🔄 Live Collection Progress")

    collection_type = st.session_state.collection_type
    collector = collectors.get(collection_type)

    if not collector:
        st.error(f"No collector found for {collection_type}")
        return

    # Create progress container
    progress_container = st.empty()

    # Progress callback function
    def update_progress(progress_data):
        with progress_container.container():
            st.info(progress_data.get('status', 'Processing...'))

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Current Page", progress_data.get('current_page', 0))
            with col2:
                st.metric("Collected", progress_data.get('collected', 0))
            with col3:
                st.metric("Failed", progress_data.get('failed', 0))
            with col4:
                st.metric("Last ID", progress_data.get('last_customer_id', progress_data.get('last_order_id', progress_data.get('last_subscription_id', 'N/A'))))

    # Stop callback function
    def should_stop():
        return not st.session_state.collection_active

    # Get collection parameters from session state
    batch_size = st.session_state.get('collection_batch_size', 1000)
    start_page = st.session_state.get('collection_start_page', 0)

    try:
        # Start collection with progress updates using stored parameters
        if collection_type == "customers":
            st.info(f"Starting customer collection from page {start_page} with batch size {batch_size}")
            result = collector.collect_customers(
                batch_size=batch_size,
                start_page=start_page,
                progress_callback=update_progress,
                stop_callback=should_stop
            )
        elif collection_type == "orders":
            st.info(f"Starting order collection from page {start_page} with batch size {batch_size}")
            result = collector.collect_orders(
                batch_size=batch_size,
                start_page=start_page,
                progress_callback=update_progress,
                stop_callback=should_stop
            )
        elif collection_type == "subscriptions":
            st.info(f"Starting subscription collection from page {start_page} with batch size {batch_size}")
            result = collector.collect_subscriptions(
                batch_size=batch_size,
                start_page=start_page,
                progress_callback=update_progress,
                stop_callback=should_stop
            )

        # Show final results
        if result:
            st.success(f"Collection completed! Results: {result}")

    except Exception as e:
        st.error(f"Collection failed: {e}")

    finally:
        # Mark collection as complete
        st.session_state.collection_active = False
        st.session_state.collection_type = None
        st.cache_data.clear()
        time.sleep(2)
        st.rerun()

def render_migration_interface(collectors):
    """Render the migration interface"""
    st.subheader("Phase 2: Atomic Customer Migration")
    st.markdown("Migrate customers with all their orders and subscriptions as atomic units")

    if not collectors:
        st.warning("⚠️ Please provide valid API credentials to enable migration")
        return

    # Check if data is available
    stats = get_database_stats()
    if stats['customers'] == 0:
        st.warning("⚠️ No customer data found. Please complete data collection first.")
        return

    st.info(f"📊 Ready to migrate: {stats['customers']:,} customers, "
            f"{stats['orders']:,} orders, {stats['subscriptions']:,} subscriptions")

    # Migration configuration
    col1, col2 = st.columns(2)

    with col1:
        batch_size = st.number_input(
            "Migration Batch Size", 
            min_value=10, 
            max_value=500, 
            value=50
        )
        test_limit = st.number_input(
            "Test Migration Limit (0 = all)", 
            min_value=0, 
            max_value=1000, 
            value=10
        )

    with col2:
        dry_run = st.checkbox("Dry Run Mode", value=True)

    # Migration controls
    render_migration_controls(collectors, batch_size, test_limit, dry_run)

def render_migration_controls(collectors, batch_size, test_limit, dry_run):
    """Render migration control buttons"""
    st.markdown("---")

    # Migration status
    status_col1, status_col2 = st.columns([3, 1])

    with status_col1:
        if st.session_state.migration_active:
            st.success("🟢 Migration Active")
        else:
            st.info("⚪ Migration Idle")

    with status_col2:
        if st.session_state.migration_active:
            if st.button("⏹️ Stop Migration", type="secondary"):
                stop_migration(collectors)

    # Migration button
    if not st.session_state.migration_active:
        if st.button("🚀 Start Atomic Migration", type="primary"):
            start_migration(collectors, batch_size, test_limit, dry_run)

    # Show migration progress if active
    if st.session_state.migration_active:
        show_migration_progress(collectors)

def start_migration(collectors, batch_size, test_limit, dry_run):
    """Start migration process"""
    st.session_state.migration_active = True
    st.rerun()

def stop_migration(collectors):
    """Stop migration process"""
    migrator = collectors.get('migrator')
    if migrator:
        migrator.stop_migration()

    st.session_state.migration_active = False
    st.rerun()

def show_migration_progress(collectors):
    """Show live migration progress"""
    st.markdown("### 🚀 Live Migration Progress")

    migrator = collectors.get('migrator')
    if not migrator:
        st.error("No migrator available")
        return

    progress_container = st.empty()

    def update_progress(progress_data):
        with progress_container.container():
            st.info(progress_data.get('status', 'Migrating...'))

            col1, col2, col3 = st.columns(3)
            with col1:
                current = progress_data.get('current', 0)
                total = progress_data.get('total', 1)
                st.metric("Progress", f"{current}/{total}")
                if total > 0:
                    st.progress(current / total)
            with col2:
                st.metric("Migrated", progress_data.get('migrated', 0))
            with col3:
                st.metric("Failed", progress_data.get('failed', 0))

    def should_stop():
        return not st.session_state.migration_active

    try:
        # Get migration parameters (you might want to store these in session state)
        batch_size = 50
        test_limit = 10
        dry_run = True

        result = migrator.migrate_customers_atomic(
            batch_size=batch_size,
            test_limit=test_limit,
            dry_run=dry_run,
            progress_callback=update_progress,
            stop_callback=should_stop
        )

        if result:
            st.success(f"Migration completed! Results: {result}")

    except Exception as e:
        st.error(f"Migration failed: {e}")

    finally:
        st.session_state.migration_active = False
        time.sleep(2)
        st.rerun()

def render_statistics_interface(collectors):
    """Render the statistics interface"""
    st.subheader("Migration Statistics")

    if not collectors:
        st.info("Enter API credentials to see detailed statistics")
        return

    # Get statistics from each collector
    try:
        migrator = collectors.get('migrator')
        if migrator:
            stats = migrator.get_migration_stats()

            # Customer migration progress
            st.markdown("#### Customer Migration Progress")
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Customers", f"{stats['customers']['total']:,}")
            with col2:
                st.metric("Migrated", f"{stats['customers']['migrated']:,}")
            with col3:
                st.metric("Pending", f"{stats['customers']['pending']:,}")
            with col4:
                st.metric("Failed", f"{stats['customers']['failed']:,}")

            # Progress bar
            if stats['customers']['total'] > 0:
                progress = stats['customers']['migrated'] / stats['customers']['total']
                st.progress(progress)
                st.write(f"**Migration Progress:** {progress:.1%}")

            # Data collection summary
            st.markdown("#### Data Collection Summary")
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Orders Collected", f"{stats['orders_collected']:,}")
            with col2:
                st.metric("Subscriptions Collected", f"{stats['subscriptions_collected']:,}")
            with col3:
                overall_progress = stats.get('migration_progress', 0)
                st.metric("Overall Progress", f"{overall_progress:.1f}%")

    except Exception as e:
        st.error(f"Error loading statistics: {e}")

def render_data_security_panel():
    """Render data security and cleanup panel"""
    with st.expander("🔒 Data Security & Cleanup", expanded=False):
        st.warning("⚠️ This tool stores sensitive customer data. Ensure data is deleted after migration.")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("🗑️ Delete All Collected Data", type="secondary"):
                if st.session_state.get('confirm_delete', False):
                    delete_all_data()
                else:
                    st.session_state['confirm_delete'] = True
                    st.rerun()

        with col2:
            if st.session_state.get('confirm_delete', False):
                st.error("⚠️ Click 'Delete All Collected Data' again to confirm permanent deletion")
                if st.button("Cancel", type="primary"):
                    st.session_state['confirm_delete'] = False
                    st.rerun()

def delete_all_data():
    """Delete all migration data"""
    try:
        db_manager = DatabaseManager(os.getenv('DATABASE_URL'))
        session = db_manager.get_session()

        try:
            tables = [
                "cratejoy_subscriptions",
                "cratejoy_orders", 
                "cratejoy_customers",
                "subscription_mappings",
                "order_mappings",
                "customer_mappings",
                "migration_batches"
            ]

            for table in tables:
                try:
                    session.execute(text(f"DELETE FROM {table}"))
                except:
                    pass  # Table might not exist

            session.commit()
            st.success("✅ All migration data deleted successfully")
            st.session_state['confirm_delete'] = False
            st.cache_data.clear()

        finally:
            session.close()

    except Exception as e:
        st.error(f"❌ Error deleting data: {e}")

def main():
    """Main application function"""
    # Initialize session state
    init_session_state()

    # Check authentication
    if not check_password():
        return

    # Main UI
    st.title("🔄 Cratejoy to Shopify Migration Tool")
    st.markdown("Transfer customer, order, and subscription data from Cratejoy to Shopify")

    # Get API credentials and initialize clients
    credentials = get_api_credentials()
    collectors = initialize_clients(credentials)

    # Data security panel
    render_data_security_panel()

    # Progress dashboard
    render_progress_dashboard()

    st.markdown("---")

    # Main interface with tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📥 Phase 1: Data Collection", 
        "🚀 Phase 2: Migration", 
        "📊 Statistics",
        "📋 Audit & Repair"
    ])

    with tab1:
        render_collection_interface(collectors)

    with tab2:
        render_migration_interface(collectors)

    with tab3:
        render_statistics_interface(collectors)

    with tab4:
        render_audit_interface(collectors)

def render_audit_interface(collectors):
    """Render the audit and repair interface"""
    st.subheader("📋 Database Audit & Repair")
    st.markdown("Compare your database against Cratejoy API to find missing records")

    if not collectors or not collectors.get('auditor'):
        st.warning("⚠️ Please provide valid API credentials to enable audit")
        return

    # Audit configuration
    st.markdown("### 🔍 Audit Configuration")

    col1, col2 = st.columns(2)
    with col1:
        audit_type = st.selectbox(
            "Audit Type",
            ["Single Page", "Page Range", "Database Overview"],
            help="Choose what to audit"
        )

    with col2:
        batch_size = st.number_input(
            "Batch Size", 
            min_value=100, 
            max_value=1000, 
            value=1000,
            help="Records per page (should match collection batch size)"
        )

    # Audit parameters based on type
    if audit_type == "Single Page":
        page_number = st.number_input(
            "Page Number", 
            min_value=0, 
            max_value=1000, 
            value=200,
            help="Specific page to audit (e.g., 200 for the problematic page)"
        )

        if st.button("🔍 Audit Single Page", type="primary"):
            run_single_page_audit(collectors['auditor'], page_number, batch_size)

    elif audit_type == "Page Range":
        col1, col2 = st.columns(2)
        with col1:
            start_page = st.number_input("Start Page", min_value=0, max_value=1000, value=115)
        with col2:
            end_page = st.number_input("End Page", min_value=0, max_value=1000, value=125)

        if start_page > end_page:
            st.error("Start page must be <= End page")
        else:
            if st.button("🔍 Audit Page Range", type="primary"):
                run_page_range_audit(collectors['auditor'], start_page, end_page, batch_size)

    elif audit_type == "Database Overview":
        if st.button("📊 Database Overview", type="primary"):
            run_database_overview(collectors['auditor'])

def run_single_page_audit(auditor, page_number, batch_size):
    """Run audit for a single page"""
    st.markdown(f"### 🔍 Auditing Page {page_number}")

    with st.spinner(f"Auditing page {page_number}..."):
        try:
            result = auditor.audit_specific_page(page_number, batch_size)

            if 'error' in result:
                st.error(f"Audit failed: {result['error']}")
                return

            # Display results
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("API Records", result['api_count'])
            with col2:
                st.metric("DB Records", result['db_count'])
            with col3:
                missing_count = len(result.get('missing_customers', []))
                st.metric("Missing Records", missing_count)

            # Show missing customers if any
            if result.get('missing_customers'):
                st.markdown("#### ❌ Missing Customers")
                missing_df = []
                for customer in result['missing_customers'][:20]:  # Show first 20
                    missing_df.append({
                        'Customer ID': customer['customer_id'],
                        'Email': customer['email'],
                        'Data Size (bytes)': customer['data_size'],
                        'Has ID': customer['has_id']
                    })

                if missing_df:
                    st.dataframe(missing_df)

                    if len(result['missing_customers']) > 20:
                        st.info(f"Showing first 20 of {len(result['missing_customers'])} missing records")

                # Repair suggestion
                st.markdown("#### 🔧 Repair Suggestion")
                st.info(f"Run collection on page {page_number} to collect these {missing_count} missing records")

                if st.button(f"🔄 Re-collect Page {page_number}", key=f"repair_{page_number}"):
                    st.info("Use the Data Collection tab to re-run this specific page")
            else:
                st.success("✅ No missing records found on this page!")

            # Show data issues if any
            if result.get('data_issues'):
                st.markdown("#### ⚠️ Data Issues")
                for issue in result['data_issues'][:10]:
                    st.warning(f"Issue: {issue['issue']} - Customer ID: {issue.get('customer_id', 'N/A')}")

        except Exception as e:
            st.error(f"Audit failed: {e}")

def run_page_range_audit(auditor, start_page, end_page, batch_size):
    """Run audit for a page range"""
    st.markdown(f"### 🔍 Auditing Pages {start_page} to {end_page}")

    progress_bar = st.progress(0)
    status_text = st.empty()

    with st.spinner(f"Auditing pages {start_page} to {end_page}..."):
        try:
            total_pages = end_page - start_page + 1

            # Update progress during audit
            def update_progress(current_page):
                progress = (current_page - start_page + 1) / total_pages
                progress_bar.progress(progress)
                status_text.text(f"Auditing page {current_page}...")

            result = auditor.audit_page_range(start_page, end_page, batch_size)

            # Clear progress indicators
            progress_bar.empty()
            status_text.empty()

            # Display summary
            summary = result['summary']
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("API Records", summary['total_api_records'])
            with col2:
                st.metric("DB Records", summary['total_db_records'])
            with col3:
                st.metric("Missing", summary['missing_count'])
            with col4:
                st.metric("Extra", summary['extra_count'])

            # Show missing records
            if result['missing_from_db']:
                st.markdown("#### ❌ Missing Customer IDs")
                missing_ids = result['missing_from_db'][:50]  # Show first 50
                st.write(f"Missing IDs: {', '.join(map(str, missing_ids))}")

                if len(result['missing_from_db']) > 50:
                    st.info(f"Showing first 50 of {len(result['missing_from_db'])} missing IDs")

                # Repair suggestion
                st.markdown("#### 🔧 Repair Suggestion")
                st.info(f"Re-run collection on pages {start_page} to {end_page} to collect missing records")
            else:
                st.success("✅ No missing records found in this range!")

            # API errors
            if result['api_errors']:
                st.markdown("#### ⚠️ API Errors")
                for error in result['api_errors']:
                    st.error(f"Page {error['page']}: {error['error']}")

        except Exception as e:
            st.error(f"Range audit failed: {e}")

def run_database_overview(auditor):
    """Run database overview audit"""
    st.markdown("### 📊 Database Overview")

    with st.spinner("Analyzing database..."):
        try:
            stats = auditor.get_database_stats_by_page_range(pages_per_chunk=10)

            if stats:
                st.markdown("#### Record Distribution by Page Ranges")

                overview_data = []
                for stat in stats:
                    overview_data.append({
                        'Page Range': stat['page_range'],
                        'Records': stat['record_count'],
                        'Expected': stat['expected_count'],
                        'Difference': stat['record_count'] - stat['expected_count'],
                        'First ID': stat['first_id'],
                        'Last ID': stat['last_id']
                    })

                st.dataframe(overview_data)

                # Highlight problematic ranges
                st.markdown("#### 🎯 Problem Areas")
                problems = [row for row in overview_data if row['Difference'] < -100]

                if problems:
                    st.warning("Found ranges with significant missing records:")
                    for problem in problems:
                        st.write(f"**{problem['Page Range']}**: Missing ~{abs(problem['Difference'])} records")
                else:
                    st.success("No major gaps detected in record distribution")
            else:
                st.info("No data found in database")

        except Exception as e:
            st.error(f"Database overview failed: {e}")

if __name__ == "__main__":
    main()