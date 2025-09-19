# Cratejoy to Shopify Migration Tool

## Project Overview
A robust Streamlit-based data migration platform designed to seamlessly transfer e-commerce data between platforms, with advanced two-phase migration capabilities for Cratejoy to Shopify migrations.

**Current Status**: Successfully deployed and actively collecting data - automated collection running continuously on Replit infrastructure with shared database access.

## User Preferences
- Prefers efficient batch processing with minimal manual intervention
- Values data security and proper cleanup capabilities
- Wants deployment for reliability during long-running collection processes
- Prioritizes collecting all customers first before moving to orders/subscriptions
- Prefers to make code changes directly - only wants code editing assistance when specifically requested

## Recent Changes
- **2025-06-16**: Fixed critical pagination bug - removed flawed use_start_page logic causing collection to revert to original page
- **2025-06-16**: Added debugging output to track page transitions and identify Cratejoy API pagination issues
- **2025-06-16**: Simplified UI messages - removed confusing smart resume, partial page warnings, and target reached messages
- **2025-06-15**: Fixed pagination logic - use result count instead of unreliable next_url field
- **2025-06-15**: Removed offset feature - simplified collection to prevent loop issues
- **2025-06-15**: Fixed database schema - upgraded ID columns to BIGINT for large Cratejoy customer IDs
- **2025-06-15**: Removed form pre-population to prevent restart loops at calculated pages
- **2025-06-15**: Fixed database connection pooling issue - batch transactions prevent collection failures

## Project Architecture
### Two-Phase Migration System
- **Phase 1**: Bulk data collection from Cratejoy to local PostgreSQL database
- **Phase 2**: Atomic customer-centric migration to Shopify with proper linking

### Key Components
- Streamlit web interface with three-tier UI (Quick Test, Enhanced, Two-Phase)
- Secure API integration clients for Cratejoy and Shopify
- Comprehensive local data staging with PostgreSQL persistence
- Advanced pagination with resume functionality
- Authentication and data security controls

### Technical Decisions
- Two-phase approach chosen for 600k+ customer scale
- Store subscription history in customer metafields with structured JSON
- Use "cj-" prefix tagging system for all migrated entities
- Cratejoy API uses page-based pagination (not offset-based)
- Recommended batch sizes: 500-1000 for collection, 25-50 for migration

## Data Security Measures
- Authentication required for app access
- Secure data deletion functionality with confirmation
- Environment-based secrets management
- Database cleanup procedures implemented

## Current Progress
- Customer collection: 119,000+ of ~600k total (20% complete) - actively running from page 120+
- Known gaps: Pages 118-119 interval needs verification after main collection
- Orders collection: Not started
- Subscriptions collection: Not started
- Migration to Shopify: Pending data collection completion

## Post-Collection Tasks
- Data integrity check: Compare collected customer count vs Cratejoy total to identify missing records
- Gap analysis: Verify pages 118-119 interval for missing customers
- Backfill missing records before proceeding to orders/subscriptions