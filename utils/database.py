"""
Database utilities for persistent migration data storage
"""
import os
import logging
from typing import Dict, List, Optional, Any
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class CustomerMapping(Base):
    """Table to store Cratejoy to Shopify customer mappings"""
    __tablename__ = 'customer_mappings'
    
    id = Column(Integer, primary_key=True)
    cratejoy_id = Column(Integer, unique=True, nullable=False, index=True)
    shopify_id = Column(Integer, nullable=True)
    email = Column(String(255), nullable=False)
    status = Column(String(50), nullable=False)  # 'pending', 'success', 'failed'
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class OrderMapping(Base):
    """Table to store Cratejoy to Shopify order mappings"""
    __tablename__ = 'order_mappings'
    
    id = Column(Integer, primary_key=True)
    cratejoy_id = Column(Integer, unique=True, nullable=False, index=True)
    shopify_id = Column(Integer, nullable=True)
    cratejoy_customer_id = Column(Integer, nullable=False)
    status = Column(String(50), nullable=False)  # 'pending', 'success', 'failed'
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class SubscriptionMapping(Base):
    """Table to store Cratejoy subscription migration status"""
    __tablename__ = 'subscription_mappings'
    
    id = Column(Integer, primary_key=True)
    cratejoy_id = Column(Integer, unique=True, nullable=False, index=True)
    cratejoy_customer_id = Column(Integer, nullable=False)
    shopify_customer_id = Column(Integer, nullable=True)
    status = Column(String(50), nullable=False)  # 'pending', 'success', 'failed'
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class MigrationBatch(Base):
    """Table to track migration batch progress"""
    __tablename__ = 'migration_batches'
    
    id = Column(Integer, primary_key=True)
    batch_type = Column(String(50), nullable=False)  # 'customers', 'orders', 'subscriptions'
    batch_number = Column(Integer, nullable=False)
    start_offset = Column(Integer, nullable=False)
    end_offset = Column(Integer, nullable=False)
    status = Column(String(50), nullable=False)  # 'pending', 'processing', 'completed', 'failed'
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)

class DatabaseManager:
    """Manager for database operations"""
    
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        self.engine = create_engine(self.database_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.logger = logging.getLogger(__name__)
    
    def create_tables(self):
        """Create all database tables"""
        Base.metadata.create_all(bind=self.engine)
        self.logger.info("Database tables created successfully")
    
    def get_session(self):
        """Get a database session"""
        return self.SessionLocal()
    
    def load_customer_mapping(self) -> Dict[int, int]:
        """Load all successful customer mappings into memory for fast lookup"""
        session = self.get_session()
        try:
            mappings = session.query(CustomerMapping).filter(
                CustomerMapping.status == 'success'
            ).all()
            
            result = {int(mapping.cratejoy_id): int(mapping.shopify_id) for mapping in mappings}
            self.logger.info(f"Loaded {len(result)} customer mappings from database")
            return result
        finally:
            session.close()
    
    def save_customer_mapping(self, cratejoy_id: int, email: str, shopify_id: Optional[int] = None, 
                             status: str = 'pending', error_message: Optional[str] = None):
        """Save or update customer mapping"""
        session = self.get_session()
        try:
            mapping = session.query(CustomerMapping).filter(
                CustomerMapping.cratejoy_id == cratejoy_id
            ).first()
            
            if mapping:
                mapping.shopify_id = shopify_id
                mapping.status = status
                mapping.error_message = error_message
                mapping.updated_at = datetime.utcnow()
            else:
                mapping = CustomerMapping(
                    cratejoy_id=cratejoy_id,
                    email=email,
                    shopify_id=shopify_id,
                    status=status,
                    error_message=error_message
                )
                session.add(mapping)
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def save_order_mapping(self, cratejoy_id: int, cratejoy_customer_id: int, 
                          shopify_id: Optional[int] = None, status: str = 'pending', 
                          error_message: Optional[str] = None):
        """Save or update order mapping"""
        session = self.get_session()
        try:
            mapping = session.query(OrderMapping).filter(
                OrderMapping.cratejoy_id == cratejoy_id
            ).first()
            
            if mapping:
                mapping.shopify_id = shopify_id
                mapping.status = status
                mapping.error_message = error_message
                mapping.updated_at = datetime.utcnow()
            else:
                mapping = OrderMapping(
                    cratejoy_id=cratejoy_id,
                    cratejoy_customer_id=cratejoy_customer_id,
                    shopify_id=shopify_id,
                    status=status,
                    error_message=error_message
                )
                session.add(mapping)
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def save_subscription_mapping(self, cratejoy_id: int, cratejoy_customer_id: int,
                                 shopify_customer_id: Optional[int] = None, status: str = 'pending',
                                 error_message: Optional[str] = None):
        """Save or update subscription mapping"""
        session = self.get_session()
        try:
            mapping = session.query(SubscriptionMapping).filter(
                SubscriptionMapping.cratejoy_id == cratejoy_id
            ).first()
            
            if mapping:
                mapping.shopify_customer_id = shopify_customer_id
                mapping.status = status
                mapping.error_message = error_message
                mapping.updated_at = datetime.utcnow()
            else:
                mapping = SubscriptionMapping(
                    cratejoy_id=cratejoy_id,
                    cratejoy_customer_id=cratejoy_customer_id,
                    shopify_customer_id=shopify_customer_id,
                    status=status,
                    error_message=error_message
                )
                session.add(mapping)
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def get_failed_customers(self) -> List[CustomerMapping]:
        """Get all failed customer mappings for retry"""
        session = self.get_session()
        try:
            return session.query(CustomerMapping).filter(
                CustomerMapping.status == 'failed'
            ).all()
        finally:
            session.close()
    
    def get_failed_orders(self) -> List[OrderMapping]:
        """Get all failed order mappings for retry"""
        session = self.get_session()
        try:
            return session.query(OrderMapping).filter(
                OrderMapping.status == 'failed'
            ).all()
        finally:
            session.close()
    
    def get_failed_subscriptions(self) -> List[SubscriptionMapping]:
        """Get all failed subscription mappings for retry"""
        session = self.get_session()
        try:
            return session.query(SubscriptionMapping).filter(
                SubscriptionMapping.status == 'failed'
            ).all()
        finally:
            session.close()
    
    def get_migration_stats(self) -> Dict[str, Any]:
        """Get overall migration statistics"""
        session = self.get_session()
        try:
            customers_total = session.query(CustomerMapping).count()
            customers_success = session.query(CustomerMapping).filter(CustomerMapping.status == 'success').count()
            customers_failed = session.query(CustomerMapping).filter(CustomerMapping.status == 'failed').count()
            
            orders_total = session.query(OrderMapping).count()
            orders_success = session.query(OrderMapping).filter(OrderMapping.status == 'success').count()
            orders_failed = session.query(OrderMapping).filter(OrderMapping.status == 'failed').count()
            
            subscriptions_total = session.query(SubscriptionMapping).count()
            subscriptions_success = session.query(SubscriptionMapping).filter(SubscriptionMapping.status == 'success').count()
            subscriptions_failed = session.query(SubscriptionMapping).filter(SubscriptionMapping.status == 'failed').count()
            
            return {
                'customers': {'total': customers_total, 'success': customers_success, 'failed': customers_failed},
                'orders': {'total': orders_total, 'success': orders_success, 'failed': orders_failed},
                'subscriptions': {'total': subscriptions_total, 'success': subscriptions_success, 'failed': subscriptions_failed}
            }
        finally:
            session.close()