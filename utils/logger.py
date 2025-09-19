import logging
import os
from datetime import datetime
from typing import Optional

# Global logger instance
_logger = None

def setup_logger(name: str = "migration", level: int = logging.INFO, 
                log_file: Optional[str] = None) -> logging.Logger:
    """
    Set up and configure logger for the migration tool
    
    Args:
        name: Logger name
        level: Logging level
        log_file: Optional log file path
    
    Returns:
        Configured logger instance
    """
    global _logger
    
    if _logger is not None:
        return _logger
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Prevent duplicate handlers
    if logger.handlers:
        logger.handlers.clear()
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(simple_formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        try:
            # Create logs directory if it doesn't exist
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)  # More detailed logging to file
            file_handler.setFormatter(detailed_formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Could not create file handler for {log_file}: {e}")
    
    # Default file handler for migration logs
    try:
        default_log_file = f"logs/migration_{datetime.now().strftime('%Y%m%d')}.log"
        os.makedirs("logs", exist_ok=True)
        
        default_file_handler = logging.FileHandler(default_log_file)
        default_file_handler.setLevel(logging.DEBUG)
        default_file_handler.setFormatter(detailed_formatter)
        logger.addHandler(default_file_handler)
    except Exception as e:
        logger.warning(f"Could not create default file handler: {e}")
    
    _logger = logger
    return logger

def get_logger() -> logging.Logger:
    """Get the global logger instance"""
    if _logger is None:
        return setup_logger()
    return _logger

class MigrationLogger:
    """Enhanced logger with migration-specific methods"""
    
    def __init__(self, base_logger: Optional[logging.Logger] = None):
        self.logger = base_logger or get_logger()
        self.migration_start_time = None
        self.operation_counts = {
            'customers': {'success': 0, 'failed': 0},
            'orders': {'success': 0, 'failed': 0},
            'subscriptions': {'success': 0, 'failed': 0}
        }
    
    def start_migration(self):
        """Log migration start"""
        self.migration_start_time = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info("MIGRATION STARTED")
        self.logger.info(f"Start time: {self.migration_start_time}")
        self.logger.info("=" * 80)
    
    def end_migration(self):
        """Log migration end with summary"""
        end_time = datetime.now()
        duration = end_time - self.migration_start_time if self.migration_start_time else None
        
        self.logger.info("=" * 80)
        self.logger.info("MIGRATION COMPLETED")
        self.logger.info(f"End time: {end_time}")
        if duration:
            self.logger.info(f"Duration: {duration}")
        
        # Log summary
        total_success = sum(counts['success'] for counts in self.operation_counts.values())
        total_failed = sum(counts['failed'] for counts in self.operation_counts.values())
        
        self.logger.info("MIGRATION SUMMARY:")
        for operation, counts in self.operation_counts.items():
            if counts['success'] > 0 or counts['failed'] > 0:
                self.logger.info(f"  {operation.title()}: {counts['success']} successful, {counts['failed']} failed")
        
        self.logger.info(f"  Total: {total_success} successful, {total_failed} failed")
        self.logger.info("=" * 80)
    
    def log_customer_success(self, customer_id: str, shopify_id: str):
        """Log successful customer migration"""
        self.operation_counts['customers']['success'] += 1
        self.logger.info(f"Customer migrated: Cratejoy ID {customer_id} -> Shopify ID {shopify_id}")
    
    def log_customer_failure(self, customer_id: str, error: str):
        """Log failed customer migration"""
        self.operation_counts['customers']['failed'] += 1
        self.logger.error(f"Customer migration failed: Cratejoy ID {customer_id} - {error}")
    
    def log_order_success(self, order_id: str, shopify_id: str):
        """Log successful order migration"""
        self.operation_counts['orders']['success'] += 1
        self.logger.info(f"Order migrated: Cratejoy ID {order_id} -> Shopify ID {shopify_id}")
    
    def log_order_failure(self, order_id: str, error: str):
        """Log failed order migration"""
        self.operation_counts['orders']['failed'] += 1
        self.logger.error(f"Order migration failed: Cratejoy ID {order_id} - {error}")
    
    def log_subscription_success(self, subscription_id: str, shopify_id: str):
        """Log successful subscription migration"""
        self.operation_counts['subscriptions']['success'] += 1
        self.logger.info(f"Subscription migrated: Cratejoy ID {subscription_id} -> Shopify ID {shopify_id}")
    
    def log_subscription_failure(self, subscription_id: str, error: str):
        """Log failed subscription migration"""
        self.operation_counts['subscriptions']['failed'] += 1
        self.logger.error(f"Subscription migration failed: Cratejoy ID {subscription_id} - {error}")
    
    def log_api_call(self, service: str, endpoint: str, method: str, status_code: int, duration: float):
        """Log API call details"""
        self.logger.debug(f"API Call: {service} {method} {endpoint} - {status_code} ({duration:.2f}s)")
    
    def log_rate_limit(self, service: str, retry_after: int):
        """Log rate limit hit"""
        self.logger.warning(f"Rate limit hit for {service}, waiting {retry_after} seconds")
    
    def log_batch_progress(self, operation: str, current: int, total: int):
        """Log batch processing progress"""
        percentage = (current / total * 100) if total > 0 else 0
        self.logger.info(f"{operation} progress: {current}/{total} ({percentage:.1f}%)")
    
    def log_validation_error(self, record_type: str, record_id: str, errors: list):
        """Log data validation errors"""
        self.logger.warning(f"Validation errors for {record_type} {record_id}: {', '.join(errors)}")
    
    # Delegate other logging methods to base logger
    def debug(self, message):
        self.logger.debug(message)
    
    def info(self, message):
        self.logger.info(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def critical(self, message):
        self.logger.critical(message)
