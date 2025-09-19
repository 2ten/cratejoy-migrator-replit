import requests
import time
from typing import Dict, List, Any, Optional
from .rate_limiter import RateLimiter
from .logger import get_logger

class ShopifyClient:
    """Client for interacting with Shopify Admin API"""
    
    def __init__(self, api_key: str, password: str, domain: str):
        self.api_key = api_key
        self.password = password
        self.domain = domain.rstrip('/')
        if not self.domain.endswith('.myshopify.com'):
            self.domain = f"{self.domain}.myshopify.com"
        
        self.base_url = f"https://{self.domain}/admin/api/2023-10"
        self.session = requests.Session()
        self.session.auth = (self.api_key, self.password)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Shopify API limit: 2 calls per second
        self.rate_limiter = RateLimiter(requests_per_second=2)
        self.logger = get_logger()
    
    def test_connection(self) -> Dict[str, Any]:
        """Test the API connection"""
        try:
            response = self._make_request('GET', '/shop.json')
            return {'success': True, 'message': 'Connection successful', 'shop': response.get('shop', {})}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make a rate-limited API request"""
        self.rate_limiter.wait()
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self.session.request(method, url, **kwargs)
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 2))
                self.logger.warning(f"Rate limit exceeded, waiting {retry_after} seconds")
                time.sleep(retry_after)
                return self._make_request(method, endpoint, **kwargs)
            
            response.raise_for_status()
            
            if response.content:
                return response.json()
            return {}
            
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error for {method} {endpoint}: {e}")
            if hasattr(e, 'response') and e.response.content:
                try:
                    error_detail = e.response.json()
                    self.logger.error(f"Error details: {error_detail}")
                except:
                    pass
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for {method} {endpoint}: {e}")
            raise
    
    def create_customer(self, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new customer in Shopify"""
        try:
            payload = {'customer': customer_data}
            response = self._make_request('POST', '/customers.json', json=payload)
            customer = response.get('customer', {})
            self.logger.info(f"Created customer {customer.get('id')} - {customer.get('email')}")
            return customer
        except Exception as e:
            self.logger.error(f"Failed to create customer: {e}")
            raise
    
    def update_customer(self, customer_id: int, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing customer in Shopify"""
        try:
            payload = {'customer': customer_data}
            response = self._make_request('PUT', f'/customers/{customer_id}.json', json=payload)
            customer = response.get('customer', {})
            self.logger.info(f"Updated customer {customer_id}")
            return customer
        except Exception as e:
            self.logger.error(f"Failed to update customer {customer_id}: {e}")
            raise
    
    def get_customer_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Find a customer by email address"""
        try:
            params = {'limit': 1, 'email': email}
            response = self._make_request('GET', '/customers/search.json', params=params)
            customers = response.get('customers', [])
            if customers:
                return customers[0]
            return None
        except Exception as e:
            self.logger.error(f"Failed to search customer by email {email}: {e}")
            return None
    
    def create_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new order in Shopify"""
        try:
            payload = {'order': order_data}
            response = self._make_request('POST', '/orders.json', json=payload)
            order = response.get('order', {})
            self.logger.info(f"Created order {order.get('id')} - {order.get('name')}")
            return order
        except Exception as e:
            self.logger.error(f"Failed to create order: {e}")
            raise
    
    def get_order(self, order_id: int) -> Dict[str, Any]:
        """Get an order by ID"""
        try:
            response = self._make_request('GET', f'/orders/{order_id}.json')
            return response.get('order', {})
        except Exception as e:
            self.logger.error(f"Failed to get order {order_id}: {e}")
            raise
    
    def create_product(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new product in Shopify"""
        try:
            payload = {'product': product_data}
            response = self._make_request('POST', '/products.json', json=payload)
            product = response.get('product', {})
            return product
        except Exception as e:
            self.logger.error(f"Failed to create product: {e}")
            raise
    
    def get_product_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """Find a product by title"""
        try:
            params = {'limit': 1, 'title': title}
            response = self._make_request('GET', '/products.json', params=params)
            products = response.get('products', [])
            if products:
                return products[0]
            return None
        except Exception as e:
            self.logger.error(f"Failed to search product by title {title}: {e}")
            return None
    
    def create_draft_order(self, draft_order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a draft order (useful for subscriptions)"""
        try:
            payload = {'draft_order': draft_order_data}
            response = self._make_request('POST', '/draft_orders.json', json=payload)
            draft_order = response.get('draft_order', {})
            self.logger.info(f"Created draft order {draft_order.get('id')}")
            return draft_order
        except Exception as e:
            self.logger.error(f"Failed to create draft order: {e}")
            raise
    
    def complete_draft_order(self, draft_order_id: int) -> Dict[str, Any]:
        """Complete a draft order to create an actual order"""
        try:
            response = self._make_request('PUT', f'/draft_orders/{draft_order_id}/complete.json')
            order = response.get('order', {})
            self.logger.info(f"Completed draft order {draft_order_id} -> order {order.get('id')}")
            return order
        except Exception as e:
            self.logger.error(f"Failed to complete draft order {draft_order_id}: {e}")
            raise
    
    def create_customer_address(self, customer_id: int, address_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a customer address"""
        try:
            payload = {'address': address_data}
            response = self._make_request('POST', f'/customers/{customer_id}/addresses.json', json=payload)
            address = response.get('address', {})
            self.logger.info(f"Created address for customer {customer_id}")
            return address
        except Exception as e:
            self.logger.error(f"Failed to create address for customer {customer_id}: {e}")
            raise
    
    def add_customer_tags(self, customer_id: int, tags: List[str]) -> Dict[str, Any]:
        """Add tags to a customer"""
        try:
            # First get the current customer to preserve existing tags
            current_customer = self._make_request('GET', f'/customers/{customer_id}.json')
            existing_tags = current_customer.get('customer', {}).get('tags', '')
            
            # Combine existing and new tags
            all_tags = set()
            if existing_tags:
                all_tags.update([tag.strip() for tag in existing_tags.split(',')])
            all_tags.update(tags)
            
            # Update customer with all tags
            customer_data = {'tags': ', '.join(sorted(all_tags))}
            return self.update_customer(customer_id, customer_data)
        except Exception as e:
            self.logger.error(f"Failed to add tags to customer {customer_id}: {e}")
            raise
    
    def create_customer_metafield(self, customer_id: int, metafield_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a metafield for a customer"""
        try:
            payload = {'metafield': metafield_data}
            response = self._make_request('POST', f'/customers/{customer_id}/metafields.json', json=payload)
            metafield = response.get('metafield', {})
            self.logger.info(f"Created metafield for customer {customer_id}: {metafield_data['key']}")
            return metafield
        except Exception as e:
            self.logger.error(f"Failed to create metafield for customer {customer_id}: {e}")
            raise

    def update_customer_metafield(self, customer_id: int, metafield_id: int, metafield_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a customer metafield"""
        try:
            payload = {'metafield': metafield_data}
            response = self._make_request('PUT', f'/customers/{customer_id}/metafields/{metafield_id}.json', json=payload)
            metafield = response.get('metafield', {})
            self.logger.info(f"Updated metafield {metafield_id} for customer {customer_id}")
            return metafield
        except Exception as e:
            self.logger.error(f"Failed to update metafield {metafield_id} for customer {customer_id}: {e}")
            raise

    def get_customer_metafields(self, customer_id: int) -> List[Dict[str, Any]]:
        """Get all metafields for a customer"""
        try:
            response = self._make_request('GET', f'/customers/{customer_id}/metafields.json')
            return response.get('metafields', [])
        except Exception as e:
            self.logger.error(f"Failed to get metafields for customer {customer_id}: {e}")
            return []

    def get_shop_info(self) -> Dict[str, Any]:
        """Get shop information"""
        try:
            response = self._make_request('GET', '/shop.json')
            return response.get('shop', {})
        except Exception as e:
            self.logger.error(f"Failed to get shop info: {e}")
            raise
