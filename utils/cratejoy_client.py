import requests
import time
import base64
from typing import Dict, List, Any, Optional
from .rate_limiter import RateLimiter
from .logger import get_logger

class CratejoyClient:
    """Client for interacting with Cratejoy API"""
    
    def __init__(self, api_key: str, domain: str, client_secret: str = ""):
        self.api_key = api_key
        self.client_secret = client_secret or api_key  # fallback to api_key if no secret provided
        self.domain = domain.rstrip('/')
        self.base_url = "https://api.cratejoy.com/v1/"
        
        # Create Basic Auth header
        credentials = f"{self.api_key}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        self.rate_limiter = RateLimiter(requests_per_second=2)  # Cratejoy API limit
        self.logger = get_logger()
    
    def test_connection(self) -> Dict[str, Any]:
        """Test the API connection"""
        try:
            response = self._make_request('GET', '/customers/', params={'limit': 1})
            return {'success': True, 'message': 'Connection successful'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make a rate-limited API request"""
        self.rate_limiter.wait()
        
        # Remove leading slash from endpoint if present to avoid double slashes
        clean_endpoint = endpoint.lstrip('/')
        url = f"{self.base_url}{clean_endpoint}"
        self.logger.info(f"Making request to URL: {url}")
        
        try:
            response = self.session.request(method, url, **kwargs)
            response.raise_for_status()
            
            if response.content:
                return response.json()
            return {}
            
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error for {method} {url}: {e}")
            if e.response.status_code == 429:
                # Rate limit exceeded, wait and retry
                time.sleep(60)
                return self._make_request(method, endpoint, **kwargs)
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error for {method} {endpoint}: {e}")
            raise
    
    def get_customers(self, limit: int = None, page: int = 0) -> Dict[str, Any]:
        """Fetch customers from Cratejoy using page-based pagination"""
        params = {'page': page}
        
        # Only add limit if specified
        if limit is not None:
            params['limit'] = limit
        
        try:
            response = self._make_request('GET', '/customers/', params=params)
            self.logger.info(f"Fetched {len(response.get('results', []))} customers (page: {page}, limit: {limit})")
            return response
        except Exception as e:
            self.logger.error(f"Failed to fetch customers: {e}")
            raise
    
    def get_customer_details(self, customer_id: int) -> Dict[str, Any]:
        """Fetch detailed customer information"""
        try:
            response = self._make_request('GET', f'/customers/{customer_id}/')
            self.logger.debug(f"Fetched details for customer {customer_id}")
            return response
        except Exception as e:
            self.logger.error(f"Failed to fetch customer {customer_id}: {e}")
            raise
    
    def get_orders(self, limit: int = 100, page: int = 0, customer_id: Optional[int] = None) -> Dict[str, Any]:
        """Fetch orders from Cratejoy"""
        params = {
            'limit': limit,
            'page': page
        }
        
        if customer_id:
            params['customer'] = customer_id
        
        try:
            response = self._make_request('GET', '/orders/', params=params)
            self.logger.info(f"Fetched {len(response.get('results', []))} orders (page: {page})")
            return response
        except Exception as e:
            self.logger.error(f"Failed to fetch orders: {e}")
            raise
    
    def get_order_details(self, order_id: int) -> Dict[str, Any]:
        """Fetch detailed order information"""
        try:
            response = self._make_request('GET', f'/orders/{order_id}/')
            self.logger.debug(f"Fetched details for order {order_id}")
            return response
        except Exception as e:
            self.logger.error(f"Failed to fetch order {order_id}: {e}")
            raise
    
    def get_subscriptions(self, limit: int = 100, page: int = 0, customer_id: Optional[int] = None) -> Dict[str, Any]:
        """Fetch subscriptions from Cratejoy"""
        params = {
            'limit': limit,
            'page': page
        }
        
        if customer_id:
            params['customer'] = customer_id
        
        try:
            response = self._make_request('GET', '/subscriptions/', params=params)
            self.logger.info(f"Fetched {len(response.get('results', []))} subscriptions (page: {page})")
            return response
        except Exception as e:
            self.logger.error(f"Failed to fetch subscriptions: {e}")
            raise
    
    def get_subscription_details(self, subscription_id: int) -> Dict[str, Any]:
        """Fetch detailed subscription information"""
        try:
            response = self._make_request('GET', f'/subscriptions/{subscription_id}/')
            self.logger.debug(f"Fetched details for subscription {subscription_id}")
            return response
        except Exception as e:
            self.logger.error(f"Failed to fetch subscription {subscription_id}: {e}")
            raise
    
    def get_products(self, limit: int = 100, page: int = 0) -> Dict[str, Any]:
        """Fetch products from Cratejoy"""
        params = {
            'limit': limit,
            'page': page
        }
        
        try:
            response = self._make_request('GET', '/products/', params=params)
            self.logger.info(f"Fetched {len(response.get('results', []))} products (page: {page})")
            return response
        except Exception as e:
            self.logger.error(f"Failed to fetch products: {e}")
            raise
    
    def get_product_instance_details(self, instance_id: int) -> Dict[str, Any]:
        """Fetch detailed product instance information including price"""
        try:
            response = self._make_request('GET', f'/product_instances/{instance_id}/')
            self.logger.debug(f"Fetched details for product instance {instance_id}")
            return response
        except Exception as e:
            self.logger.error(f"Failed to fetch product instance {instance_id}: {e}")
            raise
    
    def get_all_customers(self) -> List[Dict[str, Any]]:
        """Fetch all customers using page-based pagination"""
        all_customers = []
        page = 0
        limit = 100
        
        while True:
            response = self.get_customers(limit=limit, page=page)
            customers = response.get('results', [])
            
            if not customers:
                break
                
            all_customers.extend(customers)
            
            # Check if there are more results
            if not response.get('next'):
                break
                
            page += 1
            
        self.logger.info(f"Fetched total of {len(all_customers)} customers")
        return all_customers
    
    def get_all_orders(self) -> List[Dict[str, Any]]:
        """Fetch all orders using pagination"""
        all_orders = []
        page = 0
        limit = 100
        
        while True:
            response = self.get_orders(limit=limit, page=page)
            orders = response.get('results', [])
            
            if not orders:
                break
                
            all_orders.extend(orders)
            
            # Check if there are more results
            if not response.get('next'):
                break
                
            page += 1
            
        self.logger.info(f"Fetched total of {len(all_orders)} orders")
        return all_orders
    
    def get_all_subscriptions(self) -> List[Dict[str, Any]]:
        """Fetch all subscriptions using pagination"""
        all_subscriptions = []
        page = 0
        limit = 100
        
        while True:
            response = self.get_subscriptions(limit=limit, page=page)
            subscriptions = response.get('results', [])
            
            if not subscriptions:
                break
                
            all_subscriptions.extend(subscriptions)
            
            # Check if there are more results
            if not response.get('next'):
                break
                
            page += 1
            
        self.logger.info(f"Fetched total of {len(all_subscriptions)} subscriptions")
        return all_subscriptions
    
    def get_all_products(self) -> List[Dict[str, Any]]:
        """Fetch all products using pagination"""
        all_products = []
        page = 0
        limit = 100
        total_count = None
        
        while True:
            response = self.get_products(limit=limit, page=page)
            products = response.get('results', [])
            
            # Get total count from first response
            if total_count is None:
                total_count = response.get('count', 0)
                self.logger.info(f"Total products available: {total_count}")
            
            self.logger.info(f"Got {len(products)} products (page: {page}, total fetched: {len(all_products)})")
            
            if not products:
                self.logger.info("No more products found, stopping pagination")
                break
                
            all_products.extend(products)
            
            # Stop if we've fetched all products based on count
            if len(all_products) >= total_count:
                self.logger.info(f"Reached total count ({len(all_products)}/{total_count}), stopping pagination")
                # Trim to exact count if we went over
                all_products = all_products[:total_count]
                break
                
            # If we got fewer products than the limit, we're at the end
            if len(products) < limit:
                self.logger.info(f"Got fewer products than limit ({len(products)} < {limit}), stopping pagination")
                break
                
            page += 1
            
        self.logger.info(f"Fetched total of {len(all_products)} products")
        return all_products
