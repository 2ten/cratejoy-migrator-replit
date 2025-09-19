from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import re
from .logger import get_logger

class DataMapper:
    """Maps data between Cratejoy and Shopify formats"""
    
    def __init__(self):
        self.logger = get_logger()
    
    def _clean_text(self, text: Optional[str]) -> Optional[str]:
        """Clean text by removing HTML tags, special formatting, and normalizing"""
        if not text:
            return None
            
        text = str(text)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Remove common formatting characters and entities
        text = text.replace('&nbsp;', ' ')
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = text.replace('&quot;', '"')
        text = text.replace('&#39;', "'")
        
        # Remove extra whitespace and normalize
        text = ' '.join(text.split())
        
        return text.strip() if text.strip() else None
    
    def _clean_phone_number(self, phone: Optional[str]) -> Optional[str]:
        """Clean and validate phone number for Shopify compatibility"""
        if not phone:
            return None
            
        # Remove all non-digit characters except + and spaces
        phone = str(phone).strip()
        
        # Remove common formatting characters
        phone = re.sub(r'[^\d\+\s\-\(\)\.x]', '', phone)
        
        # Handle extension notation (remove it for Shopify)
        phone = re.sub(r'[x]\d+$', '', phone, flags=re.IGNORECASE)
        phone = re.sub(r'ext\.\s*\d+$', '', phone, flags=re.IGNORECASE)
        
        # Extract just the digits and plus sign
        digits_only = re.sub(r'[^\d\+]', '', phone)
        
        # Skip if no digits found
        if not re.search(r'\d', digits_only):
            return None
            
        # Handle US/Canada numbers (10 or 11 digits)
        if digits_only.startswith('+1'):
            digits_only = digits_only[2:]  # Remove +1
        elif digits_only.startswith('1') and len(digits_only) == 11:
            digits_only = digits_only[1:]  # Remove leading 1
            
        # Validate length for US/Canada numbers
        if len(digits_only) == 10:
            # Format as +1XXXXXXXXXX
            return f"+1{digits_only}"
        elif len(digits_only) > 10:
            # For international numbers, keep original format but clean
            clean_phone = re.sub(r'[^\d\+]', '', phone)
            if clean_phone.startswith('+'):
                return clean_phone
            else:
                return f"+{clean_phone}"
        else:
            # Too short, likely invalid
            return None
    
    def map_customer(self, cratejoy_customer: Dict[str, Any]) -> Dict[str, Any]:
        """Map Cratejoy customer to Shopify customer format"""
        try:
            # Extract address information
            shipping_address = self._extract_address(cratejoy_customer, 'shipping')
            billing_address = self._extract_address(cratejoy_customer, 'billing')
            
            # Map basic customer data
            shopify_customer = {
                'email': cratejoy_customer.get('email', ''),
                'first_name': cratejoy_customer.get('first_name', ''),
                'last_name': cratejoy_customer.get('last_name', ''),
                'phone': self._clean_phone_number(cratejoy_customer.get('phone')),
                'verified_email': True,
                'tags': 'cratejoy-import',
                'note': f"Imported from Cratejoy. Original ID: {cratejoy_customer.get('id')}",
                'created_at': self._convert_datetime(cratejoy_customer.get('date_created')),
                'updated_at': self._convert_datetime(cratejoy_customer.get('date_updated'))
            }
            
            # Add addresses if available
            addresses = []
            if shipping_address:
                addresses.append(shipping_address)
            if billing_address and billing_address != shipping_address:
                addresses.append(billing_address)
            
            if addresses:
                shopify_customer['addresses'] = addresses
                # Set default address
                shopify_customer['default_address'] = addresses[0]
            
            # Add marketing opt-in status
            if cratejoy_customer.get('marketing_opt_in'):
                shopify_customer['accepts_marketing'] = True
                shopify_customer['marketing_opt_in_level'] = 'confirmed_opt_in'
            
            self.logger.debug(f"Mapped customer {cratejoy_customer.get('id')} to Shopify format")
            return shopify_customer
            
        except Exception as e:
            self.logger.error(f"Failed to map customer {cratejoy_customer.get('id')}: {e}")
            raise
    
    def map_order(self, cratejoy_order: Dict[str, Any], shopify_customer_id: Optional[int] = None, product_mapping: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Map Cratejoy order to Shopify order format with product linking"""
        try:
            # Map line items with product linking
            line_items = []
            for item in cratejoy_order.get('items', []):
                line_item = self._map_order_line_item(item, product_mapping)
                if line_item:
                    line_items.append(line_item)
            
            # Extract addresses
            shipping_address = self._extract_address(cratejoy_order, 'shipping')
            billing_address = self._extract_address(cratejoy_order, 'billing')
            
            # Determine if this is a subscription order
            is_subscription_order = cratejoy_order.get('subscription_id') is not None
            order_tags = 'cj-import'
            if is_subscription_order:
                order_tags += ',cj-subscription'
            
            # Map order data
            shopify_order = {
                'email': cratejoy_order.get('customer_email', ''),
                'created_at': self._convert_datetime(cratejoy_order.get('date_created')),
                'updated_at': self._convert_datetime(cratejoy_order.get('date_updated')),
                'line_items': line_items,
                'financial_status': self._map_financial_status(cratejoy_order.get('status')),
                'fulfillment_status': self._map_fulfillment_status(cratejoy_order.get('fulfillment_status')),
                'tags': order_tags,
                'note': f"Imported from Cratejoy",
                'currency': cratejoy_order.get('currency', 'USD'),
                'total_price': str(cratejoy_order.get('total', '0.00')),
                'subtotal_price': str(cratejoy_order.get('subtotal', '0.00')),
                'total_tax': str(cratejoy_order.get('tax', '0.00')),
                'total_shipping_price_set': {
                    'shop_money': {
                        'amount': str(cratejoy_order.get('shipping', '0.00')),
                        'currency_code': cratejoy_order.get('currency', 'USD')
                    }
                },
                'metafields': [
                    {
                        'namespace': 'cratejoy',
                        'key': 'order_id',
                        'value': str(cratejoy_order.get('id')),
                        'type': 'single_line_text_field'
                    }
                ]
            }
            
            # Add customer ID if provided
            if shopify_customer_id:
                shopify_order['customer'] = {'id': shopify_customer_id}
            
            # Add addresses
            if shipping_address:
                shopify_order['shipping_address'] = shipping_address
            if billing_address:
                shopify_order['billing_address'] = billing_address
            
            # Add discount information
            if cratejoy_order.get('discount_amount'):
                shopify_order['discount_applications'] = [{
                    'type': 'discount_code',
                    'code': cratejoy_order.get('discount_code', 'CRATEJOY_DISCOUNT'),
                    'value': str(cratejoy_order.get('discount_amount')),
                    'value_type': 'fixed_amount',
                    'allocation_method': 'across'
                }]
            
            self.logger.debug(f"Mapped order {cratejoy_order.get('id')} to Shopify format")
            return shopify_order
            
        except Exception as e:
            self.logger.error(f"Failed to map order {cratejoy_order.get('id')}: {e}")
            raise
    
    def map_subscription_to_metafield(self, cratejoy_subscription: Dict[str, Any]) -> Dict[str, Any]:
        """Map Cratejoy subscription to Shopify customer metafield data"""
        try:
            # Extract key subscription information
            subscription_data = {
                'cratejoy_id': cratejoy_subscription.get('id'),
                'status': cratejoy_subscription.get('status', 'unknown'),
                'frequency': cratejoy_subscription.get('frequency', 'unknown'),
                'created_at': self._convert_datetime(cratejoy_subscription.get('date_created')),
                'updated_at': self._convert_datetime(cratejoy_subscription.get('date_updated')),
                'next_billing_date': self._convert_datetime(cratejoy_subscription.get('next_billing_date')),
                'cancelled_at': self._convert_datetime(cratejoy_subscription.get('cancelled_at')),
                'paused_at': self._convert_datetime(cratejoy_subscription.get('paused_at')),
                'total_value': str(cratejoy_subscription.get('total', '0.00')),
                'currency': cratejoy_subscription.get('currency', 'USD'),
                'billing_cycles_completed': cratejoy_subscription.get('billing_cycles_completed', 0),
                'products': []
            }
            
            # Map subscription items
            for item in cratejoy_subscription.get('items', []):
                product_data = {
                    'cratejoy_product_id': item.get('product_id'),
                    'product_name': item.get('product_name', 'Unknown Product'),
                    'sku': item.get('sku', ''),
                    'quantity': item.get('quantity', 1),
                    'price': str(item.get('price', '0.00')),
                    'vendor': item.get('vendor', '')
                }
                subscription_data['products'].append(product_data)
            
            # Create metafield structure
            metafield_data = {
                'namespace': 'cratejoy',
                'key': f'subscription_{cratejoy_subscription.get("id")}',
                'value': subscription_data,
                'type': 'json'
            }
            
            self.logger.debug(f"Mapped subscription {cratejoy_subscription.get('id')} to metafield")
            return metafield_data
            
        except Exception as e:
            self.logger.error(f"Failed to map subscription {cratejoy_subscription.get('id')} to metafield: {e}")
            raise
    
    def _extract_address(self, data: Dict[str, Any], address_type: str) -> Optional[Dict[str, Any]]:
        """Extract address information from Cratejoy data"""
        try:
            address_key = f"{address_type}_address"
            if address_key not in data:
                # Try alternative keys
                if address_type == 'shipping':
                    address_data = data.get('shipping_address') or data.get('address')
                elif address_type == 'billing':
                    address_data = data.get('billing_address') or data.get('address')
                else:
                    return None
            else:
                address_data = data[address_key]
            
            if not address_data:
                return None
            
            # Clean and validate address
            address = {
                'first_name': address_data.get('first_name', ''),
                'last_name': address_data.get('last_name', ''),
                'company': address_data.get('company', ''),
                'address1': address_data.get('line1', ''),
                'address2': address_data.get('line2', ''),
                'city': address_data.get('city', ''),
                'province': address_data.get('state', ''),
                'country': address_data.get('country', ''),
                'zip': address_data.get('postal_code', ''),
                'phone': self._clean_phone_number(address_data.get('phone'))
            }
            
            # Only return address if it has essential information
            if address['address1'] and address['city']:
                return address
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Failed to extract {address_type} address: {e}")
            return None
    
    def _map_order_line_item(self, cratejoy_item: Dict[str, Any], product_mapping: Optional[Dict[str, Dict[str, Any]]] = None) -> Optional[Dict[str, Any]]:
        """Map Cratejoy order item to Shopify line item with product linking"""
        try:
            sku = cratejoy_item.get('sku', '').strip()
            product_id = None
            variant_id = None
            
            # Try to link to existing Shopify product
            if product_mapping and sku and sku in product_mapping:
                shopify_product = product_mapping[sku]
                product_id = shopify_product.get('product_id')
                variant_id = shopify_product.get('variant_id')
                self.logger.debug(f"Linked SKU {sku} to Shopify variant {variant_id}")
            elif sku:
                self.logger.debug(f"No Shopify product found for SKU: {sku}")
            
            line_item = {
                'title': cratejoy_item.get('product_name', 'Unknown Product'),
                'quantity': cratejoy_item.get('quantity', 1),
                'price': str(cratejoy_item.get('price', '0.00')),
                'sku': sku,
                'vendor': cratejoy_item.get('vendor', ''),
                'requires_shipping': True,
                'taxable': True,
                'properties': [
                    {'name': 'Cratejoy Product ID', 'value': str(cratejoy_item.get('product_id', ''))}
                ]
            }
            
            # Add product/variant IDs if found
            if product_id:
                line_item['product_id'] = product_id
            if variant_id:
                line_item['variant_id'] = variant_id
            
            # Add note about linking status
            if not variant_id and sku:
                line_item['properties'].append({
                    'name': 'Migration Note', 
                    'value': f'Original SKU {sku} - no matching Shopify product found'
                })
            
            return line_item
            
        except Exception as e:
            self.logger.warning(f"Failed to map order line item: {e}")
            return None
    
    def _map_subscription_line_item(self, cratejoy_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Map Cratejoy subscription item to Shopify line item"""
        try:
            return {
                'title': cratejoy_item.get('product_name', 'Unknown Subscription Product'),
                'quantity': cratejoy_item.get('quantity', 1),
                'price': str(cratejoy_item.get('price', '0.00')),
                'sku': cratejoy_item.get('sku', ''),
                'vendor': cratejoy_item.get('vendor', ''),
                'requires_shipping': True,
                'taxable': True,
                'properties': [
                    {'name': 'Cratejoy Product ID', 'value': str(cratejoy_item.get('product_id', ''))},
                    {'name': 'Subscription Item', 'value': 'true'}
                ]
            }
        except Exception as e:
            self.logger.warning(f"Failed to map subscription line item: {e}")
            return None
    
    def _convert_datetime(self, date_string: Optional[str]) -> Optional[str]:
        """Convert Cratejoy datetime to Shopify format"""
        if not date_string:
            return None
        
        try:
            # Parse various datetime formats
            for fmt in ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S']:
                try:
                    dt = datetime.strptime(date_string, fmt)
                    # Ensure timezone awareness
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    # Return in Shopify's expected format: YYYY-MM-DDTHH:MM:SS-00:00
                    return dt.strftime('%Y-%m-%dT%H:%M:%S%z')
                except ValueError:
                    continue
            
            # If no format matches, try to clean up and format properly
            self.logger.warning(f"Unknown datetime format: {date_string}")
            return None
            
        except Exception as e:
            self.logger.warning(f"Failed to convert datetime {date_string}: {e}")
            return None
    
    def _map_financial_status(self, cratejoy_status: Optional[str]) -> str:
        """Map Cratejoy order status to Shopify financial status"""
        if not cratejoy_status:
            return 'pending'
        
        status_mapping = {
            'paid': 'paid',
            'completed': 'paid',
            'pending': 'pending',
            'cancelled': 'voided',
            'refunded': 'refunded',
            'partially_refunded': 'partially_refunded',
            'failed': 'pending'
        }
        
        return status_mapping.get(cratejoy_status.lower(), 'pending')
    
    def _map_fulfillment_status(self, cratejoy_status: Optional[str]) -> Optional[str]:
        """Map Cratejoy fulfillment status to Shopify fulfillment status"""
        if not cratejoy_status:
            return None
        
        status_mapping = {
            'shipped': 'shipped',
            'delivered': 'delivered',
            'fulfilled': 'fulfilled',
            'pending': None,
            'processing': None,
            'cancelled': None
        }
        
        return status_mapping.get(cratejoy_status.lower())
    
    # Product creation removed - products handled manually
