"""
Product Catalogue Updater with Image Scraper
Matches products from Excel to CSV, updates prices, sources images, and maintains catalogue
"""

import pandas as pd
import csv
import requests
from bs4 import BeautifulSoup
import urllib.parse
from difflib import SequenceMatcher
import logging
from typing import Optional, Dict, List, Tuple
import time
from urllib.parse import quote
import json
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('product_update.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProductUpdater:
    def __init__(self, csv_path: str, excel_path: str):
        """Initialize the product updater with CSV and Excel file paths"""
        self.csv_path = csv_path
        self.excel_path = excel_path
        self.csv_data = []
        self.excel_data = []
        self.updated_csv = []
        self.headers = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.image_cache = {}
        
    def load_csv(self) -> bool:
        """Load existing CSV file (handles both CSV and Excel formats)"""
        try:
            # Try to detect if it's actually an Excel file
            if self.csv_path.endswith(('.xlsx', '.xls')) or self._is_excel_file(self.csv_path):
                df = pd.read_excel(self.csv_path)
                self.headers = list(df.columns)
                self.csv_data = df.to_dict('records')
                logger.info(f"Loaded Excel file as CSV with {len(self.csv_data)} products")
                return True
            
            # Try different encodings for CSV
            encodings = ['utf-8', 'iso-8859-1', 'cp1252', 'latin-1']
            
            for encoding in encodings:
                try:
                    with open(self.csv_path, 'r', encoding=encoding) as f:
                        reader = csv.DictReader(f)
                        self.headers = reader.fieldnames
                        self.csv_data = list(reader)
                    logger.info(f"Loaded CSV with {len(self.csv_data)} products (encoding: {encoding})")
                    return True
                except Exception:
                    continue
            
            logger.error("Could not load CSV with any supported encoding")
            return False
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            return False
    
    def _is_excel_file(self, path: str) -> bool:
        """Check if file is actually an Excel file by reading magic bytes"""
        try:
            with open(path, 'rb') as f:
                return f.read(2) == b'PK'  # ZIP format used by Excel
        except:
            return False
    
    def load_excel(self) -> bool:
        """Load product data from Excel file"""
        try:
            df = pd.read_excel(self.excel_path)
            self.excel_data = df.to_dict('records')
            logger.info(f"Loaded Excel with {len(self.excel_data)} products")
            return True
        except Exception as e:
            logger.error(f"Error loading Excel: {e}")
            return False
    
    def similarity_ratio(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings"""
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    
    def find_product_in_csv(self, excel_product: Dict) -> Optional[int]:
        """
        Find product in CSV by reference number or description
        Returns the index if found, None otherwise
        """
        ref_number = str(excel_product.get('Reference Number', '')).strip()
        description = str(excel_product.get('Description', '')).strip()
        
        # First try exact match by reference number
        for idx, csv_product in enumerate(self.csv_data):
            csv_ref = str(csv_product.get('reference_no', '')).strip()
            if csv_ref and ref_number and csv_ref.lower() == ref_number.lower():
                logger.info(f"Exact match found by reference: {ref_number}")
                return idx
        
        # Try fuzzy match by description (>85% similarity)
        if description:
            for idx, csv_product in enumerate(self.csv_data):
                csv_desc = str(csv_product.get('description', '')).strip()
                if csv_desc:
                    similarity = self.similarity_ratio(description, csv_desc)
                    if similarity > 0.85:
                        logger.info(f"Fuzzy match found: {description} (similarity: {similarity:.2%})")
                        return idx
        
        return None
    
    def fetch_product_image(self, product_name: str, brand: str = "") -> Optional[str]:
        """
        Fetch product image URL from Google Images using bing search
        Returns the image URL if found, None otherwise
        """
        cache_key = f"{brand}_{product_name}".lower()
        if cache_key in self.image_cache:
            return self.image_cache[cache_key]
        
        try:
            # Use Bing Image Search (more reliable than Google)
            search_query = f"{product_name} {brand}".strip()
            search_url = f"https://www.bing.com/images/search?q={quote(search_query)}"
            
            response = self.session.get(search_url, timeout=10)
            response.raise_for_status()
            
            # Parse Bing Images
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find image URL from Bing (they use data attributes)
            images = soup.find_all('img', class_='mimg')
            
            if images:
                for img in images:
                    src = img.get('src') or img.get('data-src')
                    if src and 'data:image' not in src and src.startswith('http'):
                        # Validate URL
                        if self._validate_image_url(src):
                            self.image_cache[cache_key] = src
                            logger.info(f"Image found for {search_query}: {src[:60]}...")
                            return src
            
            # Fallback: Try DuckDuckGo
            image_url = self._fetch_from_duckduckgo(search_query)
            if image_url:
                self.image_cache[cache_key] = image_url
                return image_url
            
            logger.warning(f"No image found for {search_query}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching image for {product_name}: {e}")
            return None
    
    def _validate_image_url(self, url: str) -> bool:
        """Validate that URL is a valid image"""
        try:
            response = self.session.head(url, timeout=5, allow_redirects=True)
            content_type = response.headers.get('content-type', '')
            return 'image' in content_type and response.status_code == 200
        except:
            return False
    
    def _fetch_from_duckduckgo(self, query: str) -> Optional[str]:
        """Fallback image fetcher using DuckDuckGo"""
        try:
            url = f"https://duckduckgo.com/?q={quote(query)}&iax=images&ia=images"
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # DuckDuckGo stores images in JSON format
            results = soup.find_all('img', {'data-src': True})
            if results:
                for result in results:
                    img_url = result.get('data-src')
                    if img_url and img_url.startswith('http'):
                        return img_url
            return None
        except Exception as e:
            logger.error(f"DuckDuckGo fallback failed: {e}")
            return None
    
    def clone_product_from_brand(self, brand: str) -> Optional[Dict]:
        """
        Clone an existing product from the same brand
        Returns the product dict if found, None otherwise
        """
        for product in self.csv_data:
            if product.get('brand', '').lower() == brand.lower():
                cloned = product.copy()
                logger.info(f"Product cloned from brand: {brand}")
                return cloned
        
        logger.warning(f"No product found to clone from brand: {brand}")
        return None
    
    def prepare_product_row(self, excel_product: Dict, image_url: Optional[str] = None) -> Dict:
        """Prepare a product row for CSV output"""
        row = {}
        
        # Map Excel columns to CSV columns
        column_mapping = {
            'reference_no': 'Reference Number',
            'description': 'Description',
            'price_before_tax': 'Price',
            'price_after_tax': 'Price',
            'brand': 'Brand',
            'image': None,  # Will be handled separately
            'quantity': 'Quantity',
        }
        
        # Initialize with existing CSV headers
        for header in self.headers:
            row[header] = ''
        
        # Update with Excel data
        row['reference_no'] = str(excel_product.get('Reference Number', '')).strip()
        row['description'] = str(excel_product.get('Description', '')).strip()
        
        # Handle pricing - price before and after tax should be the same
        price = excel_product.get('Price')
        if pd.notna(price):
            try:
                price_value = float(price)
                row['price_before_tax'] = f"{price_value:.2f}"
                row['price_after_tax'] = f"{price_value:.2f}"
            except:
                logger.warning(f"Invalid price: {price}")
        
        row['brand'] = str(excel_product.get('Brand', '')).strip()
        row['quantity'] = 15  # Set quantity to 15 as per requirements
        row['active'] = 1  # Updated products should be active
        row['image'] = image_url or ''
        row['note'] = str(excel_product.get('Note', '')).strip()
        
        return row
    
    def update_existing_product(self, csv_idx: int, excel_product: Dict, image_url: Optional[str]) -> Dict:
        """Update an existing CSV product with Excel data"""
        product = self.csv_data[csv_idx].copy()
        
        # Update fields
        product['reference_no'] = str(excel_product.get('Reference Number', '')).strip()
        product['description'] = str(excel_product.get('Description', '')).strip()
        
        # Update price (before and after tax should be the same)
        price = excel_product.get('Price')
        if pd.notna(price):
            try:
                price_value = float(price)
                product['price_before_tax'] = f"{price_value:.2f}"
                product['price_after_tax'] = f"{price_value:.2f}"
            except:
                logger.warning(f"Invalid price: {price}")
        
        product['brand'] = str(excel_product.get('Brand', '')).strip()
        product['quantity'] = 15
        product['active'] = 1
        if image_url:
            product['image'] = image_url
        product['note'] = str(excel_product.get('Note', '')).strip()
        
        return product
    
    def deactivate_brand_products(self, brand: str, except_idx: Optional[int] = None):
        """Deactivate all products of a brand except the one being updated"""
        for idx, product in enumerate(self.updated_csv):
            if product.get('brand', '').lower() == brand.lower():
                if except_idx is None or idx != except_idx:
                    product['active'] = 0
    
    def process(self):
        """Main processing function"""
        logger.info("=" * 60)
        logger.info("Starting Product Catalogue Update Process")
        logger.info("=" * 60)
        
        # Load files
        if not self.load_csv() or not self.load_excel():
            logger.error("Failed to load files")
            return False
        
        # Initialize updated CSV with all existing products
        self.updated_csv = [product.copy() for product in self.csv_data]
        processed_excel = set()
        
        # Process each Excel product
        for idx, excel_product in enumerate(self.excel_data):
            logger.info(f"\nProcessing Excel product {idx + 1}/{len(self.excel_data)}")
            logger.info(f"  Reference: {excel_product.get('Reference Number')}")
            logger.info(f"  Description: {excel_product.get('Description')}")
            
            # Try to find matching product in CSV
            csv_idx = self.find_product_in_csv(excel_product)
            
            if csv_idx is not None:
                # Update existing product
                logger.info("  Action: Updating existing product")
                
                # Fetch image
                image_url = self.fetch_product_image(
                    excel_product.get('Description', ''),
                    excel_product.get('Brand', '')
                )
                
                # Update the product
                updated_product = self.update_existing_product(csv_idx, excel_product, image_url)
                self.updated_csv[csv_idx] = updated_product
                processed_excel.add(idx)
                
                # Deactivate other products from same brand
                brand = excel_product.get('Brand', '')
                self.deactivate_brand_products(brand, csv_idx)
                
                logger.info(f"  ✓ Product updated successfully")
            else:
                # Product not found - try to clone from same brand
                logger.info("  Action: Product not found - attempting to clone from brand")
                
                brand = excel_product.get('Brand', '')
                cloned_product = self.clone_product_from_brand(brand)
                
                if cloned_product:
                    # Fetch image for new product
                    image_url = self.fetch_product_image(
                        excel_product.get('Description', ''),
                        brand
                    )
                    
                    # Update cloned product with Excel data
                    new_product = self.prepare_product_row(excel_product, image_url)
                    
                    # Add to updated CSV
                    self.updated_csv.append(new_product)
                    processed_excel.add(idx)
                    
                    # Deactivate other products from same brand
                    self.deactivate_brand_products(brand)
                    
                    logger.info(f"  ✓ Product added (cloned from brand)")
                else:
                    logger.warning(f"  ✗ Cannot add product - no brand match to clone from")
        
        logger.info(f"\n\nProcessed {len(processed_excel)}/{len(self.excel_data)} Excel products")
        return True
    
    def save_csv(self, output_path: str) -> bool:
        """Save updated CSV file"""
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.headers)
                writer.writeheader()
                writer.writerows(self.updated_csv)
            
            logger.info(f"\n✓ Updated CSV saved: {output_path}")
            logger.info(f"  Total products: {len(self.updated_csv)}")
            return True
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
            return False
    
    def generate_report(self) -> str:
        """Generate a summary report"""
        report = "\n" + "=" * 60
        report += "\nPRODUCT UPDATE REPORT\n"
        report += "=" * 60 + "\n"
        
        active_count = sum(1 for p in self.updated_csv if p.get('active') == 1 or p.get('active') == '1')
        inactive_count = len(self.updated_csv) - active_count
        
        report += f"\nTotal products in updated CSV: {len(self.updated_csv)}\n"
        report += f"Active products: {active_count}\n"
        report += f"Inactive products: {inactive_count}\n"
        report += f"Products with images: {sum(1 for p in self.updated_csv if p.get('image'))}\n"
        report += f"\nProcessed Excel products: {len(self.excel_data)}\n"
        
        report += "\n" + "=" * 60 + "\n"
        return report


def main():
    """Main execution function"""
    csv_file = "Older CSV.csv"
    excel_file = "New product.xlsx"
    output_file = "Updated_Products.csv"
    
    updater = ProductUpdater(csv_file, excel_file)
    
    if updater.process():
        updater.save_csv(output_file)
        print(updater.generate_report())
        logger.info(f"Process completed successfully!")
    else:
        logger.error("Process failed!")


if __name__ == "__main__":
    main()
