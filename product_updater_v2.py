"""
Product Catalogue Updater with Image Scraper v2
Matches products from Excel to CSV, updates prices, sources images, and maintains catalogue
Updated to work with actual column names
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
        logging.FileHandler('product_update.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProductUpdater:
    def __init__(self, csv_path: str, excel_path: str):
        """Initialize the product updater with CSV and Excel file paths"""
        self.csv_path = csv_path
        self.excel_path = excel_path
        self.csv_df = None
        self.excel_df = None
        self.updated_rows = []
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.image_cache = {}
        self.processed_refs = set()
        
    def load_csv(self) -> bool:
        """Load existing CSV file (handles both CSV and Excel formats)"""
        try:
            # Try to detect if it's actually an Excel file
            if self.csv_path.endswith(('.xlsx', '.xls')) or self._is_excel_file(self.csv_path):
                self.csv_df = pd.read_excel(self.csv_path)
                logger.info(f"Loaded CSV as Excel with {len(self.csv_df)} products")
            else:
                # Try different encodings for CSV
                encodings = ['utf-8', 'iso-8859-1', 'cp1252', 'latin-1']
                for encoding in encodings:
                    try:
                        self.csv_df = pd.read_csv(self.csv_path, encoding=encoding)
                        logger.info(f"Loaded CSV with {len(self.csv_df)} products (encoding: {encoding})")
                        return True
                    except Exception:
                        continue
                logger.error("Could not load CSV with any supported encoding")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            return False
    
    def load_excel(self) -> bool:
        """Load product data from Excel file"""
        try:
            self.excel_df = pd.read_excel(self.excel_path)
            logger.info(f"Loaded Excel with {len(self.excel_df)} products")
            return True
        except Exception as e:
            logger.error(f"Error loading Excel: {e}")
            return False
    
    def _is_excel_file(self, path: str) -> bool:
        """Check if file is actually an Excel file by reading magic bytes"""
        try:
            with open(path, 'rb') as f:
                return f.read(2) == b'PK'  # ZIP format used by Excel
        except:
            return False
    
    def similarity_ratio(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings"""
        s1 = str(str1).lower().strip()
        s2 = str(str2).lower().strip()
        return SequenceMatcher(None, s1, s2).ratio()
    
    def find_product_in_csv(self, excel_product: pd.Series) -> Optional[int]:
        """
        Find product in CSV by reference code or description
        Returns the index if found, None otherwise
        """
        ref_code = str(excel_product.get('Product Reference Code', '')).strip()
        description = str(excel_product.get('Description', '')).strip()
        brand = str(excel_product.get('BRAND', '')).strip()
        
        # First try exact match by reference code
        if ref_code and ref_code != 'nan':
            for idx, row in self.csv_df.iterrows():
                csv_ref = str(row.get('Product Reference Code', '')).strip()
                if csv_ref and csv_ref.lower() == ref_code.lower():
                    logger.info(f"Exact match found by reference: {ref_code}")
                    return idx
        
        # Try fuzzy match by product name + description
        if description and description != 'nan':
            for idx, row in self.csv_df.iterrows():
                csv_desc = str(row.get('Description', '')).strip()
                csv_name = str(row.get('Product Name', '')).strip()
                csv_brand = str(row.get('Manufacturer Name', '')).strip()
                
                # Check if brand matches and description is similar
                if csv_brand.lower() == brand.lower():
                    similarity = self.similarity_ratio(description, csv_desc)
                    if similarity > 0.85:
                        logger.info(f"Fuzzy match found: {description} (similarity: {similarity:.2%})")
                        return idx
        
        return None
    
    def fetch_product_image(self, product_name: str, brand: str = "") -> Optional[str]:
        """
        Fetch product image URL from search engines
        Returns the image URL if found, None otherwise
        """
        cache_key = f"{brand}_{product_name}".lower()
        if cache_key in self.image_cache:
            return self.image_cache[cache_key]
        
        try:
            # Use Bing Image Search
            search_query = f"{product_name} {brand}".strip()
            
            # Try multiple sources
            image_url = self._fetch_from_bing(search_query)
            if image_url:
                self.image_cache[cache_key] = image_url
                logger.info(f"Image found for {search_query}: {image_url[:60]}...")
                return image_url
            
            # Fallback to DuckDuckGo
            image_url = self._fetch_from_duckduckgo(search_query)
            if image_url:
                self.image_cache[cache_key] = image_url
                logger.info(f"Image found (DDG) for {search_query}: {image_url[:60]}...")
                return image_url
            
            logger.warning(f"No image found for {search_query}")
            return None
            
        except Exception as e:
            logger.error(f"Error fetching image for {product_name}: {e}")
            return None
    
    def _fetch_from_bing(self, query: str) -> Optional[str]:
        """Fetch image from Bing Images"""
        try:
            search_url = f"https://www.bing.com/images/search?q={quote(query)}"
            response = self.session.get(search_url, timeout=5)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            images = soup.find_all('img', class_='mimg')
            
            if images:
                for img in images:
                    src = img.get('src') or img.get('data-src')
                    if src and 'data:image' not in src and src.startswith('http'):
                        return src
            
            return None
        except Exception as e:
            logger.debug(f"Bing fetch failed: {e}")
            return None
    
    def _fetch_from_duckduckgo(self, query: str) -> Optional[str]:
        """Fetch image from DuckDuckGo"""
        try:
            url = f"https://duckduckgo.com/?q={quote(query)}&iax=images&ia=images"
            response = self.session.get(url, timeout=5)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            results = soup.find_all('img', {'data-src': True})
            if results:
                for result in results:
                    img_url = result.get('data-src')
                    if img_url and img_url.startswith('http'):
                        return img_url
            return None
        except Exception as e:
            logger.debug(f"DuckDuckGo fetch failed: {e}")
            return None
    
    def clone_product_from_brand(self, brand: str) -> Optional[pd.Series]:
        """
        Clone an existing product from the same brand
        Returns the product row if found, None otherwise
        """
        for idx, row in self.csv_df.iterrows():
            if str(row.get('Manufacturer Name', '')).lower() == brand.lower():
                logger.info(f"Product cloned from brand: {brand}")
                return row.copy()
        
        logger.warning(f"No product found to clone from brand: {brand}")
        return None
    
    def prepare_product_row(self, excel_product: pd.Series, csv_row: Optional[pd.Series] = None) -> Dict:
        """Prepare a product row for output"""
        if csv_row is not None:
            row = csv_row.to_dict()
        else:
            # Create new row with all expected columns
            row = {col: '' for col in self.csv_df.columns}
        
        # Update with Excel data
        ref_code = str(excel_product.get('Product Reference Code', '')).strip()
        product_name = str(excel_product.get('Product Name', '')).strip()
        description = str(excel_product.get('Description', '')).strip()
        brand = str(excel_product.get('BRAND', '')).strip()
        price = excel_product.get('Price')
        
        row['Product Reference Code'] = ref_code if ref_code != 'nan' else ''
        row['Product Name'] = product_name if product_name != 'nan' else ''
        row['Description'] = description if description != 'nan' else ''
        row['Short Description'] = str(excel_product.get('Short Description', '')).strip()
        row['Manufacturer Name'] = brand if brand != 'nan' else ''
        row['Active'] = 1
        row['Quantity Physical'] = 15
        
        # Handle pricing - price before and after tax should be the same
        if pd.notna(price):
            try:
                price_value = float(price)
                row['Final Price Without Tax'] = price_value
                row['Final Price With Tax'] = price_value
            except:
                logger.warning(f"Invalid price: {price}")
        
        return row
    
    def update_existing_product(self, csv_idx: int, excel_product: pd.Series) -> Dict:
        """Update an existing CSV product with Excel data"""
        csv_row = self.csv_df.iloc[csv_idx]
        updated_row = self.prepare_product_row(excel_product, csv_row)
        
        # Fetch image if not already present
        if not updated_row.get('Product Image Urls') or pd.isna(updated_row.get('Product Image Urls')):
            image_url = self.fetch_product_image(
                excel_product.get('Description', ''),
                excel_product.get('BRAND', '')
            )
            if image_url:
                updated_row['Product Image Urls'] = image_url
                updated_row['Product Cover Image Url'] = image_url
        
        return updated_row
    
    def deactivate_brand_products(self, brand: str, except_ref: Optional[str] = None):
        """Deactivate all products of a brand except the one being updated"""
        for idx, row in enumerate(self.updated_rows):
            if str(row.get('Manufacturer Name', '')).lower() == brand.lower():
                if except_ref is None or str(row.get('Product Reference Code', '')) != except_ref:
                    row['Active'] = 0
    
    def process(self):
        """Main processing function"""
        logger.info("=" * 60)
        logger.info("Starting Product Catalogue Update Process")
        logger.info("=" * 60)
        
        # Load files
        if not self.load_csv() or not self.load_excel():
            logger.error("Failed to load files")
            return False
        
        # Initialize updated rows with all existing products
        self.updated_rows = [row.to_dict() for idx, row in self.csv_df.iterrows()]
        
        # Process each Excel product
        for idx, excel_product in self.excel_df.iterrows():
            excel_idx = idx + 1
            total = len(self.excel_df)
            
            logger.info(f"\nProcessing Excel product {excel_idx}/{total}")
            ref_code = excel_product.get('Product Reference Code', 'N/A')
            description = excel_product.get('Description', 'N/A')
            logger.info(f"  Reference: {ref_code}")
            logger.info(f"  Description: {description}")
            
            # Try to find matching product in CSV
            csv_idx = self.find_product_in_csv(excel_product)
            brand = str(excel_product.get('BRAND', '')).strip()
            
            if csv_idx is not None:
                # Update existing product
                logger.info("  Action: Updating existing product")
                updated_product = self.update_existing_product(csv_idx, excel_product)
                self.updated_rows[csv_idx] = updated_product
                self.processed_refs.add(str(ref_code))
                
                # Deactivate other products from same brand
                self.deactivate_brand_products(brand, str(ref_code))
                logger.info(f"  + Product updated successfully")
                
            else:
                # Product not found - try to clone from same brand
                logger.info("  Action: Product not found - attempting to clone from brand")
                
                cloned_product = self.clone_product_from_brand(brand)
                
                if cloned_product is not None:
                    # Prepare new product with Excel data
                    new_product = self.prepare_product_row(excel_product, cloned_product)
                    
                    # Fetch image for new product
                    image_url = self.fetch_product_image(
                        description,
                        brand
                    )
                    if image_url:
                        new_product['Product Image Urls'] = image_url
                        new_product['Product Cover Image Url'] = image_url
                    
                    # Add to updated rows
                    self.updated_rows.append(new_product)
                    self.processed_refs.add(str(ref_code))
                    
                    # Deactivate other products from same brand
                    self.deactivate_brand_products(brand)
                    
                    logger.info(f"  + Product added (cloned from brand)")
                else:
                    logger.warning(f"  ✗ Cannot add product - no brand match to clone from")
        
        logger.info(f"\n\nProcessed {len(self.processed_refs)}/{len(self.excel_df)} Excel products")
        return True
    
    def save_csv(self, output_path: str) -> bool:
        """Save updated CSV file"""
        try:
            output_df = pd.DataFrame(self.updated_rows)
            # Ensure all columns from original CSV are present
            for col in self.csv_df.columns:
                if col not in output_df.columns:
                    output_df[col] = ''
            
            # Save in same order as original
            output_df = output_df[self.csv_df.columns]
            output_df.to_excel(output_path, index=False)
            
            logger.info(f"\n+ Updated CSV saved: {output_path}")
            logger.info(f"  Total products: {len(self.updated_rows)}")
            return True
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
            return False
    
    def generate_report(self) -> str:
        """Generate a summary report"""
        report = "\n" + "=" * 60
        report += "\nPRODUCT UPDATE REPORT\n"
        report += "=" * 60 + "\n"
        
        active_count = sum(1 for p in self.updated_rows if p.get('Active') == 1 or p.get('Active') == '1')
        inactive_count = len(self.updated_rows) - active_count
        
        report += f"\nTotal products in updated catalogue: {len(self.updated_rows)}\n"
        report += f"Active products: {active_count}\n"
        report += f"Inactive products: {inactive_count}\n"
        report += f"Processed Excel products: {len(self.processed_refs)}\n"
        
        report += "\n" + "=" * 60 + "\n"
        return report


def main():
    """Main execution function"""
    csv_file = "Older CSV.csv"
    excel_file = "New product.xlsx"
    output_file = "Updated_Products.xlsx"
    
    updater = ProductUpdater(csv_file, excel_file)
    
    if updater.process():
        updater.save_csv(output_file)
        print(updater.generate_report())
        logger.info(f"Process completed successfully!")
    else:
        logger.error("Process failed!")


if __name__ == "__main__":
    main()
