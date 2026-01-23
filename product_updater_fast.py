"""
Product Catalogue Updater - Fast Version
Matches products from Excel to CSV, updates prices, manages catalogue
Focuses on speed - skips external image fetching
"""

import pandas as pd
import csv
import os
from difflib import SequenceMatcher
import logging
from typing import Optional, Dict, List
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote

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
        self.processed_refs = set()
        self.image_cache = {}
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.last_image_fetch_time = 0  # Throttle: track last fetch time
        self.image_fetch_delay = 1.0  # seconds between image fetches
        
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
            # Normalize description columns that are split across multiple unnamed columns
            self._normalize_excel_descriptions()
            logger.info(f"Loaded Excel with {len(self.excel_df)} products")
            return True
        except Exception as e:
            logger.error(f"Error loading Excel: {e}")
            return False

    def _normalize_excel_descriptions(self):
        """Combine split description/short-description parts into single CSV-formatted string.

        Many supplier sheets have 'Short Description', 'Unnamed: X', 'Unnamed: Y' and
        similarly for 'Description'. This joins up to three adjacent columns into one
        comma-separated value and drops the extra parts.
        """
        cols = list(self.excel_df.columns)

        def combine_three(main_col_name):
            if main_col_name not in cols:
                return
            main_idx = cols.index(main_col_name)
            # Collect up to two following columns if they exist
            part_cols = [main_col_name]
            for offset in (1, 2):
                idx = main_idx + offset
                if idx < len(cols):
                    part_cols.append(cols[idx])

            # Build combined values
            def join_parts(row):
                parts = []
                for c in part_cols:
                    val = row.get(c, '')
                    if pd.isna(val):
                        continue
                    s = str(val).strip()
                    if s and s.lower() not in ('nan', 'none'):
                        parts.append(s)
                return ', '.join(parts)

            # Replace main column with combined data and drop extras (except the main)
            self.excel_df[main_col_name] = self.excel_df.apply(join_parts, axis=1)

            # Drop the other part columns (if they were unnamed or not the main column)
            for c in part_cols[1:]:
                if c in self.excel_df.columns:
                    try:
                        self.excel_df.drop(columns=[c], inplace=True)
                    except Exception:
                        pass

        # Combine for both Short Description and Description
        combine_three('Short Description')
        # Recompute cols after possible drops
        cols = list(self.excel_df.columns)
        combine_three('Description')
    
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
                    logger.info(f"  > Exact match found by reference: {ref_code}")
                    return idx
        
        # Try fuzzy match by product name + description
        if description and description != 'nan':
            for idx, row in self.csv_df.iterrows():
                csv_desc = str(row.get('Description', '')).strip()
                csv_brand = str(row.get('Manufacturer Name', '')).strip()
                
                # Check if brand matches and description is similar
                if csv_brand.lower() == brand.lower():
                    similarity = self.similarity_ratio(description, csv_desc)
                    if similarity > 0.85:
                        logger.info(f"  > Fuzzy match found: {description} (similarity: {similarity:.2%})")
                        return idx
        
        return None
    
    def clone_product_from_brand(self, brand: str) -> Optional[pd.Series]:
        """
        Clone an existing product from the same brand
        Returns the product row if found, None otherwise
        """
        for idx, row in self.csv_df.iterrows():
            if str(row.get('Manufacturer Name', '')).lower() == brand.lower():
                logger.info(f"  > Product cloned from brand: {brand}")
                return row.copy()
        
        logger.warning(f"  > No product found to clone from brand: {brand}")
        return None

    def create_brand_entry(self, brand: str) -> Optional[pd.Series]:
        """Create a minimal product template for a brand that doesn't exist.

        This constructs a row with the same columns as the CSV, fills the brand
        and reasonable defaults, and returns it for use when adding new products.
        """
        try:
            # Create an empty row with the same columns as the CSV
            template = {col: '' for col in self.csv_df.columns}
            # Fill minimal required fields
            template['Manufacturer Name'] = brand
            template['Product Name'] = ''
            template['Product Reference Code'] = ''
            template['Short Description'] = ''
            template['Description'] = ''
            template['Final Price Without Tax'] = ''
            template['Final Price With Tax'] = ''
            template['Quantity Physical'] = 0
            template['Active'] = 0
            # Return as a pandas Series for compatibility
            return pd.Series(template)
        except Exception as e:
            logger.error(f"  > Failed to create brand template for {brand}: {e}")
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
        
        # Keep existing image URLs, don't overwrite
        if 'Product Image Urls' not in row or pd.isna(row.get('Product Image Urls')):
            row['Product Image Urls'] = ''
        if 'Product Cover Image Url' not in row or pd.isna(row.get('Product Cover Image Url')):
            row['Product Cover Image Url'] = ''
        
        return row

    def prepare_product_row_with_image(self, excel_product: pd.Series, csv_row: Optional[pd.Series] = None) -> Dict:
        """Prepare product row and fetch image."""
        row = self.prepare_product_row(excel_product, csv_row)
        
        # Fetch image if not already present
        if not row.get('Product Image Urls') or pd.isna(row.get('Product Image Urls')):
            image_url = self.fetch_product_image(
                excel_product.get('Description', ''),
                excel_product.get('BRAND', '')
            )
            if image_url:
                row['Product Image Urls'] = image_url
                row['Product Cover Image Url'] = image_url
        
        return row
    
    def update_existing_product(self, csv_idx: int, excel_product: pd.Series) -> Dict:
        """Update an existing CSV product with Excel data"""
        csv_row = self.csv_df.iloc[csv_idx]
        updated_row = self.prepare_product_row(excel_product, csv_row)
        return updated_row
    
    def deactivate_brand_products(self, brand: str, except_ref: Optional[str] = None):
        """Deactivate all products of a brand except the one being updated"""
        count = 0
        for idx, row in enumerate(self.updated_rows):
            if str(row.get('Manufacturer Name', '')).lower() == brand.lower():
                if except_ref is None or str(row.get('Product Reference Code', '')) != except_ref:
                    row['Active'] = 0
                    count += 1
        if count > 0:
            logger.info(f"  > Deactivated {count} other products from brand {brand}")

    def fetch_product_image(self, product_name: str, brand: str = "") -> Optional[str]:
        """Fetch product image URL with throttling and retry logic."""
        # Check cache first
        cache_key = f"{brand}_{product_name}".lower()
        if cache_key in self.image_cache:
            return self.image_cache[cache_key]

        # Throttle: enforce minimum delay between requests
        elapsed = time.time() - self.last_image_fetch_time
        if elapsed < self.image_fetch_delay:
            time.sleep(self.image_fetch_delay - elapsed)

        try:
            search_query = f"{product_name} {brand}".strip()
            
            # Try Bing first
            image_url = self._fetch_from_bing(search_query)
            if image_url:
                self.image_cache[cache_key] = image_url
                self.last_image_fetch_time = time.time()
                logger.debug(f"  > Image found for {search_query[:40]}: {image_url[:50]}...")
                return image_url
            
            # Fallback to DuckDuckGo
            image_url = self._fetch_from_duckduckgo(search_query)
            if image_url:
                self.image_cache[cache_key] = image_url
                self.last_image_fetch_time = time.time()
                logger.debug(f"  > Image found (DDG) for {search_query[:40]}")
                return image_url
            
            logger.debug(f"  > No image found for {search_query[:40]}")
            self.last_image_fetch_time = time.time()
            return None
            
        except Exception as e:
            logger.debug(f"  > Error fetching image for {product_name}: {e}")
            self.last_image_fetch_time = time.time()
            return None

    def _fetch_from_bing(self, query: str, retries: int = 2) -> Optional[str]:
        """Fetch image from Bing Images with retry logic."""
        for attempt in range(retries):
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
                if attempt < retries - 1:
                    time.sleep(0.5)
                continue
        return None

    def _fetch_from_duckduckgo(self, query: str, retries: int = 2) -> Optional[str]:
        """Fetch image from DuckDuckGo with retry logic."""
        for attempt in range(retries):
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
                if attempt < retries - 1:
                    time.sleep(0.5)
                continue
        return None
    
    def process(self):
        """Main processing function"""
        logger.info("=" * 70)
        logger.info("Starting Product Catalogue Update Process (Fast Mode)")
        logger.info("=" * 70)
        
        # Load files
        if not self.load_csv() or not self.load_excel():
            logger.error("Failed to load files")
            return False
        
        logger.info(f"\nCSV Columns: {list(self.csv_df.columns)}")
        logger.info(f"Excel Columns: {list(self.excel_df.columns)}\n")
        
        # Initialize updated rows with all existing products
        self.updated_rows = [row.to_dict() for idx, row in self.csv_df.iterrows()]
        
        updated_count = 0
        added_count = 0
        not_matched_count = 0
        
        # Process each Excel product
        for idx, excel_product in self.excel_df.iterrows():
            excel_idx = idx + 1
            total = len(self.excel_df)
            
            ref_code = excel_product.get('Product Reference Code', 'N/A')
            description = excel_product.get('Description', 'N/A')
            
            logger.info(f"[{excel_idx:3d}/{total}] Ref: {str(ref_code)[:30]:30s} | Desc: {str(description)[:30]:30s}")
            
            # Try to find matching product in CSV
            csv_idx = self.find_product_in_csv(excel_product)
            brand = str(excel_product.get('BRAND', '')).strip()
            
            if csv_idx is not None:
                # Update existing product
                updated_product = self.prepare_product_row_with_image(excel_product, self.csv_df.iloc[csv_idx])
                self.updated_rows[csv_idx] = updated_product
                self.processed_refs.add(str(ref_code))
                updated_count += 1
                
                # Deactivate other products from same brand
                self.deactivate_brand_products(brand, str(ref_code))
                logger.info(f"  >> UPDATE: Product updated")
                
            else:
                # Product not found - try to clone from same brand
                cloned_product = self.clone_product_from_brand(brand)

                if cloned_product is None:
                    # Brand not present in CSV - create a brand template
                    logger.info(f"  > Brand '{brand}' not found in catalogue - creating brand template")
                    cloned_product = self.create_brand_entry(brand)

                if cloned_product is not None:
                    # Prepare new product with Excel data (from clone or created template)
                    new_product = self.prepare_product_row_with_image(excel_product, cloned_product)
                    
                    # Add to updated rows
                    self.updated_rows.append(new_product)
                    self.processed_refs.add(str(ref_code))
                    added_count += 1
                    
                    # Deactivate other products from same brand
                    self.deactivate_brand_products(brand)
                    
                    logger.info(f"  >> ADD: Product added (cloned or created for brand)")
                else:
                    not_matched_count += 1
                    logger.warning(f"  >> SKIP: Cannot add product - failed to clone or create brand template")
        
        logger.info("\n" + "=" * 70)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Total Excel products processed: {len(self.excel_df)}")
        logger.info(f"  - Updated existing: {updated_count}")
        logger.info(f"  - Added new: {added_count}")
        logger.info(f"  - Could not match: {not_matched_count}")
        logger.info(f"Total products in final catalogue: {len(self.updated_rows)}")
        logger.info("=" * 70 + "\n")
        
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
            
            # Try to remove existing file to avoid permission issues
            try:
                if os.path.exists(output_path):
                    os.remove(output_path)
            except Exception:
                # If removal fails, fall back to saving with timestamp suffix
                ts = int(time.time())
                base, ext = os.path.splitext(output_path)
                alt_path = f"{base}_{ts}{ext}"
                logger.warning(f"Could not remove existing file, saving to {alt_path} instead")
                output_path = alt_path

            # Always save as CSV
            output_df.to_csv(output_path, index=False, encoding='utf-8')
            
            logger.info(f"+ Updated catalogue saved: {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving catalogue: {e}")
            return False


def main():
    """Main execution function"""
    csv_file = "Older CSV.csv"
    excel_file = "New product.xlsx"
    output_file = "Updated_Products.csv"
    
    start_time = time.time()
    
    updater = ProductUpdater(csv_file, excel_file)
    
    if updater.process():
        updater.save_csv(output_file)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Process completed in {elapsed_time:.1f} seconds!")
        logger.info(f"Output file: {output_file}")
    else:
        logger.error("Process failed!")


if __name__ == "__main__":
    main()
