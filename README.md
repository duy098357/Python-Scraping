# Product Catalogue Updater with Image Scraper

A Python tool to refresh your product catalogue by matching items from Excel to your existing CSV, updating prices, sourcing product images, and managing product status.

## Features

✅ **Product Matching**
- Exact match by reference number
- Fuzzy matching by description (85%+ similarity)
- Handles missing products by cloning from same brand

✅ **Image Scraping**
- Automatic image fetching from search engines (Bing, DuckDuckGo)
- Validates image URLs for quality
- Caches results to avoid duplicate searches

✅ **Product Management**
- Updates prices (before and after tax in Naira)
- Sets quantity to 15 for updated products
- Marks updated products as active (1)
- Deactivates other products from the same brand (0)
- Maintains CSV format and order

✅ **Data Integrity**
- Preserves original CSV structure
- Handles fuzzy matching for similar descriptions
- Validates all data before saving
- Detailed logging for tracking all changes

## Installation

1. **Install Python 3.7+**
   - Download from https://www.python.org/downloads/

2. **Install Required Packages**
   ```powershell
   pip install -r requirements.txt
   ```

## Usage

1. **Prepare Your Files**
   - Place `Older CSV.csv` in the same directory
   - Place `New product.xlsx` with the new product data
   - Required Excel columns: Reference Number, Description, Price, Brand, Note

2. **Run the Script**
   ```powershell
   python product_updater.py
   ```

3. **Output**
   - `Updated_Products.csv` - Your refreshed catalogue
   - `product_update.log` - Detailed processing log

## CSV Format

Your CSV should have these columns (minimum):
- `reference_no` - Product reference number
- `description` - Product description
- `price_before_tax` - Price before tax (Naira)
- `price_after_tax` - Price after tax (should be same as before tax)
- `brand` - Brand name
- `image` - Product image URL
- `quantity` - Stock quantity (will be set to 15)
- `active` - 1 (active) or 0 (inactive)
- `note` - Additional notes

## Excel Format

Your Excel file should have these columns:
- `Reference Number` - Unique product identifier
- `Description` - Full product description
- `Price` - Product price in Naira
- `Brand` - Brand name
- `Note` - Any additional notes

## How It Works

1. **Loads Files**: Reads both CSV and Excel files
2. **Matches Products**: 
   - First tries exact match by reference number
   - If not found, uses fuzzy matching on description
3. **Fetches Images**: Automatically searches for and downloads product images
4. **Updates/Adds Products**:
   - Updates matching products with new data
   - Clones and modifies products for new items from existing brands
   - Sets prices, quantities, and status appropriately
5. **Maintains Status**:
   - Updated products = Active (1)
   - Other products from same brand = Inactive (0)
   - Other products from different brands = Unchanged

## Logging

All operations are logged to `product_update.log` for debugging. Check this file if:
- Products aren't being matched as expected
- Images aren't being found
- There are data validation issues

## Customization

Edit `product_updater.py` to:
- Change image search sources (line 90+)
- Adjust fuzzy matching threshold (default 0.85 = 85% similarity)
- Modify default quantity value (currently 15)
- Change active/inactive status logic

## Troubleshooting

**Products not matching:**
- Check that descriptions are similar enough (>85% match)
- Verify reference numbers match exactly
- Check the log file for details

**Images not found:**
- The script tries Bing first, then DuckDuckGo
- Some products may not have images available
- Check `product_update.log` for specific products

**CSV not updating:**
- Ensure Excel file has required columns
- Check column names match exactly
- Verify CSV is not open in another program
- Check `product_update.log` for errors

## Support

For issues or questions:
1. Check `product_update.log` for error messages
2. Verify your input files are formatted correctly
3. Ensure all required columns are present
