import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import re
import time
import json
from typing import List, Dict, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pymongo import MongoClient

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s: %(message)s')

@dataclass
class Product:
    name: str
    price: float
    retailer: str
    category: str = 'Uncategorized'
    original_price: float = None
    discount_percentage: float = None
    product_url: str = None
    image_url: str = None
    availability: str = 'Unknown'

class RetailCrawlerConfig:
    RETAILERS = {
        'pick_n_pay': {
            'homepage': 'https://www.pnp.co.za',
            'product_pages': [
                'https://www.pnp.co.za/pnpstorefront/pnp/en/All-Products',
                'https://www.pnp.co.za/pnpstorefront/pnp/en/Groceries',
                'https://www.pnp.co.za/pnpstorefront/pnp/en/Personal-Care'
            ],
            'crawl_strategy': {
                'method': 'selenium',
                'wait_element': '.product-grid',
                'scroll_pagination': True
            },
            'selectors': {
                'product_container': '.product-item',
                'name': '.product-name',
                'price': '.price',
                'category': '.category-breadcrumb'
            }
        },
        'checkers': {
            'homepage': 'https://www.checkers.co.za',
            'product_pages': [
                'https://www.checkers.co.za/all-products',
                'https://www.checkers.co.za/groceries',
                'https://www.checkers.co.za/personal-care'
            ],
            'crawl_strategy': {
                'method': 'selenium',
                'wait_element': '.product-grid',
                'scroll_pagination': True
            },
            'selectors': {
                'product_container': '.product-card',
                'name': '.product-title',
                'price': '.current-price',
                'category': '.breadcrumb-item'
            }
        },
        'woolworths': {
            'homepage': 'https://www.woolworths.co.za',
            'product_pages': [
                'https://www.woolworths.co.za/cat/Food/_/N-rhf7',
                'https://www.woolworths.co.za/cat/Personal-Care/_/N-1z141z6'
            ],
            'crawl_strategy': {
                'method': 'selenium',
                'wait_element': '.product-list',
                'infinite_scroll': True
            },
            'selectors': {
                'product_container': '.product-item',
                'name': '.product-name',
                'price': '.price',
                'category': '.breadcrumb-item'
            }
        }
    }

class IntelligentWebCrawler:
    def __init__(self, headless=True):
        self.driver = self._setup_selenium(headless)
        self.products = []
        self.successful_retailers = []
        self.mongo_client = MongoClient("mongodb://localhost:27017")  # Update URI if using Atlas
        self.db = self.mongo_client["retail_data"]  # Database name
        self.collection = self.db["products"]  # Collection name

    def _setup_selenium(self, headless=True):
        """
        Configure Selenium WebDriver with optimal settings
        """
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            return driver
        except Exception as e:
            logging.error(f"Selenium WebDriver setup failed: {e}")
            return None

    def _scroll_page(self, driver, max_scrolls=5):
        """
        Intelligent page scrolling to load dynamic content
        """
        for _ in range(max_scrolls):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)  # Allow content to load

    def crawl_retailer(self, retailer_key: str) -> List[Product]:
        """
        Comprehensive retailer crawling method
        """
        if not self.driver:
            logging.error("Selenium WebDriver not initialized")
            return []

        retailer_config = RetailCrawlerConfig.RETAILERS.get(retailer_key)
        if not retailer_config:
            logging.error(f"No configuration found for {retailer_key}")
            return []

        retailer_products = []
        
        for product_page in retailer_config['product_pages']:
            try:
                # Navigate to product page
                self.driver.get(product_page)

                # Wait for dynamic content
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 
                        retailer_config['crawl_strategy'].get('wait_element', 'body')))
                )

                # Optional page scrolling
                if retailer_config['crawl_strategy'].get('scroll_pagination'):
                    self._scroll_page(self.driver)

                # Extract products
                products = self._extract_products(retailer_key, retailer_config)
                retailer_products.extend(products)

                logging.info(f"{retailer_key}: Crawled {len(products)} products from {product_page}")

            except Exception as e:
                logging.error(f"Error crawling {retailer_key} - {product_page}: {e}")

        return retailer_products
    
    def save_to_mongo(self, products):
        """
        Save products to MongoDB
        """
        if products:
            # Transform Product objects into dictionaries for MongoDB
            product_dicts = [vars(product) for product in products]
            self.collection.insert_many(product_dicts)
            logging.info(f"Saved {len(product_dicts)} products to MongoDB")
        else:
            logging.warning("No products to save.")

    def _extract_products(self, retailer: str, config: Dict) -> List[Product]:
        """
        Advanced product extraction with error handling
        """
        products = []
        
        try:
            product_containers = self.driver.find_elements(
                By.CSS_SELECTOR, 
                config['selectors']['product_container']
            )

            for container in product_containers:
                try:
                    name = container.find_element(
                        By.CSS_SELECTOR, 
                        config['selectors']['name']
                    ).text.strip()

                    price_element = container.find_element(
                        By.CSS_SELECTOR, 
                        config['selectors']['price']
                    )
                    price_text = price_element.text.strip()
                    
                    # Clean price extraction
                    price = float(re.sub(r'[^\d.]', '', price_text))

                    product = Product(
                        name=name,
                        price=price,
                        retailer=retailer
                    )
                    products.append(product)

                except Exception as inner_e:
                    logging.warning(f"Individual product extraction error: {inner_e}")

        except Exception as e:
            logging.error(f"Product extraction error for {retailer}: {e}")

        return products

    def parallel_crawl(self, retailers=None):
        """
        Parallel crawling of multiple retailers
        """
        if not retailers:
            retailers = list(RetailCrawlerConfig.RETAILERS.keys())

        all_products = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_retailer = {
                executor.submit(self.crawl_retailer, retailer): retailer 
                for retailer in retailers
            }

            for future in as_completed(future_to_retailer):
                retailer = future_to_retailer[future]
                try:
                    products = future.result()
                    all_products[retailer] = products
                    if products:
                        self.successful_retailers.append(retailer)
                except Exception as e:
                    logging.error(f"Error crawling {retailer}: {e}")

        # Save results to CSV for further analysis
        self._save_to_csv(all_products)
        return all_products

    def _save_to_csv(self, products_dict: Dict[str, List[Product]]):
        """
        Save crawled products to CSV files
        """
        for retailer, products in products_dict.items():
            df = pd.DataFrame([vars(product) for product in products])
            filename = f"{retailer}_products.csv"
            df.to_csv(filename, index=False)
            logging.info(f"Saved {len(products)} products from {retailer} to {filename}")

    def __del__(self):
        """
        Cleanup WebDriver
        """
        if hasattr(self, 'driver') and self.driver:
            self.driver.quit()

def main():
    crawler = IntelligentWebCrawler()
    
    # Parallel crawling
    products = crawler.parallel_crawl()
    
    # Print summary
    for retailer, retailer_products in products.items():
        print(f"{retailer.upper()}: {len(retailer_products)} products found")

if __name__ == "__main__":
    main()